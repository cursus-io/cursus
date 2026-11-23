# Preferred leader 재균형 구현 계획

이 문서는 [`preferred-leader-rebalance.md`](./preferred-leader-rebalance.md)의 승인된 설계를 테스트 가능한 구현 단위로 나눈다. 목표는 장애 후 임시 리더를 `Replicas[0]`으로 안전하게 복귀시키는 것이다. replica 배치 변경, 부하 기반 leader 최적화, rack-aware 배치는 이번 범위에 포함하지 않는다.

## 1. 완료 조건

다음 조건을 모두 만족하면 구현이 끝난 것으로 본다.

- 자동 재균형은 기본으로 켜지고 첫 검사는 설정된 전체 주기가 지난 뒤 실행된다.
- Raft leader만 계획하고 적용하며, 한 주기에 설정된 최대 개수(기본 8개)를 순차 처리한다.
- 내부 토픽 후보가 있으면 내부 토픽 1개를 먼저 처리하고 남은 용량으로 일반 토픽을 처리한다.
- preferred replica가 active이며 ISR에 있고, replica 수와 topology가 유효할 때만 이동한다.
- 계획 이후 lifecycle 또는 ordered replica 목록이 바뀐 명령은 FSM에서 거부된다.
- 이미 적용된 동일 명령을 재시도하면 topology fence가 일치하는 한 멱등 성공한다.
- `CLUSTER_STATUS`와 bounded-cardinality metric으로 불균형, 대상, 결과와 제외 이유를 확인할 수 있다.
- leader 이동 자체의 partition data copy, log scan, recovery read/write/transfer bytes는 0이다.
- 3-broker 장애, preferred replica 복귀, 컨트롤러 전환, stale command를 자동 테스트한다.

## 2. 변경 스택

서로 의존하지만 독립 리뷰가 가능한 세 계층이므로 GitHub 공식 `gh stack` 흐름을 사용한다.

| 순서 | 브랜치 | 목적 | 대표 커밋 |
| --- | --- | --- | --- |
| 1 | `docs/preferred-leader-rebalance` | Kafka 비교, 설계, 구현 계획 | `docs(cluster): design preferred leader rebalance`, `docs(cluster): plan preferred leader rebalance` |
| 2 | `fix/leader-election-topology-fence` | 기존 leader election 명령의 lifecycle/replica fence | `fix(cluster): fence leader elections by topology` |
| 3 | `feat/preferred-leader-rebalance` | 설정, planner/runner, 상태, metric, E2E | 기능별 signed commit |

현재 환경에 `gh stack` 확장이 없으므로 구현 시작 전에 `gh extension install github/gh-stack`으로 설치한다. 문서 브랜치에서 `gh stack init`을 실행한 뒤 fence 브랜치와 feature 브랜치를 `gh stack add`로 순서대로 만든다. 제출은 `gh stack submit --auto --open`으로 ready-for-review 상태를 사용한다. PR 제목과 본문에는 stack 위치나 prev/next 링크를 넣지 않는다.

## 3. 1단계: leader election topology fence

### 3.1 먼저 실패 테스트 추가

대상 파일:

- `pkg/cluster/replication/fsm/fsm_admin_test.go`
- `pkg/controller/cluster_admin_handler_test.go`

추가할 회귀 테스트:

1. 현재 topic definition과 partition metadata의 lifecycle이 명령과 같을 때 선거가 성공한다.
2. 명령을 만든 뒤 topic이 재생성되어 lifecycle이 바뀌면 거부한다.
3. 명령을 만든 뒤 ordered replica 목록의 멤버 또는 순서가 바뀌면 거부한다.
4. leader epoch만 바뀐 stale 명령을 거부한다.
5. 성공 응답을 잃은 뒤 같은 명령을 다시 적용하면 `Changed=false`로 성공한다.
6. 이미 목표 broker가 leader더라도 lifecycle 또는 replicas가 stale이면 성공으로 숨기지 않고 거부한다.
7. 수동 `ELECT_LEADER`가 현재 metadata에서 모든 fence를 채워 적용한다.

먼저 다음 명령으로 새 테스트가 기존 구현에서 실패하는지 확인한다.

```powershell
go test ./pkg/cluster/replication/fsm -run LeaderElection -count=1
go test ./pkg/controller -run 'LeaderElection|ClusterAdmin' -count=1
```

### 3.2 명령과 FSM 변경

대상 파일:

- `pkg/cluster/replication/fsm/fsm_admin.go`
- `pkg/controller/cluster_admin_handler.go`

`LeaderElectionCommand`에 다음 필드를 추가한다.

```go
ExpectedLifecycleEpoch uint64   `json:"expected_lifecycle_epoch"`
ExpectedReplicas       []string `json:"expected_replicas"`
```

FSM은 lock을 잡은 동일 snapshot에서 아래 순서로 검사한다.

1. topic definition과 partition metadata가 존재한다.
2. `definition.LifecycleEpoch == metadata.LifecycleEpoch == ExpectedLifecycleEpoch`이다.
3. `metadata.Replicas`가 `ExpectedReplicas`와 순서까지 정확히 같다.
4. 목표 broker가 active이고 replica 및 ISR에 속한다.
5. 목표가 이미 현재 leader면 변경 없이 성공한다.
6. 그 외에는 expected leader epoch가 일치할 때만 leader와 epoch를 갱신한다.

이 순서는 stale topology를 멱등 성공으로 오인하지 않으면서, 커밋 후 응답만 잃은 재시도를 성공시킨다. manual command 문법은 바꾸지 않고 handler가 조회한 metadata로 새 fence를 채운다. 하위 버전 command 호환 분기는 두지 않는다.

### 3.3 1단계 검증

```powershell
gofmt -w pkg/cluster/replication/fsm/fsm_admin.go pkg/cluster/replication/fsm/fsm_admin_test.go pkg/controller/cluster_admin_handler.go pkg/controller/cluster_admin_handler_test.go
go test ./pkg/cluster/replication/fsm -run LeaderElection -count=1
go test ./pkg/controller -run 'LeaderElection|ClusterAdmin' -count=1
go test ./pkg/cluster/replication/fsm ./pkg/controller
git diff --check
```

검증 후 `git commit -s -S -m "fix(cluster): fence leader elections by topology"`로 커밋한다.

## 4. 2단계: 설정과 순수 planner

### 4.1 설정을 테스트부터 추가

대상 파일:

- `pkg/config/properties.go`
- `pkg/config/properties_helper.go`
- `pkg/config/properties_ext_test.go`

추가할 설정:

| Go 필드 | property | 환경 변수 | CLI | 기본값 |
| --- | --- | --- | --- | --- |
| `LeaderRebalanceEnabled` | `leader.rebalance.enabled` | `LEADER_REBALANCE_ENABLED` | `--leader-rebalance-enabled` | `true` |
| `LeaderRebalanceIntervalMS` | `leader.rebalance.interval.ms` | `LEADER_REBALANCE_INTERVAL_MS` | `--leader-rebalance-interval-ms` | `30000` |
| `LeaderRebalanceMaxMovesPerCycle` | `leader.rebalance.max.moves.per.cycle` | `LEADER_REBALANCE_MAX_MOVES_PER_CYCLE` | `--leader-rebalance-max-moves-per-cycle` | `8` |

테스트는 기본값, property, env, CLI 우선순위와 0/음수 interval 및 max moves의 기본값 정규화를 각각 고정한다. bool은 기존 config parser의 명시적 `true`/`false` 규칙을 그대로 따른다.

### 4.2 결정론적 planner 구현

대상 파일:

- `pkg/cluster/replication/fsm/leader_rebalance_snapshot.go`
- `pkg/cluster/replication/fsm/leader_rebalance_snapshot_test.go`
- `pkg/cluster/controller/leader_rebalancer.go`
- `pkg/cluster/controller/leader_rebalancer_test.go`

FSM에 leader 재균형 전용 detached snapshot API를 추가한다. 이 API는 한 번의 read lock 안에서 topic definition, partition metadata, durable broker 상태를 복사하여 서로 다른 시점의 조회 결과를 섞지 않는다. 반환한 slice와 map을 호출자가 바꿔도 FSM 내부 상태가 변하지 않아야 한다.

planner는 이 snapshot과 설정을 입력받고 부작용 없이 후보, 제외 이유, 합계를 반환한다. 각 후보에는 topic, partition, 현재 leader, preferred leader, leader epoch, lifecycle epoch, ordered replicas, internal 여부를 담는다.

후보 조건:

- 현재 leader가 `Replicas[0]`과 다르다.
- replica 수가 topic replication factor와 같다.
- recovery replica가 없다.
- preferred broker가 active이며 ISR에 있다.
- definition/metadata lifecycle과 partition count가 일치한다.
- leader, replicas, ISR이 구조적으로 유효하다.

정렬과 선택 규칙:

1. internal 후보와 application 후보를 각각 `(topic, partition)` 오름차순으로 정렬한다.
2. internal 후보가 있으면 첫 후보 하나를 먼저 선택한다.
3. 남은 cycle 용량을 application 후보로 채운다.
4. internal 후보가 없으면 전체 용량을 application 후보로 채운다.

단위 테스트는 다음을 포함한다.

- 이미 preferred leader이면 제외
- preferred replica가 inactive 또는 ISR 밖이면 제외
- under-replicated/recovery 중인 partition 제외
- lifecycle, partition count, replica 구조가 잘못된 partition 제외
- 정렬 결과가 map 순회 순서와 무관함
- max moves가 1, 8, 후보 수보다 큰 경우
- internal 1개 예약과 application 7개 선택
- internal 후보가 없을 때 application 8개 선택
- planner 입력 snapshot을 변경하지 않음
- snapshot을 가져오는 중 concurrent metadata 변경이 있어도 race가 없고 내부 slice가 alias되지 않음

### 4.3 2단계 검증

```powershell
gofmt -w pkg/config/properties.go pkg/config/properties_helper.go pkg/config/properties_ext_test.go pkg/cluster/replication/fsm/leader_rebalance_snapshot.go pkg/cluster/replication/fsm/leader_rebalance_snapshot_test.go pkg/cluster/controller/leader_rebalancer.go pkg/cluster/controller/leader_rebalancer_test.go
go test ./pkg/config -count=1
go test ./pkg/cluster/replication/fsm -run LeaderRebalanceSnapshot -count=1
go test ./pkg/cluster/controller -run LeaderRebalance -count=1
git diff --check
```

설정과 planner는 함께 리뷰할 수 있는 한 커밋으로 만든다.

```powershell
git commit -s -S -m "feat(cluster): plan preferred leader moves"
```

## 5. 3단계: runner와 프로세스 lifecycle

대상 파일:

- `pkg/cluster/controller/leader_rebalancer.go`
- `pkg/cluster/controller/leader_rebalancer_test.go`
- `pkg/server/main.go`

추가 API:

```go
func (cc *ClusterController) StartLeaderRebalancer(ctx context.Context) func()
func (cc *ClusterController) RunLeaderRebalanceOnce(ctx context.Context) LeaderRebalanceCycleResult
```

runner 규칙:

1. 비활성 설정이면 goroutine을 만들지 않는 no-op stop 함수를 반환한다.
2. 시작 직후 실행하지 않고 full interval을 기다린다.
3. 각 cycle 시작 시 Raft leadership을 확인한다.
4. 같은 FSM snapshot으로 계획한 뒤 후보별 `LEADER_ELECTION`을 순차 적용한다.
5. 각 명령에 topology 및 leader epoch fence를 모두 넣는다.
6. partition 단위 stale/ineligible 오류는 기록하고 다음 후보로 진행한다.
7. Raft leadership 상실 또는 context 취소는 남은 후보를 중단한다.
8. stop 함수는 cancel 후 goroutine 종료까지 기다린다.

runner 테스트에는 fake clock/ticker 또는 주입 가능한 interval trigger를 사용해 wall-clock sleep을 피한다. 다음을 검증한다.

- 첫 주기 전에는 apply가 없다.
- follower에서는 plan/apply가 없다.
- 한 cycle의 apply 순서와 최대 개수
- 한 후보 실패 후 다음 후보 진행
- 중간 leadership 상실 시 즉시 중단
- context 취소와 stop의 goroutine 정리
- apply 성공 후 응답 오류를 가정한 다음 cycle 재시도가 멱등함

`pkg/server/main.go`에서는 topology reconciler와 같은 cluster-only lifecycle 지점에 start를 등록하고 `runtimeStops` 역순 종료를 그대로 사용한다.

```powershell
gofmt -w pkg/cluster/controller/leader_rebalancer.go pkg/cluster/controller/leader_rebalancer_test.go pkg/server/main.go
go test ./pkg/cluster/controller -run LeaderRebalance -count=1
go test ./pkg/server -count=1
git diff --check
git commit -s -S -m "feat(cluster): rebalance preferred leaders"
```

## 6. 4단계: 상태와 metric

대상 파일:

- `pkg/controller/cluster_admin_handler.go`
- `pkg/controller/cluster_admin_handler_test.go`
- `pkg/metrics/leader_rebalance.go`
- `pkg/metrics/leader_rebalance_test.go`
- `pkg/metrics/exporter.go`
- 필요하면 `pkg/observability/collector.go`

`CLUSTER_STATUS`에 cluster 합계와 partition 진단을 추가한다.

- 전체 imbalanced partition 수
- 현재 eligible partition 수
- 현재 cycle에서 선택 가능한 수
- partition별 preferred leader
- `leader_rebalance_eligible`
- `leader_rebalance_exclusion_reason`

새 metric:

- `cursus_leader_rebalance_imbalanced_partitions`
- `cursus_leader_rebalance_eligible_partitions`
- `cursus_leader_rebalance_moves_total{result,reason}`
- `cursus_leader_rebalance_cycle_duration_seconds`
- `cursus_leader_rebalance_last_success_timestamp`
- broker별 현재 leader 수와 preferred leader 수

topic, partition, broker address, 오류 문자열은 label로 사용하지 않는다. broker metric은 기존 durable broker ID 집합만 사용하고 제거된 broker series를 삭제한다. `result`와 `reason`은 코드에 정의한 유한 집합만 허용한다.

테스트는 metric descriptor/수치, 실패 reason cardinality, 삭제된 broker series 정리, status JSON의 합계와 개별 reason 일치를 검증한다.

```powershell
gofmt -w pkg/controller/cluster_admin_handler.go pkg/controller/cluster_admin_handler_test.go pkg/metrics/leader_rebalance.go pkg/metrics/leader_rebalance_test.go pkg/metrics/exporter.go pkg/observability/collector.go
go test ./pkg/controller ./pkg/metrics ./pkg/observability -count=1
git diff --check
git commit -s -S -m "feat(cluster): expose leader rebalance state"
```

존재하지 않는 선택 파일은 `gofmt` 명령에서 제외한다.

## 7. 5단계: 장애 복구와 비용 E2E

대상 파일:

- `test/e2e-cluster/preferred_leader_rebalance_e2e_test.go`
- `.github/workflows/e2e-tests.yml`

`RUN_E2E_LEADER_REBALANCE=1`로 명시적으로 켜는 3-broker 테스트를 추가한다. fixture에는 interval을 짧게 설정하되 production 기본값 검증은 config 단위 테스트에 맡긴다.

### 시나리오 A: 장애 후 자동 복귀

1. RF=3, 여러 partition 토픽을 만들고 replica 순서를 저장한다.
2. preferred leader broker를 중단해 ISR 내 다른 replica로 failover시킨다.
3. failover leader로 produce/consume이 계속됨을 확인한다.
4. preferred broker를 재시작하고 catch-up 및 ISR 복귀를 기다린다.
5. interval 이후 leader가 `Replicas[0]`으로 자동 복귀하는지 확인한다.
6. acknowledged record와 committed HWM이 유지되는지 확인한다.

### 시나리오 B: 내부 토픽 제한

내부 토픽과 일반 토픽에 동시에 후보를 만든 뒤 한 cycle의 성공 이동 중 internal이 최대 1개이고 전체가 max moves 이하인지 metric과 status로 확인한다.

### 시나리오 C: 컨트롤러 전환과 stale command

cycle 도중 기존 Raft leader를 중단한다. 새 leader가 다음 interval부터 다시 계획하고, 이전 leader가 만든 stale lifecycle/replica command는 FSM 단위 테스트와 E2E fault hook 중 가능한 가장 좁은 방식으로 거부됨을 확인한다. 전용 production 우회 경로는 추가하지 않는다.

### 비용 측정

재균형 직전과 직후 각 노드의 다음 counter delta를 기록한다.

- `cursus_recovery_work_total{unit="read_bytes"}`
- `cursus_recovery_work_total{unit="write_bytes"}`
- `cursus_recovery_work_total{unit="transfer_bytes"}`

catch-up 완료 시점을 baseline으로 잡고 leader-only 이동 구간의 세 delta가 모두 0인지 검증한다. Raft metadata command 수와 serialized payload bytes는 별도 journal에 기록한다. partition log bytes는 복사되지 않지만 Raft metadata entry와 일반 background I/O까지 0이라고 표현하지 않는다.

```powershell
gofmt -w test/e2e-cluster/preferred_leader_rebalance_e2e_test.go
$env:RUN_E2E_LEADER_REBALANCE='1'
go test ./test/e2e-cluster -run PreferredLeaderRebalance -count=1 -v
Remove-Item Env:RUN_E2E_LEADER_REBALANCE
git diff --check
git commit -s -S -m "test(cluster): cover preferred leader recovery"
```

## 8. 전체 검증

feature 브랜치에서 아래 순서로 검증한다.

```powershell
go test ./pkg/config ./pkg/cluster/replication/fsm ./pkg/cluster/controller ./pkg/controller ./pkg/metrics ./pkg/observability ./pkg/server -count=1
go test -race ./pkg/cluster/replication/fsm ./pkg/cluster/controller ./pkg/controller
go test ./... -count=1
git diff --check
git status --short
```

E2E는 앞 절의 전용 테스트와 기존 cluster fault/recovery suite를 CI에서 실행한다. race가 Windows 또는 외부 toolchain 제약으로 실행 불가하면 실패를 숨기지 않고 명령과 원인을 PR에 기록한다.

각 커밋은 다음을 확인한다.

```powershell
git log --show-signature --format=fuller -1
git log -1 --format=%B
```

GPG signature와 `Signed-off-by`가 모두 있어야 하며 commit message에는 issue 번호, URL, closing reference를 넣지 않는다.

## 9. PR 제출과 리뷰

1. 각 브랜치에서 repository PR template를 그대로 채운다.
2. issue 연결은 PR 본문에서만 repository 관례에 맞게 한다.
3. `gh stack submit --auto --open`으로 세 PR을 ready-for-review 상태로 제출한다.
4. Cursus 조직 repository이므로 각 PR이 처음 review-ready가 된 뒤 RabbitCode의 최초 자동 리뷰 하나만 기다린다.
5. actionable feedback을 수정하고 signed commit으로 push한다.
6. 해당 review thread를 resolve하고 CI를 다시 확인한다.
7. 수정 뒤 bot review를 수동 재요청하지 않는다.
8. PR은 사용자가 직접 merge하며 `gh stack merge`를 실행하지 않는다.

하위 fence 계층이 바뀌면 `gh stack rebase`와 `gh stack push`로 상위 feature 계층을 전파한다. 사용자가 하위 PR을 merge한 뒤에는 `gh stack sync` 후 남은 계층을 계속한다.

## 10. 중단과 되돌리기

- 운영 중 즉시 중단은 `leader.rebalance.enabled=false`로 수행한다. 이미 커밋된 leader election은 되돌리지 않지만 이후 자동 이동은 없다.
- feature 계층을 되돌려도 topology fence 계층은 기존 수동 선거의 안전성을 높이므로 독립 유지할 수 있다.
- 잘못된 자동 이동은 data copy를 만들지 않으며 다음 수동 선거 또는 metadata 갱신으로 교정할 수 있다.
- 데이터 손실, ISR 밖 선거, stale topology 명령 수락이 하나라도 관찰되면 feature 제출을 중단하고 fence 계층부터 수정한다.

## 11. 후속 범위

이번 스택이 안정화된 뒤 별도 설계와 PR로 다룬다.

- durable replica reassignment와 operator API
- `__transaction_state` 기반 transaction coordinator 분산
- broker 부하, leader 수, throughput을 함께 보는 load-aware leader optimizer
- rack-aware replica placement와 자동 replica rebalance
