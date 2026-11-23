# 자동 preferred leader 재균형 설계

- 상태: 설계 승인, 구현 계획 작성 전
- 작성일: 2026-11-23
- 대상: 분산 모드의 파티션 리더 배치

## 1. 배경

Cursus는 파티션마다 `Leader`, `Replicas`, `ISR`, `LeaderEpoch`를 Raft FSM에 저장한다. 토픽을 만들 때 consistent hash로 replica 목록을 정하고 첫 번째 replica를 초기 리더로 선택한다. 요청은 현재 파티션 리더로 전달되며, 리더가 비활성화되면 `ISRManager`가 살아 있는 ISR 구성원 중 새 리더를 선출한다. 관리자는 `ELECT_LEADER`로 리더를 수동 이동할 수 있다.

장애 조치 뒤 복구된 기존 리더는 자동으로 리더 역할을 되찾지 않는다. 따라서 장애가 해소된 뒤에도 일부 broker에 리더가 몰릴 수 있다. 현재 topology reconciler는 replication factor보다 replica 수가 적은 기존 파티션만 보수하므로 이 리더 불균형을 해결하지 않는다.

이 설계는 `Replicas[0]`을 preferred leader로 정의하고, 안전한 후보를 제한된 수만큼 주기적으로 복귀시킨다. replica 데이터 배치는 바꾸지 않는다.

## 2. Kafka 비교와 Cursus의 선택

Kafka 4.1은 replica 목록의 첫 번째 broker를 preferred replica로 정의한다. `auto.leader.rebalance.enable`이 켜져 있으면 컨트롤러가 주기적으로 불균형을 확인하고 preferred replica가 사용 가능한 파티션의 리더를 복귀시킨다. 관리자는 preferred leader election을 수동으로 실행할 수도 있다.

Kafka의 현재 KRaft 구현은 한 번의 불균형 처리에서 수행할 선출 수를 제한한다. Apache Kafka에서는 복구 직후 대량 선출을 더 점진적으로 제어하려는 논의도 진행 중이다. Cursus는 다음을 채택한다.

- `Replicas[0]`을 preferred leader로 사용한다.
- 자동 재균형을 기본으로 활성화한다.
- Raft 리더인 컨트롤러만 계획하고 적용한다.
- preferred replica가 active 상태이며 ISR에 포함된 경우에만 clean election을 수행한다.
- 기존 수동 `ELECT_LEADER` 경로를 재사용하고 lifecycle, replica 목록과 `LeaderEpoch`를 함께 fencing한다.
- 실행당 이동 수를 제한한다.

Cursus는 다음과 같이 다르게 동작한다.

- Kafka의 대규모 클러스터 기본값을 그대로 따르지 않고 한 주기당 최대 8개만 이동한다.
- coordinator 이동 비용이 큰 내부 토픽은 한 주기당 최대 1개만 이동한다.
- broker별 불균형 백분율 임계값을 두지 않는다. 적격 후보가 있으면 제한된 속도로 완전히 수렴한다.
- 실행 계획을 영속화하지 않는다. 컨트롤러가 바뀌면 현재 메타데이터에서 다시 계산한다.

이 차이는 수렴 속도보다 장애 복구 직후의 부하와 coordinator 재탐색량을 제한하는 데 우선순위를 둔 결과다.

### 참고한 Kafka 1차 자료

- [Kafka 4.1 Basic Kafka Operations: Balancing leadership](https://kafka.apache.org/41/operations/basic-kafka-operations/)
- [Kafka 4.1 broker configuration: `auto.leader.rebalance.enable`](https://kafka.apache.org/41/configuration/broker-configs/)
- [Apache Kafka 4.1.0 `ReplicationControlManager`](https://github.com/apache/kafka/blob/4.1.0/metadata/src/main/java/org/apache/kafka/controller/ReplicationControlManager.java)
- [KIP-460: Admin Leader Election RPC](https://cwiki.apache.org/confluence/spaces/KAFKA/pages/113707931/KIP-460%2BAdmin%2BLeader%2BElection%2BRPC)
- [KIP-1358: Gradual and gated preferred-leader election](https://cwiki.apache.org/confluence/spaces/KAFKA/pages/430409245/KIP-1358%2BGradual%2Band%2Bgated%2Bpreferred-leader%2Belection%2Bin%2Bthe%2BKRaft%2Bcontroller)

KIP-1358은 이 문서를 작성한 시점에 제안 상태다. Cursus의 이동 제한을 Kafka에서 이미 제공하는 동작으로 표현해서는 안 된다.

## 3. 목표

- 장애 조치 뒤 리더를 결정적인 preferred replica로 자동 복귀시킨다.
- 여러 파티션의 preferred leader가 서로 다른 broker에 배치된 기존 구조를 다시 활용한다.
- replica 복사나 파티션 로그 스캔 없이 리더 메타데이터만 변경한다.
- stale 계획과 이전 리더의 지연된 작업을 `LeaderEpoch`로 차단한다.
- 내부 토픽의 coordinator 이동을 일반 데이터 파티션보다 느리게 진행한다.
- 컨트롤러 장애나 응답 유실 뒤에도 별도 롤백 절차 없이 현재 상태에서 수렴한다.
- 이동 수, 제외 사유, Raft 기록 비용과 클라이언트 재시도를 관측할 수 있게 한다.

## 4. 제외 범위

- QPS, bytes, CPU 또는 디스크 지연 기반 leader optimizer
- replica 데이터 이동과 broker 추가 시 자동 데이터 재분산
- rack 또는 availability zone 인식
- unclean leader election
- transaction coordinator의 `__transaction_state` 내부 토픽 전환
- 진행 중 replica reassignment의 durable 작업 모델

부하 기반 leader optimizer는 durable replica reassignment와 broker 부하 관측이 준비된 뒤 별도 설계로 다룬다.

## 5. 용어와 불변 조건

- **preferred leader**: `PartitionMetadata.Replicas[0]`
- **불균형 파티션**: 현재 `Leader`가 preferred leader와 다른 파티션
- **적격 후보**: 안전 조건을 모두 만족하여 이번 주기에 이동할 수 있는 불균형 파티션
- **내부 파티션**: `__consumer_offsets`와 이후 도입할 `__transaction_state` 같은 시스템 토픽의 파티션

다음 불변 조건을 유지해야 한다.

1. 리더는 언제나 replica 목록과 ISR에 포함되어야 한다.
2. 자동 재균형은 active 상태가 아닌 broker를 선출하지 않는다.
3. 리더가 바뀌면 `LeaderEpoch`가 반드시 증가해야 한다.
4. 파티션 lifecycle, replica 배치 또는 leader epoch가 계획 뒤 바뀌면 이전 계획을 적용하지 않는다.
5. 자동 재균형은 파티션 데이터와 HWM을 변경하지 않는다.
6. 한 주기의 성공한 이동 수는 전체 8개, 내부 파티션 1개를 넘지 않는다.

## 6. 설정

다음 설정을 `config.Config`에 추가한다.

| YAML | JSON | 환경 변수 | CLI | 형식 | 기본값 | 의미 |
| --- | --- | --- | --- | --- | --- | --- |
| `leader_rebalance_enabled` | `leader.rebalance.enabled` | `LEADER_REBALANCE_ENABLED` | `--leader-rebalance-enabled` | bool | `true` | 자동 preferred leader 재균형을 활성화한다. |
| `leader_rebalance_interval_ms` | `leader.rebalance.interval.ms` | `LEADER_REBALANCE_INTERVAL_MS` | `--leader-rebalance-interval-ms` | int | `30000` | 재균형 주기다. |
| `leader_rebalance_max_moves_per_cycle` | `leader.rebalance.max.moves.per.cycle` | `LEADER_REBALANCE_MAX_MOVES_PER_CYCLE` | `--leader-rebalance-max-moves-per-cycle` | int | `8` | 한 주기에 성공할 수 있는 전체 이동 수다. |

`leader_rebalance_interval_ms`와 `leader_rebalance_max_moves_per_cycle`은 양수여야 한다. 잘못된 값은 각각 기본값으로 정규화한다. 내부 파티션 제한은 첫 버전에서 상수 `1`로 유지하여 설정 조합을 늘리지 않는다.

각 broker의 설정은 동일해야 한다. 활성 Raft 리더의 설정이 해당 주기에 적용되므로 broker별 값이 다르면 컨트롤러 전환 뒤 속도가 달라질 수 있다.

## 7. 구성 요소

### 7.1 `LeaderRebalancePlanner`

순수한 계획 구성 요소다. 현재 topic definition, partition metadata, broker 상태를 입력받아 후보를 반환한다. Raft 적용이나 시간 제어를 직접 수행하지 않는다.

후보에는 다음 값이 포함된다.

- topic과 partition
- 현재 leader
- preferred leader
- 계획 당시 `LeaderEpoch`
- `LifecycleEpoch`
- 계획 당시 replica 목록
- 내부 파티션 여부

계획기는 파티션별 제외 사유도 집계한다. 제외 사유는 bounded enum으로 관리한다.

- `already_preferred`
- `missing_replica`
- `assignment_in_progress`
- `preferred_inactive`
- `preferred_out_of_isr`
- `recovery_pending`
- `invalid_metadata`

### 7.2 `LeaderRebalancer`

`ClusterController`가 소유하는 주기 실행기다. `StartLeaderRebalancer(ctx)`는 취소 가능한 goroutine을 시작하고 종료 함수는 goroutine이 완전히 끝날 때까지 join한다. 첫 실행은 컨트롤러 시작 직후가 아니라 한 번의 전체 interval이 지난 뒤 수행한다. 새 Raft 리더가 heartbeat와 ISR 상태를 관측하기 전에 계획을 적용하는 것을 피하기 위해서다.

`RunLeaderRebalanceOnce(ctx)`는 테스트에서 직접 호출할 수 있는 단일 실행 진입점이다.

### 7.3 기존 leader election FSM

새로운 durable 명령 prefix는 추가하지 않는다. 각 후보는 기존 `LEADER_ELECTION` 명령으로 적용한다. 다만 `LeaderElectionCommand`에 `ExpectedLifecycleEpoch`와 `ExpectedReplicas`를 추가하여 계획 뒤 leader가 아닌 topology만 바뀐 경우도 차단한다. 명령은 topic, partition, preferred broker, expected lifecycle epoch, expected replica 목록과 expected leader epoch를 포함한다. FSM은 다음을 다시 검증한다.

- 파티션과 broker가 존재한다.
- 후보 broker가 active 상태다.
- 후보 broker가 replicas와 ISR에 포함된다.
- expected lifecycle epoch가 현재 partition과 topic definition의 lifecycle epoch와 같다.
- expected replica 목록이 현재 replica 목록과 순서까지 같다.
- expected leader epoch가 현재 값과 같다.

성공하면 기존 방식대로 `LeaderEpoch`를 증가시킨다.

수동 `ELECT_LEADER`의 텍스트 명령 계약은 바꾸지 않는다. handler가 현재 metadata에서 lifecycle과 replica fence를 채운다.

## 8. 후보 선택

파티션은 다음 조건을 모두 만족할 때만 적격하다.

1. `Leader != Replicas[0]`이다.
2. topic definition과 partition metadata가 존재한다.
3. `len(Replicas)`가 topic replication factor와 같다.
4. `RecoveryReplicas`가 비어 있다.
5. preferred replica가 durable broker registry에서 active 상태다.
6. preferred replica가 ISR에 포함된다.
7. 현재 leader, preferred leader, ISR과 replica 목록에 중복이나 빈 broker ID가 없다.

replica 수가 replication factor보다 크거나 작으면 reassignment 또는 legacy topology 보수가 진행 중일 수 있으므로 건너뛴다. 향후 durable reassignment 상태가 도입되면 명시적인 in-progress 표시를 우선 사용한다.

적격 후보는 일반 파티션과 내부 파티션으로 나눈 뒤 각 큐에서 topic, partition 순으로 정렬한다.

- 내부 후보가 있으면 첫 번째 내부 후보에 1개 슬롯을 예약한다.
- 남은 슬롯에는 일반 후보를 최대 7개 넣는다.
- 일반 후보가 없더라도 내부 후보를 한 주기에 두 개 이상 선택하지 않는다.
- 내부 후보가 없으면 일반 후보를 최대 8개 선택한다.

성공한 후보는 다음 주기에 `already_preferred`가 되어 자연스럽게 빠진다. 실패한 후보는 다음 주기에 새 metadata로 다시 평가한다. 별도 cursor나 계획 상태는 저장하지 않는다.

## 9. 적용 흐름

한 주기는 다음 순서로 실행된다.

1. 기능이 꺼져 있거나 현재 노드가 Raft 리더가 아니면 아무 작업도 하지 않는다.
2. 현재 metadata에서 계획을 만든다.
3. 선택한 후보를 순서대로 `LEADER_ELECTION`에 적용한다.
4. 각 적용은 계획에 포함된 expected lifecycle, replica 목록과 `LeaderEpoch`를 사용한다.
5. stale lifecycle, replica 변경, stale epoch, ISR 변경 또는 broker 비활성화로 거부된 후보는 실패로 기록하고 다음 후보를 처리한다.
6. Raft 리더십 상실이나 context 취소가 감지되면 남은 후보를 즉시 중단한다.
7. 주기 결과와 비용을 metric에 반영한다.

적용은 순차적으로 수행한다. 최대 8개의 작은 metadata 명령을 병렬 적용하여 Raft 순서와 오류 분류를 복잡하게 만들 이유가 없다.

응답이 유실됐지만 명령이 적용된 경우에도 롤백하지 않는다. 다음 주기에 현재 leader가 preferred leader임을 확인하고 완료 상태로 수렴한다. 같은 expected epoch를 가진 중복 명령은 기존 election 검증으로 무해하게 처리되어야 한다.

## 10. 장애와 다른 제어 루프의 상호작용

### 10.1 장애 조치

`ISRManager`의 clean failover가 자동 재균형보다 우선한다. 현재 리더가 죽으면 재균형 interval을 기다리지 않고 살아 있는 ISR에서 새 리더를 선출한다. 복구된 preferred replica가 catch-up을 마치고 ISR에 다시 들어온 뒤에만 재균형 후보가 된다.

### 10.2 replica catch-up과 reassignment

`RecoveryReplicas`가 있거나 replica 수가 replication factor와 다르면 이동하지 않는다. 따라서 catch-up, bootstrap, 수동 add/remove 단계와 계획적으로 겹치지 않는다.

### 10.3 consumer coordinator

`__consumer_offsets` 파티션 리더가 consumer coordinator이므로 리더 이동 뒤 클라이언트가 coordinator를 다시 조회할 수 있다. stale route는 기존 owner, leader epoch, lifecycle epoch 검증으로 차단한다. 한 주기에 내부 파티션을 하나만 이동하여 대량 재탐색을 제한한다.

### 10.4 컨트롤러 전환

계획은 영속화하지 않는다. 이전 컨트롤러가 일부 후보를 적용한 뒤 내려가면 새 컨트롤러가 첫 interval 이후 현재 metadata에서 다시 계획한다. 이미 적용된 파티션은 제외되고 미적용 파티션만 남는다.

## 11. 가용성, 안전성과 복구 비용

### 가용성

자동 재균형은 현재 사용 가능한 리더를 ISR 구성원인 preferred replica로 옮긴다. metadata 적용과 클라이언트 라우팅 갱신 사이에 짧은 `NOT_LEADER` 또는 `NOT_COORDINATOR` 재시도가 발생할 수 있다. clean election 자체는 파티션 데이터를 복사하지 않으며, 후보 검증을 통과한 파티션을 의도적으로 unavailable 상태로 만들지 않는다.

### 안전성

ISR 밖 replica와 inactive broker는 절대 선출하지 않는다. 계획 뒤 상태가 바뀌면 expected lifecycle, replica 목록, leader epoch와 적용 시점 검증이 stale 이동을 차단한다. unclean election은 이 실행기에서 호출할 수 없다.

### 복구 비용

성공한 이동 한 건의 기대 비용은 다음과 같다.

- 파티션 데이터 복사: `0`바이트
- 파티션 로그 스캔: `0`바이트
- replica bootstrap/catch-up 요청: `0`건
- Raft metadata 명령: `1`건
- 추가 작업: 라우팅 갱신과 클라이언트 재시도

테스트와 측정에서는 명령 건수만 보고하지 않는다. 직렬화된 Raft command와 log entry의 실제 바이트, 클라이언트 재시도 수와 이동 전후 요청 지연을 함께 기록한다.

## 12. 관측성

다음 metric을 추가한다. topic과 partition을 label로 사용하지 않는다.

- `cursus_leader_rebalance_imbalanced_partitions`
- `cursus_leader_rebalance_eligible_partitions`
- `cursus_leader_rebalance_moves_total{result,reason}`
- `cursus_leader_rebalance_cycle_duration_seconds`
- `cursus_leader_rebalance_last_success_timestamp`
- broker별 현재 leader 수와 preferred leader 수를 나타내는 bounded broker metric

`reason`은 계획기의 bounded enum만 허용한다. 임의 오류 문자열을 label로 사용하지 않는다.

`CLUSTER_STATUS`에는 cluster 합계와 파티션별 진단을 추가한다.

- `preferred_leader`
- `leader_imbalanced`
- `preferred_leader_active`
- `preferred_leader_in_isr`
- `leader_rebalance_eligible`
- `leader_rebalance_exclusion_reason`

로그는 주기 요약을 info 또는 debug 수준으로 남기고, 파티션별 반복 제외 로그는 rate limit하거나 status에서만 제공한다.

## 13. 구현 위치

예상 변경 범위는 다음과 같다.

- `pkg/config/properties.go`: 설정 필드, 기본값, CLI와 환경 변수 연결
- `pkg/config/properties_helper.go`: 설정 정규화
- `pkg/cluster/controller/leader_rebalancer.go`: 계획기와 실행기
- `pkg/cluster/controller/leader_rebalancer_test.go`: 순수 계획과 실행 단위 테스트
- `pkg/cluster/replication/fsm/fsm_admin.go`: lifecycle과 replica assignment election fence
- `pkg/cluster/replication/fsm/fsm_admin_test.go`: 새 election fence 회귀 테스트
- `pkg/server/main.go`: 실행기 lifecycle 연결
- `pkg/controller/cluster_admin_handler.go`: `CLUSTER_STATUS` 진단 확장
- observability package: bounded metric 등록
- `test/e2e-cluster`: 장애 조치, 복귀, coordinator 재탐색과 비용 검증

수동 `ELECT_LEADER`의 외부 텍스트 계약은 바꾸지 않는다. 내부 `fsm.LeaderElectionCommand`에는 stale topology를 차단할 fence를 추가한다.

## 14. 검증 계획

### 14.1 계획기 단위 테스트

- 현재 leader가 preferred leader면 후보에서 제외한다.
- preferred replica가 inactive이거나 ISR 밖이면 제외한다.
- replica 수가 replication factor와 다르면 제외한다.
- `RecoveryReplicas`가 있으면 제외한다.
- 후보 순서가 입력 map 순서와 무관하게 결정적이다.
- 전체 최대 8개와 내부 최대 1개를 지킨다.
- 내부 후보가 있으면 슬롯 하나를 예약한다.
- 유효하지 않은 metadata는 panic 없이 명시적 사유로 제외한다.

### 14.2 실행기와 FSM 테스트

- stale leader epoch 명령을 거부한다.
- stale lifecycle epoch와 변경된 replica 목록을 거부한다.
- 계획 뒤 preferred replica가 ISR에서 빠지면 이동하지 않는다.
- broker가 비활성화되면 이동하지 않는다.
- 성공한 이동은 `LeaderEpoch`를 정확히 한 번 증가시킨다.
- 한 후보 실패가 다른 후보 적용을 막지 않는다.
- Raft 리더십을 잃으면 남은 적용을 중단한다.
- 응답 유실 뒤 다음 주기가 중복 이동하지 않는다.
- stop 함수가 goroutine을 join하고 저장소 종료 뒤 접근하지 않는다.

### 14.3 3-broker 장애 복구 E2E

1. 여러 파티션의 preferred leader가 세 broker에 분산된 토픽을 만든다.
2. 한 broker를 중지하고 해당 파티션이 clean failover되는지 확인한다.
3. failover 동안 `acks=all` publish와 committed read를 계속한다.
4. broker를 복구하고 replica catch-up과 ISR 재진입을 기다린다.
5. 자동 재균형이 preferred leader를 복귀시키는지 확인한다.
6. ACK된 레코드 손실과 중복이 없는지 검증한다.
7. replica 복사 바이트와 파티션 로그 스캔 바이트가 리더 이동 때문에 증가하지 않았는지 확인한다.

### 14.4 내부 토픽 E2E

- 여러 `__consumer_offsets` 파티션의 리더가 불균형한 상태를 만든다.
- 한 주기에 내부 파티션이 하나만 이동하는지 확인한다.
- stale coordinator 요청이 fenced되는지 확인한다.
- SDK가 `NOT_COORDINATOR` 뒤 새 coordinator를 조회하고 정상 동작을 재개하는지 확인한다.
- group generation, membership와 committed offset이 유지되는지 확인한다.

### 14.5 컨트롤러 장애 E2E

- 한 주기에서 일부 이동을 적용한 뒤 Raft 리더를 종료한다.
- 새 컨트롤러가 저장된 계획 없이 현재 상태에서 남은 파티션만 수렴시키는지 확인한다.
- 이전 컨트롤러의 지연된 명령이 epoch fence에 의해 거부되는지 확인한다.

### 14.6 성능 측정

- 균형 상태에서 기능 활성화 전후 처리량과 p50/p95/p99 지연을 비교한다.
- 최대 파티션 설정에 가까운 metadata에서 계획 시간, CPU와 allocation을 측정한다.
- 8개 일반 파티션 이동과 내부 파티션 1개 이동 중 요청 지연과 재시도량을 각각 측정한다.
- Raft command와 log entry의 실제 기록 바이트를 보고한다.

성능 회귀가 확인되면 metadata 인덱스를 추가하기 전에 측정 결과로 병목을 특정한다. 첫 구현에서는 30초마다 `O(partitions)` 스캔하는 단순한 계획기를 우선한다.

## 15. 후속 설계

이 문서는 다음 작업의 기반이지만 해당 동작을 구현하지 않는다.

1. durable replica reassignment: 계획 제출, add/catch-up/remove 자동 완료, 조회, 취소, 재시작 복구와 bytes/sec throttle
2. `__transaction_state`: transaction state 파티션 리더 기반 coordinator와 compacted 상태 로그
3. load-aware leader optimizer: 안정화된 부하 지표, hysteresis, cooldown과 이동 비용을 반영한 리더 선택

후속 기능은 이 재균형 실행기의 무데이터 이동 계약과 내부 토픽 이동 제한을 깨지 않아야 한다.
