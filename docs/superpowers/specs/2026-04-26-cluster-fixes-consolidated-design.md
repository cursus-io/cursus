# Cluster Mode — Consolidated Bug Fixes & Design Improvements

**Date:** 2026-04-26
**Status:** Draft

## Issues Summary

### Critical (동작 오류)

| ID | 문제 | 근본 원인 |
|---|---|---|
| C1 | CONSUME에서 ValidateOwnership이 FSM 전파 지연 시 generation 불일치로 실패 | 파티션 리더의 FSM generation이 coordinator보다 뒤처짐 |
| C2 | BATCH PUBLISH 바이너리 데이터가 파티션 리더로 포워딩 안됨 | `isPartitionLeaderAndForward`가 텍스트 커맨드만 포워딩, 바이너리 미지원 |
| C3 | `applyViaLeader` 실패 시 로컬 fallback이 Raft와 불일치 유발 | `handleJoinGroup`에서 applyViaLeader 실패 → 로컬 AddConsumer → FSM과 상태 불일치 |
| C4 | 클러스터 produce가 sync write 강제 (성능 저하) | async write 후 ACK 하면 consumer가 못 읽어서 sync로 변경됨 |

### Important (기능 부족)

| ID | 문제 | 영향 |
|---|---|---|
| I1 | SDK에서 METADATA 주기적 갱신 없음 | 리더 변경 시 NOT_LEADER를 받아야만 업데이트 |
| I2 | Producer SDK에 METADATA 연동 없음 | produce가 항상 브로커 간 포워딩 경유 |
| I3 | FIND_COORDINATOR 결과의 `checkCoordinator`에서 매번 네트워크 호출 | 이미 캐싱 추가했으나 TTL 만료 후 반복 호출 |

---

## C1: CONSUME에서 ownership 검증 제거

**현재**: `readFromTopic`에서 `ValidateOwnership(group, member, generation, partition)` 호출. 파티션 리더의 FSM generation이 coordinator보다 뒤처지면 실패.

**Kafka 방식**: FETCH는 ownership 검증 없음. 파티션 리더는 단순히 offset 기반으로 메시지 반환. Ownership은 coordinator의 JOIN/SYNC에서만 관리.

**수정**: `readFromTopic`에서 `RecordHeartbeat`와 `ValidateOwnership` 호출을 제거. CONSUME은 파티션 리더의 역할이고, 그룹 멤버십 관리는 coordinator의 역할.

## C2: BATCH PUBLISH 바이너리 포워딩

**현재**: `HandleBatchMessage`에서 `isPartitionLeaderAndForward(batch.Topic, batch.Partition, "BATCH")` — 텍스트 "BATCH"만 포워딩하고 실제 바이너리 데이터는 전달 안됨. 비-파티션 리더에서 `IsAuthorized` 실패 시 `ForwardDataToPartitionLeader`로 바이너리 전달 시도하지만 3회 재시도로 불안정.

**수정**: `HandleBatchMessage` 시작 부분의 `isPartitionLeaderAndForward` (텍스트) 호출을 제거하고, `ForwardDataToPartitionLeader` (바이너리) 경로만 사용. SDK가 METADATA로 파티션 리더를 직접 찾아서 보내면 포워딩 자체가 불필요.

## C3: applyViaLeader 로컬 fallback 제거

**현재**: `handleJoinGroup`에서 `applyViaLeader` 실패 후 로컬 `AddConsumer` fallback → Raft FSM과 불일치.

**수정**: 로컬 fallback을 제거하고 `applyViaLeader` 실패 시 에러 반환. SDK가 재시도.

## C4: Async write + flush before ACK

**현재**: 클러스터 produce에서 `EnqueueBatchSync` (매 메시지 disk write). 성능 저하.

**수정**: `EnqueueBatch` (async, 채널에 넣기) → replication → `Partition.FlushDisk()` → ACK. 여러 메시지가 배치로 한번에 flush되어 처리량 향상.

Partition에 `FlushDisk()` 메서드 추가:
```go
func (p *Partition) FlushDisk() {
    p.dh.Flush()
}
```

produce_handler.go:
```go
p.EnqueueBatch(msgs)                // 채널에 넣기 (빠름)
ch.Cluster.ReplicateToFollowers()   // 복제
p.FlushDisk()                       // 채널 drain + 배치 write
// ACK
```

## I1: SDK METADATA 주기적 갱신

**현재**: `fetchMetadata()` 시작 시 1회만 호출.

**수정**: Go SDK — `metadataRefreshInterval` 설정 추가 (기본 30초). 백그라운드 goroutine이 주기적으로 METADATA 호출. Python SDK — 동일 패턴.

## I2: Producer SDK METADATA 연동

**현재**: Go/Python Producer SDK에 METADATA 연동 없음. 항상 아무 브로커에 PUBLISH → 파티션 리더가 아니면 브로커 간 포워딩.

**수정**: Producer SDK에 `fetchMetadata()` 추가. 파티션 결정 후 해당 파티션 리더에 직접 전송. NOT_LEADER 시 리더 업데이트.

---

## Files Changed

**Broker:**
- `pkg/controller/consume_handler.go` — readFromTopic에서 heartbeat/ownership 제거
- `pkg/controller/produce_handler.go` — EnqueueBatchSync → EnqueueBatch + FlushDisk
- `pkg/controller/command_handler.go` — handleJoinGroup 로컬 fallback 제거
- `pkg/topic/partition.go` — FlushDisk() 메서드 추가

**Go SDK:**
- `sdk/consumer.go` — 주기적 metadata 갱신
- `sdk/producer.go` — METADATA 연동, 파티션 리더 직접 전송

**Python SDK:**
- `src/cursus/consumer.py` — 주기적 metadata 갱신
- `src/cursus/producer.py` — METADATA 연동
