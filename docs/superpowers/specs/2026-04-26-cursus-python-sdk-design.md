# Cursus Python SDK Design Spec

## Overview

Cursus message broker를 위한 Python 클라이언트 라이브러리. cursus-java 레포와 동일한 수준의 구조, 문서, CI/CD를 갖춘 별도 레포(`cursus-python`)로 구성한다.

**Target**: Python 중심 개발자 및 데이터/ML 엔지니어 (Cursus 입문 친화적)

## Key Decisions

| 항목 | 결정 |
|---|---|
| Python 버전 | 3.10+ |
| 동기/비동기 | 둘 다 제공 (sync 기본, async `Async` 접두사) |
| 빌드 도구 | uv + hatchling |
| 배포 | PyPI (`pip install cursus-client`) |
| 레포 구조 | 별도 레포, uv workspace 기반 멀티 패키지 |
| 기능 범위 | Go SDK 전체 대응 (Producer, Consumer, ConsumerGroup, EventStore) |
| 테스트 | pytest + 단위/통합 + GitHub Actions CI/CD + PyPI 자동 배포 |
| 프레임워크 통합 | FastAPI (Java의 Spring Boot Starter 대응) |

---

## Module Structure

Java 3-모듈 구조를 Python에 대응한다.

| cursus-java | cursus-python | 설명 |
|---|---|---|
| `cursus-client` | `cursus-client/` | 코어 라이브러리 |
| `cursus-spring-boot-starter` | `cursus-fastapi/` | 프레임워크 통합 |
| `cursus-examples` | `cursus-examples/` | standalone + framework 예제 |
| Maven Central | PyPI | 패키지 레지스트리 |
| Gradle 멀티모듈 | uv workspace | 모노레포 내 다중 패키지 |

```
cursus-python/
|
+-- cursus-client/                      # PyPI: cursus-client
|   +-- pyproject.toml
|   +-- src/cursus/
|   |   +-- __init__.py                 # 공개 API re-export
|   |   +-- py.typed                    # PEP 561
|   |   +-- protocol/                   # 와이어 프로토콜 (0xBA7C batch, 4-byte length prefix)
|   |   +-- connection/                 # sync(socket) + async(asyncio) 연결 관리
|   |   +-- compression/                # gzip 내장, snappy/lz4 extras
|   |   +-- config.py                   # ProducerConfig, ConsumerConfig (dataclass)
|   |   +-- types.py                    # Message, AckResponse
|   |   +-- errors.py                   # CursusError 계층
|   |   +-- producer.py                 # Producer (sync)
|   |   +-- consumer.py                 # Consumer (sync)
|   |   +-- eventstore.py              # EventStore (sync)
|   |   +-- async_producer.py
|   |   +-- async_consumer.py
|   |   +-- async_eventstore.py
|   +-- tests/
|       +-- unit/                       # protocol, config, compression, types
|       +-- integration/                # 실제 브로커 대상 (Docker fixture)
|
+-- cursus-fastapi/                     # PyPI: cursus-fastapi
|   +-- pyproject.toml
|   +-- src/cursus_fastapi/
|       +-- config.py                   # CursusSettings (pydantic-settings)
|       +-- dependencies.py             # get_producer(), get_consumer() DI
|       +-- decorator.py                # @cursus_listener
|       +-- health.py                   # health check endpoint
|
+-- cursus-examples/
|   +-- standalone/                     # 6개 예제 (simple, keyed, batch, group, streaming, event_sourcing)
|   +-- fastapi/                        # FastAPI REST app + listener
|
+-- docs/                              # 7개 문서 (Java와 동일 구성)
|   +-- getting-started.md
|   +-- architecture.md
|   +-- producer-guide.md
|   +-- consumer-guide.md
|   +-- protocol.md
|   +-- configuration-reference.md
|   +-- fastapi-integration.md
|
+-- .github/workflows/
|   +-- ci.yml                          # lint + test + type check
|   +-- release.yml                     # PyPI 배포
+-- CONTRIBUTING.md
+-- LICENSE
+-- README.md
```

---

## Layer Architecture

Java의 레이어드 아키텍처를 그대로 대응한다.

```
+-------------------------------------------------------+
|  Public API                                           |
|  Producer  .  Consumer  .  EventStore                 |
|  AsyncProducer  .  AsyncConsumer  .  AsyncEventStore  |
+-------------------------------------------------------+
|  Configuration Layer                                  |
|  ProducerConfig  .  ConsumerConfig  (dataclass)       |
+-------------------------------------------------------+
|  Protocol Layer                                       |
|  ProtocolEncoder  .  ProtocolDecoder  .  CommandBuilder|
+-------------------------------------------------------+
|  Connection Layer                                     |
|  ConnectionManager (sync)  .  AsyncConnectionManager  |
+-------------------------------------------------------+
|  Compression                                          |
|  gzip (builtin)  .  snappy / lz4 (extras)            |
+-------------------------------------------------------+
|  Transport: TCP socket / asyncio streams              |
+-------------------------------------------------------+
|  Cursus Broker (Go)  port 9000                        |
+-------------------------------------------------------+
```

---

## Public API Design

### Producer

```python
config = ProducerConfig(
    brokers=["localhost:9000"],
    topic="my-topic",
    partitions=4,
    acks=Acks.ONE,
    batch_size=500,
    linger_ms=100,
)

# Context manager (AutoCloseable 대응)
with Producer(config) as p:
    seq = p.send("Hello, Cursus!")
    seq = p.send("keyed message", key="order-123")
    p.flush()
    print(f"acked={p.unique_ack_count}")

# Async
async with AsyncProducer(config) as p:
    seq = await p.send("Hello, Cursus!")
    await p.flush()
```

### Consumer

Python에서는 이터레이터 패턴을 기본 권장하고, 콜백 방식도 지원한다.

```python
config = ConsumerConfig(
    brokers=["localhost:9000"],
    topic="my-topic",
    group_id="my-group",
    mode=ConsumerMode.STREAMING,
)

# 이터레이터 (Pythonic, 기본 권장)
with Consumer(config) as consumer:
    for msg in consumer:
        print(f"offset={msg.offset} payload={msg.payload}")

# 콜백 (Java/Go 스타일, 호환용)
consumer = Consumer(config)
consumer.start(lambda msg: print(msg.payload))

# Async 이터레이터
async with AsyncConsumer(config) as consumer:
    async for msg in consumer:
        print(msg.payload)
```

### EventStore

```python
es = EventStore(addr="localhost:9000", topic="orders", producer_id="svc-1")

result = es.append(
    key="order-123",
    expected_version=0,
    event=Event(type="OrderCreated", payload='{"amount": 99.99}'),
)
# result.version, result.offset, result.partition

stream = es.read_stream("order-123")
# stream.snapshot (Snapshot | None), stream.events (list[StreamEvent])

version = es.stream_version("order-123")
es.save_snapshot("order-123", version=5, payload='{"state": "..."}')
snapshot = es.read_snapshot("order-123")  # Snapshot | None

# Async
async with AsyncEventStore(...) as es:
    result = await es.append(...)
```

---

## Configuration

### ProducerConfig (dataclass)

Go/Java SDK의 모든 설정 필드를 대응한다. snake_case 네이밍.

| Field | Type | Default | Description |
|---|---|---|---|
| `brokers` | `list[str]` | `["localhost:9000"]` | 브로커 주소 |
| `topic` | `str` | **필수** | 발행 토픽 |
| `partitions` | `int` | `4` | 파티션 수 |
| `acks` | `Acks` | `Acks.ONE` | ACK 레벨 (NONE, ONE, ALL) |
| `batch_size` | `int` | `500` | 배치 크기 |
| `buffer_size` | `int` | `10000` | 파티션 버퍼 크기 |
| `linger_ms` | `int` | `100` | 배치 대기 시간(ms) |
| `max_inflight_requests` | `int` | `5` | 동시 전송 수 |
| `idempotent` | `bool` | `False` | 멱등성 |
| `write_timeout_ms` | `int` | `5000` | 쓰기 타임아웃 |
| `flush_timeout_ms` | `int` | `30000` | flush 타임아웃 |
| `leader_staleness_ms` | `int` | `30000` | 리더 캐시 만료 |
| `compression_type` | `str` | `"none"` | "none", "gzip", "snappy", "lz4" |
| `tls_cert_path` | `str \| None` | `None` | TLS 인증서 경로 |
| `tls_key_path` | `str \| None` | `None` | TLS 키 경로 |
| `max_retries` | `int` | `3` | 재시도 횟수 |
| `max_backoff_ms` | `int` | `10000` | 최대 백오프 |

### ConsumerConfig (dataclass)

| Field | Type | Default | Description |
|---|---|---|---|
| `brokers` | `list[str]` | `["localhost:9000"]` | 브로커 주소 |
| `topic` | `str` | **필수** | 구독 토픽 |
| `group_id` | `str \| None` | `None` | 컨슈머 그룹 ID |
| `consumer_id` | `str \| None` | `None` (자동 생성) | 컨슈머 식별자 |
| `mode` | `ConsumerMode` | `STREAMING` | POLLING / STREAMING |
| `auto_commit_interval_s` | `float` | `5.0` | 자동 커밋 주기(초) |
| `session_timeout_ms` | `int` | `30000` | 세션 타임아웃 |
| `heartbeat_interval_ms` | `int` | `3000` | 하트비트 주기 |
| `max_poll_records` | `int` | `100` | 폴링 시 최대 레코드 수 |
| `batch_size` | `int` | `100` | 배치 힌트 크기 |
| `immediate_commit` | `bool` | `False` | 즉시 커밋 |
| `commit_batch_size` | `int` | `100` | 커밋 배치 크기 |
| `compression_type` | `str` | `"none"` | 압축 알고리즘 |
| `tls_cert_path` | `str \| None` | `None` | TLS 인증서 |
| `tls_key_path` | `str \| None` | `None` | TLS 키 |
| `max_retries` | `int` | `3` | 재시도 횟수 |
| `max_backoff_ms` | `int` | `10000` | 최대 백오프 |

---

## Types

### Message (dataclass)

```python
@dataclass
class Message:
    offset: int
    seq_num: int
    payload: str
    key: str = ""
    producer_id: str = ""
    epoch: int = 0
    event_type: str = ""
    schema_version: int = 0
    aggregate_version: int = 0
    metadata: str = ""
```

### AckResponse (dataclass)

```python
@dataclass
class AckResponse:
    status: str
    last_offset: int
    producer_epoch: int
    producer_id: str
    seq_start: int
    seq_end: int
    leader: str = ""
    error: str = ""
```

### Event, StreamEvent, Snapshot, StreamData, AppendResult

Go SDK의 eventstore.go 타입을 1:1 대응한 dataclass.

### Enums

```python
class Acks(str, Enum):
    NONE = "0"
    ONE = "1"
    ALL = "-1"

class ConsumerMode(str, Enum):
    POLLING = "polling"
    STREAMING = "streaming"
```

---

## Error Hierarchy

Java의 예외 계층을 미러링한다.

```
CursusError (base)
+-- ConnectionError       # 연결 실패, 타임아웃
+-- ProtocolError         # 프로토콜 디코딩/인코딩 오류
+-- ProducerClosedError   # 닫힌 프로듀서에 send 시도
+-- ConsumerClosedError   # 닫힌 컨슈머 조작
+-- TopicNotFoundError    # 존재하지 않는 토픽
+-- NotLeaderError        # 리더가 아닌 브로커에 요청
```

---

## Protocol Layer

Go SDK `protocol.go`를 1:1 대응한다.

### Wire Format

- 4-byte big-endian length prefix + payload
- 최대 프레임 크기: 64MB
- 텍스트 커맨드: UTF-8 문자열
- 배치 메시지: 바이너리 (매직 `0xBA7C`)

### Batch Encoding

Go SDK `EncodeBatchMessages` / `DecodeBatchMessages`와 바이트 레벨 호환. `struct` 모듈 사용.

```
Header: magic(2) + topic(2+N) + partition(4) + acks(1+N) + idempotent(1)
        + seqStart(8) + seqEnd(8) + msgCount(4)
Per-message: offset(8) + seqNum(8) + producerId(2+N) + key(2+N) + epoch(8)
             + payload(4+N) + eventType(2+N) + schemaVersion(4)
             + aggregateVersion(8) + metadata(2+N)
```

### CommandBuilder

Java의 `CommandBuilder`와 동일한 커맨드 생성 함수.

```python
CommandBuilder.create(topic, partitions)      # "CREATE topic=... partitions=..."
CommandBuilder.consume(topic, partition, offset)
CommandBuilder.stream(topic, partition, offset)
CommandBuilder.join_group(topic, group, member)
CommandBuilder.sync_group(topic, group, member, generation)
CommandBuilder.leave_group(topic, group, member)
CommandBuilder.heartbeat(topic, group, member, generation)
CommandBuilder.commit_offset(topic, group, partition, offset, generation, member)
CommandBuilder.batch_commit(topic, group, member, generation, offsets)
CommandBuilder.fetch_offset(topic, partition, group)
```

---

## Compression

### 내장: gzip (stdlib)

### Optional extras:
- `pip install cursus-client[snappy]` -> python-snappy
- `pip install cursus-client[lz4]` -> lz4

### Registry 패턴 (Java CompressionRegistry 대응)

```python
class CursusCompressor(Protocol):
    @property
    def algorithm_name(self) -> str: ...
    def compress(self, data: bytes) -> bytes: ...
    def decompress(self, data: bytes) -> bytes: ...

# 커스텀 알고리즘 등록
CompressionRegistry.register(ZstdCompressor())
```

---

## Connection Layer

### Sync: `socket` (stdlib)
- `ConnectionManager`: TCP 연결 풀, 리더 추적, failover
- length-prefix framing (read/write)
- TLS: `ssl.create_default_context()` 래핑

### Async: `asyncio.open_connection`
- `AsyncConnectionManager`: 동일 로직의 async 버전
- `asyncio.StreamReader` / `StreamWriter` 기반

sync/async 모두 동일한 프로토콜 레이어(encoder/decoder)를 공유한다.

---

## FastAPI Integration (cursus-fastapi)

Java의 Spring Boot Starter를 FastAPI에 대응한다.

### CursusSettings (pydantic-settings)

```yaml
# config.yaml 또는 환경변수
CURSUS_BROKERS: ["localhost:9000"]
CURSUS_PRODUCER_TOPIC: my-topic
CURSUS_CONSUMER_TOPIC: my-topic
CURSUS_CONSUMER_GROUP_ID: my-group
CURSUS_CONSUMER_MODE: streaming
```

### DI (Dependency Injection)

```python
from cursus_fastapi import get_producer, get_consumer

@app.post("/publish")
async def publish(producer: AsyncProducer = Depends(get_producer)):
    await producer.send("hello")
```

### @cursus_listener decorator

```python
from cursus_fastapi import cursus_listener

@cursus_listener(topic="my-topic", group_id="my-group")
async def handle(msg: Message):
    print(msg.payload)
```

lifespan 이벤트에서 자동으로 consumer를 시작/종료한다.

### Health Check

```python
from cursus_fastapi import cursus_health_router
app.include_router(cursus_health_router)
# GET /health/cursus -> {"status": "ok", "broker": "connected"}
```

---

## CI/CD

### ci.yml (Java ci.yml 대응)

| Job | 내용 |
|---|---|
| `test` | `uv run pytest` (Python 3.10, 3.12 매트릭스) |
| `lint` | `ruff check` + `ruff format --check` |
| `type-check` | `mypy` |

### release.yml (Java release.yml 대응)

- workflow_dispatch (버전 입력)
- `uv build` -> `uv publish` (PyPI, Trusted Publisher)
- Git tag + GitHub Release 생성

---

## Go SDK Mapping

| Go SDK | Python SDK |
|---|---|
| `PublisherConfig` | `ProducerConfig` |
| `ConsumerConfig` | `ConsumerConfig` |
| `Message` struct | `Message` dataclass |
| `AckResponse` struct | `AckResponse` dataclass |
| `EncodeMessage` / `EncodeBatchMessages` | `ProtocolEncoder.encode_message()` / `.encode_batch()` |
| `DecodeBatchMessages` | `ProtocolDecoder.decode_batch()` |
| `WriteWithLength` / `ReadWithLength` | `ConnectionManager.write_frame()` / `.read_frame()` |
| `CompressMessage` / `DecompressMessage` | `CompressionRegistry.compress()` / `.decompress()` |
| `BATCH_MAGIC = 0xBA7C` | `ProtocolEncoder.BATCH_MAGIC` |
| `ErrProducerClosed` etc. | `ProducerClosedError` etc. |
| `EventStore.Append()` | `EventStore.append()` |
| `EventStore.ReadStream()` | `EventStore.read_stream()` |
| `EventStore.SaveSnapshot()` | `EventStore.save_snapshot()` |

---

## Examples (cursus-examples)

Java의 6개 예제를 Python으로 대응한다.

### standalone/

| 파일 | 설명 | Java 대응 |
|---|---|---|
| `simple_producer.py` | 기본 메시지 발행 | `SimpleProducer.java` |
| `simple_consumer.py` | 기본 메시지 소비 (이터레이터) | `SimpleConsumer.java` |
| `keyed_producer.py` | 키 기반 파티셔닝 | `KeyedProducer.java` |
| `batch_producer.py` | 배치 설정 튜닝 | `BatchProducer.java` |
| `consumer_group.py` | 컨슈머 그룹 예제 | `ConsumerGroupExample.java` |
| `event_sourcing.py` | EventStore 사용 | (Go SDK 예제 대응) |

### fastapi/

| 파일 | 설명 | Java 대응 |
|---|---|---|
| `app.py` | FastAPI REST + @cursus_listener | `CursusExampleApp.java` + `EventListener.java` |

---

## Documentation (docs/)

Java와 동일한 7개 문서 구성.

| 문서 | 설명 |
|---|---|
| `getting-started.md` | 설치, 브로커 기동, 첫 메시지 |
| `architecture.md` | 모듈 구조, 레이어 다이어그램, Go SDK 매핑 |
| `producer-guide.md` | 배칭, 압축, 멱등성, 모니터링 |
| `consumer-guide.md` | 모드, 그룹, 오프셋, 종료 |
| `protocol.md` | 와이어 포맷, 커맨드, 프레임 구조 |
| `configuration-reference.md` | 전체 설정 필드 (타입, 기본값) |
| `fastapi-integration.md` | 자동 설정, @cursus_listener, health check |
