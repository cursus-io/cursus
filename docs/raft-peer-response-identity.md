# Raft response identity after address reuse

## Evidence and design before implementation

The commit-application crash test repeatedly recovered the acknowledged prefix
on two nodes but failed to rejoin the original leader. Its Raft log stayed at
index 22 while the surviving leader reached 37 and beyond. Stack captures ruled
out a blocked FSM or recovery worker. With two stopped containers, Docker reused
an address for a different broker. A surviving Raft connection then reported:
`target=broker-3-9000 response_id=broker-2-9000 success=true`. A TCP connection and
a successful response were being mistaken for evidence from the intended voter.
Changing the Raft leader rebuilt connections and sometimes hid the failure.

Kafka's accepted [KIP-853](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=217391519)
separates voter identity from endpoints. The versioned
[Kafka 4.0.0 KafkaRaftClient](https://github.com/apache/kafka/blob/4.0.0/raft/src/main/java/org/apache/kafka/raft/KafkaRaftClient.java)
checks the intended voter key in vote and epoch-establishment requests; the key
includes the node ID and, when supplied, directory ID. Adopt explicit voter
identity checks rather than treating an address as identity.

Cursus uses HashiCorp Raft 1.7.3 with its existing RPC headers, not Kafka's fetch
protocol or directory-key membership. Its responses already carry a server ID.
The implemented correction validates that ID against the requested voter for
append/heartbeat, vote and pre-vote responses.
Missing or different IDs fail closed. Pipelined appends also need validation:
HashiCorp's pipeline decoder consumes `Response().Success`, without checking the
future's error. A rejected future must therefore expose an unsuccessful response
as well as an error. Discard pooled connections after a mismatch so the next
attempt resolves the endpoint again. Keep the current wire/storage formats and
TLS configuration. This checks accidental misrouting, not cryptographic identity
or directory-incarnation fencing; TLS remains the authentication mechanism.

Safety takes precedence over availability: a mismatched reply cannot count as a
vote or acknowledged append. A mistaken request can already
have reached another member before response validation; this change does not add
a receiver-side target field. Reconnection may interrupt other pooled RPCs, which
must use existing retry/recovery. It adds no retained-data scan/hash/rewrite;
rejected in-flight requests and re-established connections can add network work.

Before implementation, a real TCP transport regression must show the old code
accepting another server's ID, then reject mismatched/missing IDs for quorum RPCs,
including both append-future access paths. Correct IDs must still work, a stale
pooled connection must be discarded, and closing a blocked pipeline must join
its relay. Finally rerun the two-node crash with original volumes, full decoded
record verification, and stable three-member ISR. Retain failed runs and their
diagnostic evidence.

Inspection of [HashiCorp Raft 1.7.3 raft.go](https://github.com/hashicorp/raft/blob/v1.7.3/raft.go)
refined the initial all-response proposal: `installSnapshot` and `timeoutNow`
construct responses without RPC headers. Those two methods retain their existing
transport behavior, with a TCP regression using the actual headerless shapes.
Rejecting these replies would break compatible snapshot recovery and leadership
transfer. A snapshot covers an already committed prefix; future append and vote
evidence still requires the requested ID. Receiver-side target validation and
identity-bearing snapshot/transfer replies would need a separate protocol change.

## Deterministic validation

The pre-fix TCP regression accepted both missing and different response IDs for
append, vote, pre-vote and pipelined appends. The corrected tests reject
them and retain normal replies. A counting listener verifies that rejecting a
pooled response causes the next request to use a fresh TCP connection. Pipeline
checks cover both the returned future and completion stream, rejection of the
wrong member's successful/higher-term response, and closure without consuming
the completion stream. The two access paths share a checked future: their first
race run exposed concurrent calls to HashiCorp's non-thread-safe `Future.Error`;
the shared result fixes that race. The entire replication package subsequently
passed under the Linux race detector, including the headerless-response
compatibility regression (22.830 seconds). The identity checks also passed five
consecutive race-enabled runs.

The corrected Docker reproduction kept broker 1 as the Raft leader while the
other two restarted with exchanged addresses. It rejected responses from broker
2 when requesting broker 3, and vice versa, rather than counting them. All three
replicas then rejoined with committed HWM 10. This
exercises the previously failing topology without pinning container IPs or
extending the small-replica recovery deadline. The complete commit-application
crash scenario passed in 143.69 seconds, including the unchanged 65-second
stable-ISR observation and full ten-record comparison on all replicas and the
public consumer.
