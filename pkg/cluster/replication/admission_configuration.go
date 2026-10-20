package replication

import (
	"bytes"
	"fmt"
	"reflect"

	"github.com/hashicorp/raft"
)

// VoterAdmissionIndex recovers the configuration index omitted by the library's
// GetConfiguration future. Only new-voter admission needs this scan. The caller
// passes the observed configuration and AddVoter still checks the returned index
// atomically against Raft's latest configuration; a concurrent change must retry.
func (rm *RaftReplicationManager) VoterAdmissionIndex(expected raft.Configuration) (uint64, error) {
	if rm.raftStore == nil || rm.snapshots == nil {
		return 0, fmt.Errorf("raft configuration storage unavailable")
	}
	return voterAdmissionIndex(rm.raftStore, rm.snapshots, expected)
}

func voterAdmissionIndex(logs raft.LogStore, snapshots raft.SnapshotStore, expected raft.Configuration) (uint64, error) {
	stored, err := snapshots.List()
	if err != nil {
		return 0, err
	}
	var snapshot *raft.SnapshotMeta
	for _, candidate := range stored {
		if snapshot == nil || candidate.Index > snapshot.Index {
			snapshot = candidate
		}
	}
	last, err := logs.LastIndex()
	if err != nil {
		return 0, err
	}
	first, err := logs.FirstIndex()
	if err != nil {
		return 0, err
	}
	encoded := raft.EncodeConfiguration(expected)
	for index := last; index > 0 && index >= first; index-- {
		if snapshot != nil && index <= snapshot.ConfigurationIndex {
			break
		}
		var entry raft.Log
		if err := logs.GetLog(index, &entry); err != nil {
			return 0, err // Concurrent compaction is retryable, never index zero.
		}
		if entry.Type == raft.LogConfiguration {
			if !bytes.Equal(entry.Data, encoded) {
				return 0, fmt.Errorf("raft configuration changed during voter admission")
			}
			return index, nil
		}
	}
	if snapshot != nil && snapshot.ConfigurationIndex > 0 && reflect.DeepEqual(snapshot.Configuration, expected) {
		return snapshot.ConfigurationIndex, nil
	}
	return 0, fmt.Errorf("matching raft configuration index unavailable")
}
