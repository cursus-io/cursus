package controller

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	clusterController "github.com/cursus-io/cursus/pkg/cluster/controller"
	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/types"
)

func (ch *CommandHandler) parseConsumerRoute(encoded, topic string, partition int, client *ClientContext) (*types.ConsumerMetadataRoute, error) {
	if encoded == "" {
		return nil, nil
	}
	if client == nil || !client.Internal || topic != config.ConsumerOffsetsTopicName || len(encoded) > 16384 {
		return nil, fmt.Errorf("consumer metadata route requires internal offsets publish")
	}
	data, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil {
		return nil, fmt.Errorf("decode consumer metadata route: %w", err)
	}
	var route types.ConsumerMetadataRoute
	decoder := json.NewDecoder(strings.NewReader(string(data)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&route); err != nil {
		return nil, fmt.Errorf("decode consumer metadata route: %w", err)
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return nil, fmt.Errorf("trailing consumer metadata route data")
	}
	if !ch.isDistributed() || ch.Cluster == nil || ch.Cluster.RaftManager == nil || ch.Cluster.RaftManager.GetFSM() == nil {
		return nil, fmt.Errorf("consumer metadata route requires distributed metadata")
	}
	if proof := route.Transaction; proof != nil {
		if response := ch.waitForReplicatedTransactionState([]types.Message{{TransactionalID: proof.ID, Epoch: proof.ProducerEpoch}}, proof.Revision, DefaultFSMApplyTimeout); response != "" {
			return nil, fmt.Errorf("transaction route state pending: %s", response)
		}
	}
	if err := ch.Cluster.RaftManager.GetFSM().ValidateConsumerMetadataRoute(topic, partition, &route); err != nil {
		return nil, err
	}
	return &route, nil
}

func validateConsumerRouteSnapshot(route *types.ConsumerMetadataRoute, snapshot clusterController.PartitionReplicationSnapshot) error {
	if route != nil && (route.Owner != snapshot.Leader || route.LeaderEpoch != snapshot.LeaderEpoch || route.LifecycleEpoch != snapshot.LifecycleEpoch || route.PartitionCount != snapshot.PartitionCount) {
		return fmt.Errorf("consumer metadata route changed before append")
	}
	return nil
}

func (ch *CommandHandler) transactionConsumerRouteArgument(topic string, partition int, id string) (string, error) {
	if topic != config.ConsumerOffsetsTopicName || !ch.isDistributed() {
		return "", nil
	}
	if !ch.Cluster.RaftManager.GetFSM().ConsumerMetadataRoutingEnabled() {
		return "", nil
	}
	route, err := ch.Cluster.RaftManager.GetFSM().ConsumerTransactionMetadataRoute(ch.Cluster.Router.BrokerID(), id, partition)
	if err != nil {
		return "", err
	}
	data, err := json.Marshal(route)
	if err != nil {
		return "", err
	}
	return " consumer_route=" + base64.RawURLEncoding.EncodeToString(data), nil
}
