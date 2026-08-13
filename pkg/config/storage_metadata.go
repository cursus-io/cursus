package config

const (
	// TopicMetadataFileName is the broker-owned standalone topic manifest.
	TopicMetadataFileName = "__topic_metadata.json"
	// ConsumerOffsetsTopicName is the broker-owned standalone consumer metadata log.
	ConsumerOffsetsTopicName = "__consumer_offsets"
	// ConsumerMetadataMigrationFileName records an explicit pre-manifest recovery selection.
	ConsumerMetadataMigrationFileName = "__consumer_metadata_migration.json"
)
