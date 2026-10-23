package coordinator

import "fmt"

// PreserveConsumerReplayClocks selects only partitions whose ownership proof
// remains unchanged. Restore/startup and newly acquired partitions must not
// inherit old session windows. No clock is persisted in a replay index.
func (c *Coordinator) PreserveConsumerReplayClocks(p *PreparedConsumerMetadataReplay, route ConsumerMetadataRoute, unchanged map[int]bool) error {
	if p == nil || p.owner != c || p.scratch == nil || p.scope != nil || route == nil {
		return fmt.Errorf("invalid replay clock preservation request")
	}
	if err := c.validateConsumerMetadataReplayStamp(p.stamp); err != nil {
		return err
	}
	p.preserveClocks = make(map[string]bool)
	for name := range p.scratch.groups {
		partition, err := route(name)
		if err != nil {
			return err
		}
		if unchanged[partition] {
			p.preserveClocks[name] = true
		}
	}
	return nil
}
