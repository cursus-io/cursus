package producer

import (
	"crypto/tls"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cursus-io/cursus/test/publisher/config"
	"github.com/google/uuid"
)

const defaultLeaderStalenessThreshold = 30 * time.Second

type leaderInfo struct {
	addr    string
	updated time.Time
}

type ProducerState struct {
	ProducerID  string         `json:"producer_id"`
	LastSeqNums map[int]uint64 `json:"last_seq_nums"`
	Epoch       int64          `json:"epoch"`
}

type ProducerClient struct {
	ID           string
	globalSeqNum atomic.Uint64

	Epoch  int64
	mu     sync.RWMutex
	conns  atomic.Pointer[[]net.Conn]
	config *config.PublisherConfig

	leader atomic.Pointer[leaderInfo]
}

func NewProducerClient(partitions int, config *config.PublisherConfig) *ProducerClient {
	pc := &ProducerClient{
		ID:     uuid.New().String(),
		Epoch:  time.Now().UnixNano(),
		config: config,
	}

	pc.leader.Store(&leaderInfo{
		addr:    "",
		updated: time.Time{},
	})

	return pc
}

func (pc *ProducerClient) NextSeqNum(partition int) uint64 {
	return pc.globalSeqNum.Add(1)
}

func (pc *ProducerClient) connectPartitionLocked(idx int, addr string, useTLS bool, certPath, keyPath string) error {
	if idx < 0 {
		return fmt.Errorf("invalid partition index: %d", idx)
	}

	var conn net.Conn
	var err error

	if useTLS {
		var cert tls.Certificate
		cert, err = tls.LoadX509KeyPair(certPath, keyPath)
		if err != nil {
			return fmt.Errorf("load TLS cert: %w", err)
		}
		conn, err = tls.Dial("tcp", addr, &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12})
		if err != nil {
			return fmt.Errorf("TLS dial to %s failed: %w", addr, err)
		}
	} else {
		conn, err = net.Dial("tcp", addr)
		if err != nil {
			return fmt.Errorf("TCP dial to %s failed: %w", addr, err)
		}
	}

	var currentConns []net.Conn
	if ptr := pc.conns.Load(); ptr != nil {
		currentConns = *ptr
	}

	newSize := idx + 1
	if len(currentConns) > newSize {
		newSize = len(currentConns)
	}

	tmp := make([]net.Conn, newSize)
	copy(tmp, currentConns)
	tmp[idx] = conn

	pc.conns.Store(&tmp)
	return nil
}

func (pc *ProducerClient) GetConn(part int) net.Conn {
	ptr := pc.conns.Load()
	if ptr == nil {
		return nil
	}
	conns := *ptr
	if part >= 0 && part < len(conns) {
		return conns[part]
	}
	return nil
}

func (pc *ProducerClient) Close() error {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	ptr := pc.conns.Swap(nil)
	if ptr == nil {
		return nil
	}

	conns := *ptr
	for i, c := range conns {
		if c != nil {
			_ = c.Close()
			conns[i] = nil
		}
	}

	return nil
}

func (pc *ProducerClient) GetLeaderAddr() string {
	info := pc.leader.Load()
	if info == nil || info.addr == "" {
		return ""
	}
	return info.addr
}

func (pc *ProducerClient) UpdateLeader(leaderAddr string) {
	old := pc.leader.Load()
	if old != nil && old.addr == leaderAddr {
		return
	}

	pc.leader.Store(&leaderInfo{
		addr:    leaderAddr,
		updated: time.Now(),
	})
}

func (pc *ProducerClient) selectBroker() string {
	if pc.config == nil || len(pc.config.BrokerAddrs) == 0 {
		return ""
	}

	info := pc.leader.Load()
	if info != nil && info.addr != "" && time.Since(info.updated) < defaultLeaderStalenessThreshold {
		return info.addr
	}

	return pc.config.BrokerAddrs[0]
}

func (pc *ProducerClient) ConnectPartition(idx int, addr string, useTLS bool, certPath, keyPath string) error {
	if addr == "" {
		addr = pc.selectBroker()
	}
	if addr == "" {
		return fmt.Errorf("no broker address available for partition %d", idx)
	}

	pc.mu.Lock()
	defer pc.mu.Unlock()

	return pc.connectPartitionLocked(idx, addr, useTLS, certPath, keyPath)
}

func (pc *ProducerClient) ReconnectPartition(idx int, addr string, useTLS bool, certPath, keyPath string) error {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	oldPtr := pc.conns.Load()
	if oldPtr != nil {
		conns := *oldPtr
		if idx < len(conns) && conns[idx] != nil {
			_ = conns[idx].Close()
		}
	}

	return pc.connectPartitionLocked(idx, addr, useTLS, certPath, keyPath)
}
