package cluster

import "net"

// Admission and WaitGroup.Add share the same lock as the closed transition.
func (h *ClusterServer) admitConnection(conn net.Conn) bool {
	h.lifecycleMu.Lock()
	defer h.lifecycleMu.Unlock()
	if h.closed {
		return false
	}
	select {
	case h.connectionSlot <- struct{}{}:
	default:
		return false
	}
	h.connections[conn] = struct{}{}
	h.workers.Add(1)
	return true
}

// Close cancels requests, closes sockets, and joins all admitted handlers before
// the caller may close the source's storage. Concurrent callers join the same close.
func (h *ClusterServer) Close() error {
	h.lifecycleMu.Lock()
	if h.closed {
		done := h.closeDone
		h.lifecycleMu.Unlock()
		<-done
		return nil
	}
	h.closed = true
	listeners := make([]net.Listener, 0, len(h.listeners))
	for ln := range h.listeners {
		listeners = append(listeners, ln)
	}
	connections := make([]net.Conn, 0, len(h.connections))
	for conn := range h.connections {
		connections = append(connections, conn)
	}
	h.lifecycleMu.Unlock()
	h.cancel()
	for _, ln := range listeners {
		_ = ln.Close()
	}
	for _, conn := range connections {
		_ = conn.Close()
	}
	h.workers.Wait()
	close(h.closeDone)
	return nil
}
