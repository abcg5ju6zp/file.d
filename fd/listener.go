package fd

import (
	"context"
	"net"
	"syscall"
)

// ReuseListenConfig returns a ListenConfig with SO_REUSEPORT enabled.
// It lets a candidate generation bind a listener while the old generation
// keeps serving the same address; a real external conflict is detected
// during the candidate build, before the old pipeline is touched.
func ReuseListenConfig() net.ListenConfig {
	return net.ListenConfig{
		Control: func(_ string, _ string, c syscall.RawConn) error {
			var setErr error
			if err := c.Control(func(fd uintptr) {
				setErr = syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_REUSEPORT, 1)
			}); err != nil {
				return err
			}
			return setErr
		},
	}
}

// Listen binds a reuseport TCP listener.
func Listen(network, address string) (net.Listener, error) {
	lc := ReuseListenConfig()
	return lc.Listen(context.Background(), network, address)
}

// ListenPacket binds a reuseport packet (UDP) connection.
func ListenPacket(network, address string) (net.PacketConn, error) {
	lc := ReuseListenConfig()
	return lc.ListenPacket(context.Background(), network, address)
}
