package natslog

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	"github.com/nats-io/go-nats"
)

const (
	natsConnectInbox = "raft.%s.accept"
	natsRequestInbox = "raft.%s.request.%s"

	defaultBufferSize = 32 * 1024
)

type natsAddr string

func (n natsAddr) Network() string {
	return "nats"
}

func (n natsAddr) String() string {
	return string(n)
}

type connectRequestProto struct {
	ID    string `json:"id"`
	Inbox string `json:"inbox"`
}

type connectResponseProto struct {
	Inbox string `json:"inbox"`
}

type requestProto struct {
	Disconnect bool   `json:"disconnect"`
	Data       []byte `json:"data"`
}

type natsConn struct {
	parent     *natsStreamLayer
	localAddr  natsAddr
	remoteAddr natsAddr
	sub        *nats.Subscription
	outbox     string
	mu         sync.RWMutex
	closed     bool
	reader     *timeoutReader
	writer     io.WriteCloser
}

func newNATSConn(n *natsStreamLayer, address string) *natsConn {
	// TODO: probably want a buffered pipe.
	reader, writer := io.Pipe()
	return &natsConn{
		parent:     n,
		localAddr:  n.localAddr,
		remoteAddr: natsAddr(address),
		reader:     newTimeoutReader(reader),
		writer:     writer,
	}
}

func (n *natsConn) Read(b []byte) (int, error) {
	n.mu.RLock()
	closed := n.closed
	n.mu.RUnlock()
	if closed {
		return 0, errors.New("read from closed conn")
	}
	return n.reader.Read(b)
}

func (n *natsConn) Write(b []byte) (int, error) {
	n.mu.RLock()
	closed := n.closed
	n.mu.RUnlock()
	if closed {
		return 0, errors.New("write to closed conn")
	}

	// TODO: handle MAX_PAYLOAD_SIZE limit here.
	// TODO: also JSON is inefficient and need to pool proto structs.
	requestProto := &requestProto{Data: b}
	data, err := json.Marshal(requestProto)
	if err != nil {
		panic(err)
	}
	if err := n.parent.conn.Publish(n.outbox, data); err != nil {
		return 0, err
	}

	return len(b), nil
}

func (n *natsConn) Close() error {
	return n.close(true)
}

func (n *natsConn) close(signalRemote bool) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.closed {
		return nil
	}

	if err := n.sub.Unsubscribe(); err != nil {
		return err
	}

	if signalRemote {
		// Send disconnect proto to peer for graceful disconnect.
		proto := &requestProto{Disconnect: true}
		data, err := json.Marshal(proto)
		if err != nil {
			panic(err)
		}
		// Not concerned with errors here as this is best effort.
		n.parent.conn.Publish(n.outbox, data)
		n.parent.conn.Flush()
	}

	n.closed = true
	n.writer.Close()
	n.reader.Close()

	return nil
}

func (n *natsConn) LocalAddr() net.Addr {
	return n.localAddr
}

func (n *natsConn) RemoteAddr() net.Addr {
	return n.remoteAddr
}

func (n *natsConn) SetDeadline(t time.Time) error {
	if err := n.SetReadDeadline(t); err != nil {
		return err
	}
	return n.SetWriteDeadline(t)
}

func (n *natsConn) SetReadDeadline(t time.Time) error {
	n.reader.SetDeadline(t)
	return nil
}

func (n *natsConn) SetWriteDeadline(t time.Time) error {
	return nil
}

func (n *natsConn) msgHandler(msg *nats.Msg) {
	var proto requestProto
	if err := json.Unmarshal(msg.Data, &proto); err != nil {
		n.parent.logger.Println("[ERR] raft-nats: Invalid request message (invalid data)")
		return
	}

	// Check if remote peer disconnected.
	if proto.Disconnect {
		n.close(false)
	}

	n.writer.Write(proto.Data)
}

type natsStreamLayer struct {
	conn      *nats.Conn
	localAddr natsAddr
	sub       *nats.Subscription
	logger    *log.Logger
}

func newNATSStreamLayer(id string, conn *nats.Conn, logger *log.Logger) (*natsStreamLayer, error) {
	var (
		n        = &natsStreamLayer{localAddr: natsAddr(id), conn: conn, logger: logger}
		sub, err = conn.SubscribeSync(fmt.Sprintf(natsConnectInbox, id))
	)
	n.sub = sub
	conn.Flush()
	return n, err
}

func (n *natsStreamLayer) Dial(address string, timeout time.Duration) (net.Conn, error) {
	// QUESTION: The Raft NetTransport does connection pooling, which is useful
	// for TCP sockets. The NATS transport simulates a socket using a
	// subscription at each endpoint, but everything goes over the same NATS
	// socket. This means there is little advantage to pooling here currently.
	// Should we actually Dial a new NATS connection here and rely on pooling?

	connect := &connectRequestProto{
		ID:    n.localAddr.String(),
		Inbox: fmt.Sprintf(natsRequestInbox, n.localAddr.String(), nats.NewInbox()),
	}
	data, err := json.Marshal(connect)
	if err != nil {
		panic(err)
	}

	peerConn := newNATSConn(n, address)

	// Setup inbox.
	sub, err := n.conn.Subscribe(connect.Inbox, peerConn.msgHandler)
	if err != nil {
		return nil, err
	}
	peerConn.sub = sub
	n.conn.Flush()

	// Make connect request to peer.
	msg, err := n.conn.Request(fmt.Sprintf(natsConnectInbox, address), data, timeout)
	if err != nil {
		sub.Unsubscribe()
		return nil, err
	}
	var resp connectResponseProto
	if err := json.Unmarshal(msg.Data, &resp); err != nil {
		sub.Unsubscribe()
		return nil, err
	}

	peerConn.outbox = resp.Inbox
	return peerConn, nil
}

func (n *natsStreamLayer) Accept() (net.Conn, error) {
	for {
		msg, err := n.sub.NextMsgWithContext(context.TODO())
		if err != nil {
			return nil, err
		}
		if msg.Reply == "" {
			n.logger.Println("[ERR] raft-nats: Invalid connect message (missing reply inbox)")
			continue
		}

		var connect connectRequestProto
		if err := json.Unmarshal(msg.Data, &connect); err != nil {
			n.logger.Println("[ERR] raft-nats: Invalid connect message (invalid data)")
			continue
		}

		peerConn := newNATSConn(n, connect.ID)
		peerConn.outbox = connect.Inbox

		// Setup inbox for peer.
		inbox := fmt.Sprintf(natsRequestInbox, n.localAddr.String(), nats.NewInbox())
		sub, err := n.conn.Subscribe(inbox, peerConn.msgHandler)
		if err != nil {
			n.logger.Printf("[ERR] raft-nats: Failed to create inbox for remote peer: %v", err)
			continue
		}
		peerConn.sub = sub

		// Reply to peer.
		resp := &connectResponseProto{Inbox: inbox}
		data, err := json.Marshal(resp)
		if err != nil {
			panic(err)
		}
		if err := n.conn.Publish(msg.Reply, data); err != nil {
			n.logger.Printf("[ERR] raft-nats: Failed to send connect response to remote peer: %v", err)
			sub.Unsubscribe()
			continue
		}
		n.conn.Flush()

		return peerConn, nil
	}
}

func (n *natsStreamLayer) Close() error {
	return n.sub.Unsubscribe()
}

func (n *natsStreamLayer) Addr() net.Addr {
	return n.localAddr
}

func NewNATSTransport(id string, conn *nats.Conn, timeout time.Duration, logOutput io.Writer) (*raft.NetworkTransport, error) {
	if logOutput == nil {
		logOutput = os.Stderr
	}
	return NewNATSTransportWithLogger(id, conn, timeout, log.New(logOutput, "", log.LstdFlags))
}

func NewNATSTransportWithLogger(id string, conn *nats.Conn, timeout time.Duration, logger *log.Logger) (*raft.NetworkTransport, error) {

	return newNATSTransport(id, conn, timeout, logger, func(stream raft.StreamLayer) *raft.NetworkTransport {
		return raft.NewNetworkTransportWithLogger(stream, 3, timeout, logger)
	})
}

func newNATSTransport(id string, conn *nats.Conn, timeout time.Duration, logger *log.Logger,
	transportCreator func(stream raft.StreamLayer) *raft.NetworkTransport) (*raft.NetworkTransport, error) {

	stream, err := newNATSStreamLayer(id, conn, logger)
	if err != nil {
		return nil, err
	}

	return transportCreator(stream), nil
}
