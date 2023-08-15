package paho

import (
	"io"

	"github.com/eclipse/paho.golang/packets"
	"github.com/eclipse/paho.golang/paho/session"
)

// SessionManager will manage the mqtt session state; note that the state may outlast a single `Client` instance
type SessionManager interface {
	// ConAckReceived must be called when a CONNACK has been received (with no error). The session
	//
	ConAckReceived(io.Writer, *packets.Connect, *packets.Connack)

	// ConnectionLost must be called whenever the connection is lost or a DISCONNECT packet is received. It can be
	// called multiple times for the same event as long as ConAckReceived is not called in the interim.
	ConnectionLost(dp *packets.Disconnect) error

	// StartTransaction should be called to transmit any packet requiring a packet identifier. It will add the
	// identifier and send the packet (to the connection provided via ConAckReceived).
	// The return of an error indicates that there is an issue with the connection.
	// If no error is returned then a message will be sent to the channel passed in. This message may be nil (if the
	// session ends before a response is received).
	// TODO: Queuing publish packets
	StartTransaction(session.PacketIdAndType, chan<- packets.ControlPacket) error

	// PacketReceived must be called when any packet with a packet identifier is received. It will make any required
	// response and pass any publish messages that need to be passed to the user via the channel.
	PacketReceived(*packets.ControlPacket, chan<- *packets.Publish) error

	// Ack must be called when the client message handlers have completed (or, if manual acknowledgements are enabled,
	// when`client.ACK()` has been called - note the potential issues discussed in issue #160.
	Ack(pb *packets.Publish) error

	// Close shuts down the session store, this will release any blocked calls
	// Note: `paho` will only call this if it created the session (i.e. it was not passed in the config)
	Close() error

	// SetErrorLogger enables error logging via the passed logger (not thread safe)
	SetErrorLogger(l session.Logger)

	// SetDebugLogger enables debug logging via the passed logger (not thread safe)
	SetDebugLogger(l session.Logger)
}
