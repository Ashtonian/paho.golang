package session

import (
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/eclipse/paho.golang/packets"
	"github.com/eclipse/paho.golang/paho/session/store/memory"
)

// The Session State, as per the MQTT spec, contains:
//
//	> QoS 1 and QoS 2 messages which have been sent to the Server, but have not been completely acknowledged.
//	> QoS 2 messages which have been received from the Server, but have not been completely acknowledged.
//
// and, importantly, is used when resending as follows:
//
// > When a Client reconnects with Clean Start set to 0 and a session is present, both the Client and Server MUST resend
// > any unacknowledged PUBLISH packets (where QoS > 0) and PUBREL packets using their original Packet Identifiers. This
// > is the only circumstance where a Client or Server is REQUIRED to resend messages. Clients and Servers MUST NOT
// > resend messages at any other time
//
// There are a few other areas where the State is important:
//    * When allocating a new packet identifier we need to know what Id's are already in the session state.
//    * If a QOS2 Publish with `DUP=TRUE` is received then we should not pass it to the client if we have previously
//    sent a `PUBREC` (indicating that the message has already been processed).
//    * Some subscribers may need a transaction (i.e. commit transaction when `PUBREL` received), this is not implemented
//       here, but the option is left open.
//
// This means that the following information may need to be retained after the connection is lost:
//  * The IDs of any transactions initiated by the client (so that we don't reuse IDs and can notify the requester (if
//    known) when a response is received or the request is removed from the state).
//  * For client initiated publish:
//     * Outgoing `PUBLISH` packets to allow resend.
//     * Outgoing `PUBREL` packets to allow resend.
//  * For server initiated publish:
//     * Outgoing `PUBREL` packets. The fact that this has been sent indicates that the user app has acknowledged the
//       message, and it should not be re-presented (doing so would breach the "exactly once" requirement).
//     * In memory only - the fact that a QOS2 PUBLISH has been received and sent to the handler, but not acknowledged.
//       This allows us to avoid presenting the message a second time (if the application is restarted, we have no option
//       but to represent it because we have no way of knowing if the application completed handling it).
//
//  It is important to note that there are packets with identifiers that do not form part of the session state
//  (SUBSCRIBE, SUBACK, UNSUBSCRIBE, UNSUBACK). Whilst these will never be stored to disk, it is important to track
//  the packet IDs to ensure we don't reuse them.
//  For packets relating to client-initiated transactions sent during a `session.State` lifetime we also want to link
//  a channel to the message ID so that we can notify our user when the transaction is complete (allowing a call to, for
//  instance `Publish()` to block until the message is fully acknowledged even if we disconnect/reconnect in the interim.

var (
	ErrNoConnection                 = errors.New("no connection available")       // We are not in-between a call to ConAckReceived and ConnectionLost
	ErrorPacketIdentifiersExhausted = errors.New("all packet identifiers in use") // // There are no available Packet IDs
	// free message ids to be used.
)

const (
	midMin uint16 = 1
	midMax uint16 = 65535
)

type (
	// PacketIdAndType must be implemented by any packets to be sent with a packet identifier
	PacketIdAndType interface {
		packets.Packet
		SetIdentifier(uint16) // Sets the packet identifier
		Type() byte           // Gets the packet type
	}

	// clientGenerated holds information on client-generated packets (e.g. an outgoing SUBSCRIBE request)
	clientGenerated struct {
		packetType byte // The type of the last packet sent (i.e. PUBLISH, SUBSCRIBE or UNSUBSCRIBE) - 0 means unknown until loaded from the store

		// When a message is fully acknowledged, we need to let the requester know by sending the final response to this
		// channel. One and only one message will be sent (the channel will then be closed to ensure this!).
		// We also guarantee to always send to the channel (assuming there is a clean shutdown) so that the end user knows
		// the status of the request.
		responseChan chan<- packets.ControlPacket
	}
)

// State manages the session state. The client will send messages that may impact the state via
// us, and we will maintain the session state
type State struct {
	mu                    sync.Mutex // protects whole struct (operations should be quick, so the impact of multiple mutexes is likely to be low)
	connectionLostAt      time.Time  // Time that the connection was lost
	sessionExpiryInterval uint32     // The session expiry interval sent with the most recent CONNECT packet

	conn io.Writer // current connection or nil if we are not connected

	// client store - holds packets where the message ID was generated on the client (i.e. by paho.golang)
	clientPackets map[uint16]clientGenerated // Store relating to messages sent TO the server
	clientStore   storer                     // Used to store session state that survives connection loss
	lastMid       uint16                     // The message ID most recently issued

	// server store - holds packets where the message ID was generated on the server
	serverPackets map[uint16]byte // The last packet received from the server with this ID (cleared when the transaction is complete)
	serverStore   storer          // Used to store session state that survives connection loss

	// queue
	queueActive bool // if true there is data in the queue (this must be sent before anything else)

	debug  Logger
	errors Logger
}

// New creates a new state which will persist information using the passed in storer's.
func New(client storer, server storer) *State {
	return &State{
		clientPackets: make(map[uint16]clientGenerated),
		clientStore:   client,
		serverPackets: make(map[uint16]byte),
		serverStore:   server,
		debug:         NOOPLogger{},
		errors:        NOOPLogger{},
	}
}

// NewInMemory returns a default State that stores all information in memory
func NewInMemory() *State {
	return &State{
		clientPackets: make(map[uint16]clientGenerated),
		clientStore:   memory.New(),
		serverPackets: make(map[uint16]byte),
		serverStore:   memory.New(),
		debug:         NOOPLogger{},
		errors:        NOOPLogger{},
	}
}

// Close closes the session state
func (s *State) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for packetID, cg := range s.clientPackets {
		cg.responseChan <- packets.ControlPacket{} // Default control packet indicates that we are shutting down (TODO: better solution?)
		delete(s.clientPackets, packetID)
	}
	return nil
}

// ConAckReceived will be called when the client receives a CONACK that indicates the connection has been successfully
// established. This indicates that a new connection is live and the passed in connection should be used going forward.
// It is also the trigger to resend any queued messages. Note that this function should not be called concurrently with
// others (we should not begin sending/receiving packets until after the CONACK has been processed).
//
// TODO: Add errors() function so we can notify the client of errors whilst transmitting the session stuff?
// TODO: send any packets in the state that need to be retransmitted.
// For this I think we need a send queue of some form; stuff from the session MUST be sent first (To retain packet order)
// then we need some form of queue when `Publish` is called and there are no slots available (queue with no ID)
// Ideally the `queue` needs to be controlled by another store so that the messages are not held in RAM. Would be
// nice if there is a way to continue adding them when the connection is down.
func (s *State) ConAckReceived(conn io.Writer, cp *packets.Connect, ca *packets.Connack) {
	// We could use cp.Properties.SessionExpiryInterval /  ca.Properties.SessionExpiryInterval to clear the session
	// after the specified time period (if the Session Expiry Interval is absent the value in the CONNECT Packet used)
	// however, this is not something the generic client can really accomplish (forks may wish to do this!).
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.conn != nil {
		s.errors.Println("ConAckReceived called whilst connection active (you MUST call ConnectionLost before starting a new connection")
		_ = s.connectionLost(nil) // assume the connection dropped
	}
	s.conn = conn

	// If the server indicates that this is a cleansession we empty any saved session state
	if !ca.SessionPresent {
		s.clean()
	}

	// If the Server accepts a connection with Clean Start set to 1, the Server MUST set Session Present to 0 in the
	// CONNACK packet in addition to setting a 0x00 (Success) Reason Code in the CONNACK packet [MQTT-3.2.2-2].
	// If the Server accepts a connection with Clean Start set to 0 and the Server has Session State for the ClientID,
	// it MUST set Session Present to 1 in the CONNACK packet, otherwise it MUST set Session Present to 0 in the CONNACK
	// packet. In both cases, it MUST set a 0x00 (Success) Reason Code in the CONNACK packet [MQTT-3.2.2-3].
	//
	// If the Session Expiry Interval is absent the value 0 is used. If it is set to 0, or is absent, the Session ends
	// when the Network Connection is closed (3.1.2.11.2).
	if ca.Properties != nil && ca.Properties.SessionExpiryInterval != nil {
		s.sessionExpiryInterval = *ca.Properties.SessionExpiryInterval
	} else {
		s.sessionExpiryInterval = 0
	}

	// We need to send all queued packets - howto? This may block so we don't really want to just send all packets
	// here with the mu locked (as there may well be responses coming in the other direction)
	// Questions - managing the maximum transmit etc
}

// ConnectionLost will be called when the connection is lost; either because we received a DISCONNECT packet or due
// to a network error (`nil` will be passed in)
func (s *State) ConnectionLost(dp *packets.Disconnect) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.connectionLost(dp)
}

// connectionLost process loss of connection
// Caller MUST have locked c.Mu
func (s *State) connectionLost(dp *packets.Disconnect) error {
	if s.conn == nil {
		return nil // ConnectionLost may be called multiple times (but call ref Disconnect packet should be first)
	}
	s.conn = nil
	s.connectionLostAt = time.Now()

	if dp != nil && dp.Properties != nil && dp.Properties.SessionExpiryInterval != nil {
		s.sessionExpiryInterval = *dp.Properties.SessionExpiryInterval
	}
	// The Client and Server MUST store the Session State after the Network Connection is closed if the Session Expiry
	// Interval is greater than 0 [MQTT-3.1.2-23]
	if s.sessionExpiryInterval == 0 {
		s.clean()
	}
	return nil
}

// StartTransaction begins a client-initiated transaction (i.e. sends a request to which a response is expected).
// This function is responsible for assigning the Packet Identifier.
// If a nil error is returned, we guarantee that one, and only one, message will be sent to resp (assuming `Close()`
// is called).
func (s *State) StartTransaction(packet PacketIdAndType, resp chan<- packets.ControlPacket) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Basic logic... PUBLISH packets only
	// if we have a queue and queueActive is true then
	//     write the message to the queue
	//     if !queue.Immutable() then send error thingy to client
	//     return
	// otherwise attempt to gain semaphore; if this fails
	//     add item to queue
	//     set queueActive = true
	//     start go routine that transmits from queue
	//     if !queue.Immutable() then send error thingy to client
	//     return as above
	//
	// Question: How do we shutdown the goroutine (on close?)
	// ALTERNATIVE - just send a special error if packet needs to be queued and provide a way to be notified when
	//               there is space? (use a sync.Cond for this and call Broadcast everytime a semaphore is freed?
	//    LIKE THIS IDEA - it moves the queuing to the user (i.e. Autopaho) so reduces complexity here)
	//    Could be simpler - just do nonblocking send on a channel every time semaphore is freed?

	//
	if s.conn == nil {
		return ErrNoConnection
	}
	mid, err := s.allocateNextMid(packet.Type(), resp)
	if err != nil {
		return err
	}
	packet.SetIdentifier(mid)
	if _, err := packet.WriteTo(s.conn); err != nil {
		// Should we clear error in this case? Probably not as it should be in store
		return err
	}
	return nil
}

// endClientGenerated should be called when a client-generated transaction has been fully acknowledged
// (or if, due to connection loss, it will never be acknowledged).
func (s *State) endClientGenerated(packetID uint16, recv *packets.ControlPacket) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if cg, ok := s.clientPackets[packetID]; ok {
		cg.responseChan <- *recv
		delete(s.clientPackets, packetID)
		if err := s.clientStore.Delete(packetID); err != nil {
			s.errors.Printf("failed to remove message %d from store: %s", packetID, err)
		}
	} else {
		s.debug.Println("received a response for a message ID we don't know:", recv.PacketID())
	}
	return nil // TODO: Should we return errors here (not much that could be done with them)
}

// Ack is called when the client message handlers have completed (or, if manual acknowledgements are enabled, when
// `client.ACK()` has been called - this may happen some time after the message was received and it is conceivable that
// the connection may have been dropped and reestablished in the interim).
// See issue 160 re issues when the State is called after the connection is dropped. We assume that the
// user will ensure that all ACK's are completed before the State is applied to a new connection (not doing
// this may have unpredictable results).
func (s *State) Ack(pb *packets.Publish) error {
	return s.ack(pb)
}

// ack sends an acknowledgment of the `PUBLISH` (which will have been received from the server)
// `s.mu` must NOT be locked when this is called.
// Note: Adding properties to the response is not currently supported. If this functionality is added, then it is
// important to note that QOS2 PUBREC's may be resent if a duplicate `PUBLISH` is received.
// This function will only return comms related errors (so caller can assume connection has been lost).
func (s *State) ack(pb *packets.Publish) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	var err error
	switch pb.QoS {
	case 1:
		pa := packets.Puback{
			Properties: &packets.Properties{},
			PacketID:   pb.PacketID,
		}
		if s.conn != nil {
			s.debug.Println("sending PUBACK")
			_, err = pa.WriteTo(s.conn)
			if err != nil {
				s.errors.Printf("failed to send PUBACK for %d: %s", pb.PacketID, err)
			}
		} else {
			s.debug.Println("PUBACK not send because connection down")
		}
		// We don't store inbound QOS1 packets. This is because they will be retransmitted if the connection is
		// reestablished before the server reecives the ACK. Unfortunately there is no definitive way to determine if
		// such messages are duplicates or not (so we are forced to treat them all as if they are new).
	case 2:
		pr := packets.Pubrec{
			Properties: &packets.Properties{},
			PacketID:   pb.PacketID,
		}
		s.debug.Printf("sending PUBREC")
		_, err = pr.WriteTo(s.conn)
		if err != nil {
			s.errors.Printf("failed to send PUBREC for %d: %s", pb.PacketID, err)
		}

		// We need to record the fact that a PUBREC has been sent so we can detect receipt of a duplicate `PUBLISH`
		// (which should not be passed to the client app)
		cp := pr.ToControlPacket()
		s.serverStore.Put(cp)
		s.serverPackets[pb.PacketID] = cp.Type
	default:
		err = errors.New("ack called but publish not QOS 1 or 2")
	}
	return err
}

// PacketReceived should be called whenever one of the following is received:
// `PUBLISH` (QOS1+ only), `PUBACK`, `PUBREC`, `PUBREL`, `PUBCOMP`, `SUBACK`, `UNSUBACK`
// It will handle sending any neccessary response or passing the message to the client.
// pubChan will be sent a `PUBLISH` if applicable (and a receiver must be active whilst this function runs)
func (s *State) PacketReceived(recv *packets.ControlPacket, pubChan chan<- *packets.Publish) error {
	// Note: we do a type switch rather than using the packet type because it's safer and easier to understand
	switch rp := recv.Content.(type) {
	//
	// Packets in response to client-generated transactions
	//
	case *packets.Suback: // Not in store, just need to advise client and free Message Identifier
		s.debug.Println("received SUBACK packet with id ", rp.PacketID)
		s.endClientGenerated(rp.PacketID, recv)
		return nil
	case *packets.Unsuback: // Not in store, just need to advise client and free Message Identifier
		s.debug.Println("received UNSUBACK packet with id ", rp.PacketID)
		s.endClientGenerated(rp.PacketID, recv)
		return nil
	case *packets.Puback: // QOS 1 initial (and final) response
		s.debug.Println("received PUBACK packet with id ", rp.PacketID)
		s.clientStore.Delete(rp.PacketID)
		s.endClientGenerated(rp.PacketID, recv)
		return nil
	case *packets.Pubrec: // Initial response to a QOS2 Publish
		s.debug.Println("received PUBREC packet with id ", rp.PacketID)
		s.mu.Lock()
		defer s.mu.Unlock()
		if cg, ok := s.clientPackets[rp.PacketID]; !ok {
			pl := packets.Pubrel{ // Respond with "Packet Identifier not found"
				PacketID:   recv.Content.(*packets.Pubrec).PacketID,
				ReasonCode: 0x92,
			}
			s.debug.Println("sending PUBREL (unknown ID) for ", pl.PacketID)
			_, err := pl.WriteTo(s.conn)
			if err != nil {
				s.errors.Printf("failed to send PUBREL for %d: %s", pl.PacketID, err)
			}
		} else {
			if rp.ReasonCode >= 0x80 {
				// Received a failure code (ending the transaction)
				cg.responseChan <- *recv
				delete(s.clientPackets, rp.PacketID)
				s.clientStore.Delete(rp.PacketID)
			} else {
				pl := packets.Pubrel{
					PacketID: rp.PacketID,
				}
				s.debug.Println("sending PUBREL for", rp.PacketID)
				_, err := pl.WriteTo(s.conn)
				if err != nil {
					s.errors.Printf("failed to send PUBREL for %d: %s", rp.PacketID, err)
				}
			}
		}
		return nil
	case *packets.Pubcomp: // QOS 2 final response
		s.debug.Printf("received PUBCOMP packet with id %d", rp.PacketID)
		s.clientStore.Delete(rp.PacketID)
		s.endClientGenerated(rp.PacketID, recv)
		return nil
		//
		// Packets relating to server generated PUBLISH
		//
	case *packets.Publish:
		s.debug.Printf("received QoS%d PUBLISH", rp.QoS)
		// There is no need to store the packet because it will be resent if not acknowledged before the connection is
		// reestablished.
		if rp.QoS > 0 {
			if rp.PacketID == 0 { // Invalid
				return fmt.Errorf("received QOS %d PUBLISH with 0 PacketID", rp.QoS)
			}
			if rp.QoS == 2 {
				s.mu.Lock()
				defer s.mu.Unlock()
				if lastSent, ok := s.serverPackets[rp.PacketID]; ok {
					// If we have sent a PUBREC, that means that the client has already seen this message, so we can
					// simply resend the acknowledgment.
					if lastSent == packets.PUBREC {
						// If the message is not flagged as a duplicate, then something is wrong; to avoid message loss,
						// we will treat this as a new message (because it appears there is a session mismatch, and we
						// have not actually seen this message).
						if rp.Duplicate {
							// The client has already seen this message meaning we do not want to resend it and, instead
							// immediately acknowledge it.
							s.mu.Unlock() // mu must be unlocked to call ack
							return s.ack(rp)
						}
						s.errors.Printf("received duplicate PUBLISH (%d) but dup flag not set (will assume this overwrites old publish)", rp.PacketID)
					} else {
						s.errors.Printf("received PUBLISH (%d) but lastSent type is %d (unexpected!)", lastSent)
					}
				}
			}
		}
		pubChan <- rp // the message will be passed to router (and thus the end user app)
		return nil
	case *packets.Pubrel:
		s.debug.Println("received PUBREL for", recv.PacketID())
		// Auto respond to pubrels unless failure code
		pr := recv.Content.(*packets.Pubrel)
		if pr.ReasonCode >= 0x80 {
			// Received a failure code meaning the server does not know about the message (so all we can do is to remove
			// it from our store).
			s.errors.Printf("received PUBREL with reason code %d ", pr.ReasonCode)
			return nil
		} else {
			pc := packets.Pubcomp{
				PacketID: pr.PacketID,
			}
			s.mu.Lock()
			defer s.mu.Unlock()
			s.debug.Println("sending PUBCOMP for", pr.PacketID)
			var err error
			if s.conn != nil {
				_, err = pc.WriteTo(s.conn)
				if err != nil {
					s.errors.Printf("failed to send PUBCOMP for %d: %s", pc.PacketID, err)
				}
				// Note: If connection is down we do not clear store (because the server will resend PUBREL upon reconnect)
				delete(s.serverPackets, pr.PacketID)
				if sErr := s.serverStore.Delete(pr.PacketID); sErr != nil {
					s.errors.Printf("failed to remove message %d from server store: %s", pr.PacketID, sErr)
				}
			}
			return err
		}
	default:
		s.errors.Printf("State.PacketReceived received unexpected packet: %#v ", rp)
		return nil
	}
}

// allocateNextMid assigns the next available MID
// Callers must hold lock on s.mu
func (s *State) allocateNextMid(forPacketType byte, resp chan<- packets.ControlPacket) (uint16, error) {
	cg := clientGenerated{
		packetType:   forPacketType,
		responseChan: resp,
	}

	// Scan from lastMid to end of range.
	for i := s.lastMid + 1; i != 0; i++ {
		if _, ok := s.clientPackets[i]; ok {
			continue
		}
		s.clientPackets[i] = cg
		s.lastMid = i
		return i, nil
	}

	// Default struct will set s.lastMid=0 meaning we have already scanned all mids
	if s.lastMid == 0 {
		s.lastMid = 1
		return 0, ErrorPacketIdentifiersExhausted
	}

	// Scan from start of range to lastMid (use +1 to avoid rolling over when s.lastMid = 65535)
	for i := uint16(0); i < s.lastMid; i++ {
		if _, ok := s.clientPackets[i+1]; ok {
			continue
		}
		s.clientPackets[i+1] = cg
		s.lastMid = i + 1
		return i + 1, nil
	}
	return 0, ErrorPacketIdentifiersExhausted
}

// clean deletes any existing stored session information
// caller is responsible for locking s.mu
func (s *State) clean() {
	s.debug.Println("State.clean() called")
	s.serverPackets = make(map[uint16]byte)

	for _, p := range s.clientPackets {
		p.responseChan <- packets.ControlPacket{}
	}
	s.clientPackets = make(map[uint16]clientGenerated)

	s.serverStore.Reset()
	s.clientStore.Reset()
}

// clean deletes any existing stored session information
// as per section 4.1 in the spec; The Session State in the Client consists of:
// > · QoS 1 and QoS 2 messages which have been sent to the Server, but have not been completely acknowledged.
// > · QoS 2 messages which have been received from the Server, but have not been completely acknowledged.
// This means that we keep PUBLISH, PUBREC and PUBREL packets. PUBACK and PUBCOMP will not be stored (the MID
// will be free once they have been sent). PUBREC is retained so we can check newly received PUBLISH messages (and
// confirm if they have already been processed).
// We only resend PUBLISH (where QoS > 0) and PUBREL packets (as per spec section 4.4)
// caller is responsible for locking s.mu
func (s *State) tidy(trigger *packets.ControlPacket) {
	s.debug.Println("State.tidy() called")
	for id, p := range s.serverPackets {
		switch p {
		case packets.PUBREC:
			// For inbound messages, we only retain `PUBREC` messages so that we can determine if a PUBLISH received has
			// already been processed (the `PUBREL` will be sent when the message has been processed by our user).
			// The broker will resend any `PUBLISH` and `PUBREL` messages, so there is no need to retain those.
		default:
			delete(s.serverPackets, id)
			s.serverStore.Delete(id)
		}
	}

	for id, p := range s.clientPackets {
		// We only need to remember `PUBLISH` and `PUBREL` messages (both originating from a PUBLISH)
		if p.packetType != packets.PUBLISH {
			delete(s.clientPackets, id)
			s.clientStore.Delete(id)
			p.responseChan <- packets.ControlPacket{}
		}
	}
}

// SetDebugLogger takes an instance of the paho Logger interface
// and sets it to be used by the debug log endpoint
func (s *State) SetDebugLogger(l Logger) {
	s.debug = l
}

// SetErrorLogger takes an instance of the paho Logger interface
// and sets it to be used by the error log endpoint
func (s *State) SetErrorLogger(l Logger) {
	s.errors = l
}

// AllocateClientPacketIDForTest is intended for use in tests only. It allocates a packet ID in the client session state
// This feels like a hack but makes it easier to test packet identifier exhaustion
func (s *State) AllocateClientPacketIDForTest(packetID uint16, forPacketType byte, resp chan<- packets.ControlPacket) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.clientPackets[packetID] = clientGenerated{
		packetType:   forPacketType,
		responseChan: resp,
	}
}
