package paho

import (
	"errors"
	"sync"

	"github.com/eclipse/paho.golang/packets"
)

// Store - Provides a place to store queued or in transit PUBLISH messages
// The MQTT client will open two stores; one for PUBLISH messages from the server and the other for messages
// originating on the client (these may well share the same basic backend).
//
// as per section 4.1 in the spec; The Session State in the Client consists of:
// > · QoS 1 and QoS 2 messages which have been sent to the Server, but have not been completely acknowledged.
// > · QoS 2 messages which have been received from the Server, but have not been completely acknowledged.
//
// This means that we only need to store PUBLISH (QOS1+) related outbound packets (if we have not responded, the packet
// will be resent). So the only things needed are:
//  `PUBLISH` - This is the only situation where the message body is needed (so we can resend it).
//  `PUBREC` - response to inbound PUBLISH. The only packet originating on the server that will be stored.
//  `PUBREL` - relating to a PUBLISH we initiated
// We do need to store the entire packet because, when resending it, this should include any properties.
//
// The vast majority of operations will be writes. The only time data should be read is:
//    * At startup an inventory of packets held will be needed
//    * At startup we resend any
//    when a connection is established
// (at application startup an inventory of packets held will be needed, after that specific message IDs will be requested).
//
// Design goals:
//   - Simplicity
//   - Store agnostic (allow for memory, disk files, database, REDIS etc)
//   - Minimise data transfers (only read when data is needed)
//   - Allow for properties (we use `packets.ControlPacket` so that a whole packet is stored; when resending we
//     need to include properties etc).

var (
	ErrNotInStore = errors.New("the requested ID was not found in the store") // Returned when requested ID not found
)

// Storer is a generic interface offering the ability to store/amend/retrieve and delete data indexed with a `uint16` key
type Storer interface {
	Put(*packets.ControlPacket) error                    // Store the packet
	Get(packetID uint16) (*packets.ControlPacket, error) // Retrieve the packet with the specified in ID
	Delete(id uint16) error                              // Removes the message with the specified store ID
	List() ([]uint16, error)                             // Returns packet IDs in the order they were Put
	Reset() error                                        // Clears the store (deleting all messages)
}

// memoryPacket is an element in the memory store
type memoryPacket struct {
	c int                    // message count (used for ordering; as this is 32 bit min chance of rolling over seems remote)
	p *packets.ControlPacket // the packet we are storing
}

// NewMemoryStore creates a MemoryStore
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		data: make(map[uint16]memoryPacket),
	}

}

// MemoryStore is an implementation of a Store that stores the data in memory
type MemoryStore struct {
	// server store - holds packets where the message ID was generated on the server
	sync.Mutex
	data map[uint16]memoryPacket // Holds messages initiated by the server (i.e. we will receive the PUBLISH)
	c    int                     // sequence counter used to maintain message order
}

var _ Storer = (*MemoryStore)(nil) // Verify that MemoryStore implements Storer

// Put stores the packet
func (m *MemoryStore) Put(cp *packets.ControlPacket) error {
	m.Lock()
	defer m.Unlock()
	m.data[cp.PacketID()] = memoryPacket{
		c: m.c,
		p: cp,
	}
	m.c++
	return nil
}

func (m *MemoryStore) Get(packetID uint16) (*packets.ControlPacket, error) {
	m.Lock()
	defer m.Unlock()
	d, ok := m.data[packetID]
	if !ok {
		return nil, ErrNotInStore
	}
	return d.p, nil
}

// Delete removes the message with the specified store ID
func (m *MemoryStore) Delete(id uint16) error {
	m.Lock()
	defer m.Unlock()
	delete(m.data, id)
	return nil
}

// List returns packet IDs in the order they were Put
func (m *MemoryStore) List() ([]uint16, error) {
	m.Lock()
	defer m.Unlock()

	ids := make([]uint16, 0, len(m.data))
	seq := make([]int, 0, len(m.data))

	// Basic insert sort from map ordered by time
	// As the map is relatively small, this should be quick enough (data is retrieved infrequently)
	itemNo := 0
	var pos int
	for i, v := range m.data {
		for pos = 0; pos < itemNo; pos++ {
			if seq[pos] > v.c {
				break
			}
		}
		ids = append(ids[:pos], append([]uint16{i}, ids[pos:]...)...)
		seq = append(seq[:pos], append([]int{v.c}, seq[pos:]...)...)
		itemNo++
	}
	return ids, nil
}

// Reset clears the store (deleting all messages)
func (m *MemoryStore) Reset() error {
	m.Lock()
	defer m.Unlock()
	m.data = make(map[uint16]memoryPacket)
	return nil
}
