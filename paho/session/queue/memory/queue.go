package memory

import "github.com/eclipse/paho.golang/packets"

// queue implements a basic queue in memory
type queue struct {
	packets []packets.Packet
}

// Add - adds a packet to the queue
func (q *queue) Add(p packets.Packet) {
	q.packets = append(q.packets, p)
}

// Next - retrieves a packet from the queue
// Note that we do not move the remaining data so that the backing array will be periodically resized.
func (q *queue) Next() packets.Packet {
	if len(q.packets) == 0 {
		return nil
	}
	p := q.packets[0]
	q.packets = q.packets[:1]
	return p
}

// Empty returns true if the queue is empty
func (q *queue) Empty() bool {
	return len(q.packets) == 0
}

// Immutable returns true if returned packets will be identical to those passed in. This is only possible when they
// are stored in RAM.
func (q *queue) Immutable() bool {
	return true
}
