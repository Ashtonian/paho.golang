package paho

import "github.com/eclipse/paho.golang/packets"

// CPContext.Context is never used so should be removed

type PersistenceX interface {
	Open()
	Put(uint16, packets.ControlPacket)
	Get(uint16) packets.ControlPacket
	All() []packets.ControlPacket
	Delete(uint16)
	Close()
	Reset()
}

type storeInfo interface {
}

type Store interface {
	RequestID(c *CPContext) (uint32, error) // Retrieve a new message ID
	Get(i uint16) *CPContext

	Open()
	Put(uint16, packets.ControlPacket)
	Getx(uint16) packets.ControlPacket
	All() []packets.ControlPacket
	Delete(uint16)
	Close()
	Reset()
}

type store struct {
}
