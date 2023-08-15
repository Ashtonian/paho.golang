package session

import "github.com/eclipse/paho.golang/packets"

// storer must be implemented by session state stores
type storer interface {
	Put(*packets.ControlPacket) error                    // Store the packet
	Get(packetID uint16) (*packets.ControlPacket, error) // Retrieve the packet with the specified in ID
	Delete(id uint16) error                              // Removes the message with the specified store ID
	List() ([]uint16, error)                             // Returns packet IDs in the order they were Put
	Reset() error                                        // Clears the store (deleting all messages)
}
