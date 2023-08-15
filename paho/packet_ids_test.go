package paho

import (
	"context"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/packets"
	"github.com/eclipse/paho.golang/paho/internal/basictestserver"
	"github.com/eclipse/paho.golang/paho/session"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/semaphore"
)

// TestPackedIdNoExhaustion tests interactions between Publish and the session ensuring that IDs are
// released and reused
func TestPackedIdNoExhaustion(t *testing.T) {
	ts := basictestserver.New(&testLog{l: t, prefix: "TestServer:"})
	ts.SetResponse(packets.PUBACK, &packets.Puback{
		ReasonCode: packets.PubackSuccess,
		Properties: &packets.Properties{},
	})
	go ts.Run()
	defer ts.Stop()

	c := NewClient(ClientConfig{
		Conn: ts.ClientConn(),
	})
	require.NotNil(t, c)

	c.serverInflight = semaphore.NewWeighted(10)
	c.stop = make(chan struct{})
	c.publishPackets = make(chan *packets.Publish)
	go c.incoming()
	go c.PingHandler.Start(c.Conn, 30*time.Second)
	c.Session.ConAckReceived(c.Conn, &packets.Connect{}, &packets.Connack{})

	for i := 0; i < 70000; i++ {
		p := &Publish{
			Topic:   "test/1",
			QoS:     1,
			Payload: []byte("test payload"),
		}

		pa, err := c.Publish(context.Background(), p)
		require.Nil(t, err)
		assert.Equal(t, uint8(0), pa.ReasonCode)
	}

	time.Sleep(10 * time.Millisecond)
}

// TestPacketIdExhaustion confirm that an attempt to publish more messages than IDs are available will result in the
// appropriate error
func TestPacketIdExhaustion(t *testing.T) {
	clientLogger := &testLog{l: t, prefix: "MidExhaustion:"}
	ts := basictestserver.New(&testLog{l: t, prefix: "TestServer:"})
	go ts.Run()
	defer ts.Stop()

	ss := session.NewInMemory()
	for i := uint16(1); i != 0; i++ {
		ss.AllocateClientPacketIDForTest(i, packets.PUBLISH, make(chan packets.ControlPacket, 1))
	}

	c := NewClient(ClientConfig{
		Conn:    ts.ClientConn(),
		Session: ss,
	})
	require.NotNil(t, c)

	c.serverInflight = semaphore.NewWeighted(10)
	c.stop = make(chan struct{})
	c.publishPackets = make(chan *packets.Publish)
	c.Session.ConAckReceived(c.Conn,
		&packets.Connect{CleanStart: false}, // Not currently checkes
		&packets.Connack{
			SessionPresent: true, // Session must be present or session will be cleared
		})
	c.SetErrorLogger(clientLogger)
	c.SetDebugLogger(clientLogger)

	// Publish enough messages to use all IDs - each in its own go routine
	_, err := c.Publish(context.Background(), &Publish{
		Topic:   "test/1",
		QoS:     1,
		Payload: []byte("test payload"),
	})
	assert.ErrorIs(t, err, session.ErrorPacketIdentifiersExhausted)
	_ = c.Disconnect(&Disconnect{})

	ss.Close() // Important as we created the store
}
