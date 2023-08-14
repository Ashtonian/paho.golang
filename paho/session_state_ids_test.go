package paho

import (
	"context"
	"math"
	"math/rand"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/packets"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/semaphore"
)

func TestMidNoExhaustion(t *testing.T) {
	serverLogger := &testLog{l: t, prefix: "testServer:"} // This will output a LOT

	ts := newTestServer()
	ts.logger = serverLogger
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
	c.clientInflight = semaphore.NewWeighted(10)
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

func TestMidExhaustion(t *testing.T) {
	serverLogger := &testLog{l: t, prefix: "testServer:"}
	clientLogger := &testLog{l: t, prefix: "MidExhaustion:"}

	ts := newTestServer() // publish with `conn = nil` will not return the expected message
	ts.logger = serverLogger
	go ts.Run()
	defer ts.Stop()

	ss := sessionState{
		serverPackets: make(map[uint16]byte),
		serverStore:   NewMemoryStore(),
		clientPackets: make(map[uint16]clientGenerated),
		clientStore:   NewMemoryStore(),
		debug:         NOOPLogger{},
		errors:        NOOPLogger{},
	}
	c := NewClient(ClientConfig{
		Conn:    ts.ClientConn(),
		Session: &ss,
	})
	require.NotNil(t, c)

	c.serverInflight = semaphore.NewWeighted(10)
	c.clientInflight = semaphore.NewWeighted(10)
	c.stop = make(chan struct{})
	c.publishPackets = make(chan *packets.Publish)
	c.Session.ConAckReceived(c.Conn, &packets.Connect{}, &packets.Connack{})
	c.SetErrorLogger(clientLogger)
	c.SetDebugLogger(clientLogger)

	for i := uint16(1); i != 0; i++ {
		v, _ := ss.allocateNextMid(packets.PUBLISH, nil)
		assert.Equal(t, i, v)
	}

	_, err := ss.allocateNextMid(packets.PUBLISH, nil)
	assert.ErrorIs(t, err, ErrorMidsExhausted)

	p := &Publish{
		Topic:   "test/1",
		QoS:     1,
		Payload: []byte("test payload"),
	}

	pa, err := c.Publish(context.Background(), p)
	assert.Nil(t, pa)
	assert.ErrorIs(t, err, ErrorMidsExhausted)
}

// TestMidAllocateAndFreeAll checks that we can allocate all message identifiers and that, when freed, a message is always
// sent to the response channel
func TestMidAllocateAndFreeAll(t *testing.T) {
	ss := &sessionState{
		serverPackets: make(map[uint16]byte),
		serverStore:   NewMemoryStore(),
		clientPackets: make(map[uint16]clientGenerated),
		clientStore:   NewMemoryStore(),
		debug:         NOOPLogger{},
		errors:        NOOPLogger{},
	}

	// Use full band
	cpChan := make(chan packets.ControlPacket)
	for i := uint16(1); i != 0; i++ {
		v, _ := ss.allocateNextMid(packets.PUBLISH, cpChan)
		assert.Equal(t, i, v)
	}

	// Free all Mids
	allResponded := make(chan struct{})
	go func() {
		for i := uint16(0); i < midMax; i++ {
			<-cpChan
		}
		close(allResponded)
	}()

	resp := packets.ControlPacket{
		Content: nil,
		FixedHeader: packets.FixedHeader{
			Type:  packets.PUBACK,
			Flags: 0,
		},
	}
	for i := uint16(1); i != 0; i++ {
		assert.NoError(t, ss.endClientGenerated(i, &resp))
	}
	select {
	case <-allResponded:
	case <-time.After(time.Second):
		t.Fatal("did not receive responses")
	}
	select {
	case <-cpChan:
		t.Fatal("unexpected response")
	default:
	}

	// Allocate all Mids again
	for i := uint16(1); i != 0; i++ {
		v, _ := ss.allocateNextMid(packets.PUBLISH, cpChan)
		assert.Equal(t, i, v)
	}

	// Closing the store should free all Ids sending a message to the provided channel
	gotCp := make(chan struct{})
	go func() {
		for i := uint16(0); i < midMax; i++ {
			<-cpChan
		}
		close(gotCp)
	}()
	ss.Close()
	select {
	case <-gotCp:
	case <-time.After(time.Second):
		t.Fatal("did not receive responses")
	}
	select {
	case <-cpChan:
		t.Fatal("unexpected response")
	default:
	}
}

// TestMidHoles confirms that random "holes" within the Message ID map will be found and utilised
func TestMidHoles(t *testing.T) {
	ss := &sessionState{
		serverPackets: make(map[uint16]byte),
		serverStore:   NewMemoryStore(),
		clientPackets: make(map[uint16]clientGenerated),
		clientStore:   NewMemoryStore(),
		debug:         NOOPLogger{},
		errors:        NOOPLogger{},
	}

	// For this test we ignore responses
	cpChan := make(chan packets.ControlPacket)
	defer close(cpChan)
	go func() {
		for range cpChan {
		}
	}()

	// Allocate all Mids
	for i := uint16(1); i != 0; i++ {
		v, _ := ss.allocateNextMid(packets.PUBLISH, cpChan)
		assert.Equal(t, i, v)
	}

	resp := packets.ControlPacket{
		Content: nil,
		FixedHeader: packets.FixedHeader{
			Type:  packets.PUBACK,
			Flags: 0,
		},
	}

	// Currently MIDs.index is filled in, randomly dig some holes and try to fill in all of them again.
	h := map[uint16]bool{}
	for i := 0; i < 60000; i++ {
		r := uint16(rand.Intn(math.MaxUint16))
		r += 1 // Want 0-65535

		ss.endClientGenerated(r, &resp)
		h[r] = true
	}
	t.Log("Num of holes:", len(h))
	for i := 0; i < len(h); i++ {
		_, err := ss.allocateNextMid(packets.PUBLISH, cpChan)
		assert.Nil(t, err)
	}
}

// Expecting MIDs.Free(0) always do nothing (no panic), because 0 identifier is invalid and ignored.
func TestMIDsFreeZeroID(t *testing.T) {
	ss := &sessionState{
		serverPackets: make(map[uint16]byte),
		serverStore:   NewMemoryStore(),
		clientPackets: make(map[uint16]clientGenerated),
		clientStore:   NewMemoryStore(),
		debug:         NOOPLogger{},
		errors:        NOOPLogger{},
	}
	resp := packets.ControlPacket{
		Content: nil,
		FixedHeader: packets.FixedHeader{
			Type:  packets.PUBACK,
			Flags: 0,
		},
	}
	assert.NotPanics(t, func() { assert.NoError(t, ss.endClientGenerated(0, &resp)) })
}
