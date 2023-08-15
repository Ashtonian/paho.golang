package autopaho

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/autopaho/internal/testserver"
	"github.com/eclipse/paho.golang/paho"
)

//
// This file contains tests that focus on session state persistence and delivery of QOS1/2 messages.
//

// TestBasicPubSub performs pub/sub operations at each QOS level
func TestBasicPubSub44(t *testing.T) {
	t.Parallel()
	broker, _ := url.Parse(dummyURL)
	serverLogger := &testLog{l: t, prefix: "testServer:"}
	logger := &testLog{l: t, prefix: "test:"}
	defer func() {
		// Prevent any logging after completion. Unfortunately, there is currently no way to know if paho.Client
		// has fully shutdown. As such, messages may be logged after shutdown (which will result in a panic).
		serverLogger.Stop()
		logger.Stop()
	}()

	ts := testserver.New(serverLogger)

	type tsConnUpMsg struct {
		cancelFn func()        // Function to cancel test broker context
		done     chan struct{} // Will be closed when the test broker has disconnected (and shutdown)
	}
	tsConnUpChan := make(chan tsConnUpMsg) // Message will be sent when test broker connection is up
	pahoConnUpChan := make(chan struct{})  // When autopaho reports connection is up write to channel will occur

	const expectedMessages = 3
	var mrMu sync.Mutex
	mrDone := make(chan struct{}) // Closed when the expected messages have been received
	var messagesReceived []*paho.Publish

	atCount := 0

	config := ClientConfig{
		BrokerUrls:        []*url.URL{broker},
		KeepAlive:         60,
		ConnectRetryDelay: time.Millisecond, // Retry connection very quickly!
		ConnectTimeout:    shortDelay,       // Connection should come up very quickly
		AttemptConnection: func(ctx context.Context, _ ClientConfig, _ *url.URL) (net.Conn, error) {
			atCount += 1
			if atCount > 1 { // force failure if a reconnection is attempted (the connection should not drop in this test)
				return nil, errors.New("connection attempt failed")
			}
			ctx, cancel := context.WithCancel(ctx)
			conn, done, err := ts.Connect(ctx)
			if err == nil { // The above may fail if attempted too quickly (before disconnect processed)
				tsConnUpChan <- tsConnUpMsg{cancelFn: cancel, done: done}
			} else {
				cancel()
			}
			logger.Println("connection up")
			return conn, err
		},
		OnConnectionUp: func(*ConnectionManager, *paho.Connack) { pahoConnUpChan <- struct{}{} },
		Debug:          logger,
		PahoDebug:      logger,
		PahoErrors:     logger,
		ClientConfig: paho.ClientConfig{
			ClientID: "test",
			Router: paho.NewSingleHandlerRouter(func(publish *paho.Publish) {
				mrMu.Lock()
				defer mrMu.Unlock()
				messagesReceived = append(messagesReceived, publish)
				if len(messagesReceived) == expectedMessages {
					close(mrDone)
				}
			}),
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), longerDelay)
	defer cancel()
	cm, err := NewConnection(ctx, config)
	if err != nil {
		t.Fatalf("expected NewConnection success: %s", err)
	}

	// Wait for connection to come up
	var initialConnUpMsg tsConnUpMsg
	select {
	case initialConnUpMsg = <-tsConnUpChan:
	case <-time.After(shortDelay):
		t.Fatal("timeout awaiting initial connection request")
	}
	select {
	case <-pahoConnUpChan:
	case <-time.After(shortDelay):
		t.Fatal("timeout awaiting connection up")
	}

	testFmt := "Test%d"
	var subs []paho.SubscribeOptions
	for i := 0; i < 3; i++ {
		subs = append(subs, paho.SubscribeOptions{
			Topic: fmt.Sprintf(testFmt, i),
			QoS:   byte(i),
		})
	}
	if _, err = cm.Subscribe(ctx, &paho.Subscribe{
		Subscriptions: subs,
	}); err != nil {
		t.Fatalf("subscribe failed: %s", err)
	}

	// Note: The client does not currently support
	for i := 0; i < 3; i++ {
		t.Logf("publish QOS %d message", i)
		msg := fmt.Sprintf(testFmt, i)
		if _, err := cm.Publish(ctx, &paho.Publish{
			QoS:        byte(i),
			Topic:      msg,
			Properties: nil,
			Payload:    []byte(msg),
		}); err != nil {
			t.Fatalf("publish at QOS %d failed: %s", i, err)
		}
		t.Logf("publish QOS %d message complete", i)
	}

	// Wait until we have received the expected messages
	select {
	case <-mrDone:
	case <-time.After(shortDelay):
		mrMu.Lock()
		t.Fatalf("received %d of the expected %d messages (%v)", len(messagesReceived), expectedMessages, messagesReceived)
		// mrMu.Unlock() not needed as Fatal exits
	}

	// Note: While messages have been received, the QOS2 handshake process is probably still in progress
	// (router callback is called before any acknowledgement is sent).
	t.Log("messages received - closing connection")
	cancel() // Cancelling outer context will cascade
	select { // Wait for the local client to terminate
	case <-cm.Done():
	case <-time.After(shortDelay):
		t.Fatal("timeout awaiting connection manager shutdown")
	}

	select { // Wait for test server to terminate
	case <-initialConnUpMsg.done:
	case <-time.After(shortDelay):
		t.Fatal("test server did not shut down in a timely manner")
	}

	// Check we got what we expected
	for i := 0; i < 3; i++ {
		msg := fmt.Sprintf(testFmt, i)
		if messagesReceived[i].Topic != msg {
			t.Errorf("expected topic %s, got %s", msg, messagesReceived[i].Topic)
		}
		if string(messagesReceived[i].Payload) != msg {
			t.Errorf("expected message %v, got %v", []byte(msg), messagesReceived[i].Payload)
		}
		if err != nil {
			t.Fatalf("publish at QOS %d failed: %s", i, err)
		}
	}
}
