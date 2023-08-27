package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/eclipse/paho.golang/autopaho"
	"github.com/eclipse/paho.golang/paho"
	"github.com/eclipse/paho.golang/paho/log"
	"github.com/eclipse/paho.golang/paho/store/memory"
)

// Connect to the broker and publish a message periodically
func main() {
	cfg, err := getConfig()
	if err != nil {
		panic(err)
	}

	// We will be publishing at various QOS levels and want the session state to survive reconnection
	// as such we create a `session` manually (stores are also created manually so they can be output
	// for debug purposes).
	clientStore := memory.New()
	serverStore := memory.New()
	sess := state.New(clientStore, serverStore)
	defer sess.Close()
	if cfg.debug {
		sess.SetErrorLogger(logger{prefix: "autoPaho sess"})
		sess.SetDebugLogger(logger{prefix: "autoPaho sess"})
	}

	cliCfg := autopaho.ClientConfig{
		BrokerUrls:        []*url.URL{cfg.serverURL},
		KeepAlive:         cfg.keepAlive,
		ConnectRetryDelay: cfg.connectRetryDelay,
		OnConnectionUp: func(_ *autopaho.ConnectionManager, ack *paho.Connack) {
			fmt.Println("mqtt connection up")
		},
		OnConnectError:        func(err error) { fmt.Printf("error whilst attempting connection: %s\n", err) },
		Debug:                 log.NOOPLogger{},
		CleanStart:            false, // the default
		SessionExpiryInterval: 60,    // Session remains live 60 seconds after disconnect
		ClientConfig: paho.ClientConfig{
			ClientID:      cfg.clientID,
			Session:       sess,
			OnClientError: func(err error) { fmt.Printf("server requested disconnect: %s\n", err) },
			OnServerDisconnect: func(d *paho.Disconnect) {
				if d.Properties != nil {
					fmt.Printf("server requested disconnect: %s\n", d.Properties.ReasonString)
				} else {
					fmt.Printf("server requested disconnect; reason code: %d\n", d.ReasonCode)
				}
			},
		},
	}

	if cfg.debug {
		cliCfg.Debug = logger{prefix: "autoPaho"}
		cliCfg.PahoDebug = logger{prefix: "paho"}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Connect to the broker - this will return immediately after initiating the connection process
	cm, err := autopaho.NewConnection(ctx, cliCfg)
	if err != nil {
		panic(err)
	}

	var wg sync.WaitGroup

	// Start off a goRoutine that publishes messages
	wg.Add(1)
	go func() {
		defer wg.Done()
		var count uint64
		dropAt := time.Now().Add(cfg.dropAfter)
		for {
			// AwaitConnection will return immediately if connection is up; adding this call stops publication whilst
			// connection is unavailable.
			err = cm.AwaitConnection(ctx)
			if err != nil { // Should only happen when context is cancelled
				fmt.Printf("publisher done (AwaitConnection: %s)\n", err)
				return
			}

			count += 1
			// The message could be anything; lets make it JSON containing a simple count (makes it simpler to track the messages)
			msg, err := json.Marshal(struct {
				Count uint64
			}{Count: count})
			if err != nil {
				panic(err)
			}

			// Publish will block so we run it in a goRoutine
			go func(msg []byte) {
				// It is possible that the connection may be down when we attempt to publish so we will retry until
				// successful, or it's time to exit
				for {
					if ctx.Err() != nil { // Abort if context is cancelled
						return
					}

					pr, err := cm.Publish(ctx, &paho.Publish{
						QoS:     cfg.qos,
						Topic:   cfg.topic,
						Payload: msg,
					})
					if err != nil {
						fmt.Printf("error publishing: %s\n", err)
						continue
					} else if pr.ReasonCode != 0 && pr.ReasonCode != 16 { // 16 = Server received message but there are no subscribers
						fmt.Printf("reason code %d received\n", pr.ReasonCode)
						return
					} else if cfg.printMessages {
						fmt.Printf("sent message: %s\n", msg)
						return
					}
					time.Sleep(500 * time.Millisecond)
				}
			}(msg)

			if time.Now().After(dropAt) {
				cm.TerminateConnectionForTest() // This just closes the connection (autopaho should reconnect automatically)
				dropAt = dropAt.Add(cfg.dropAfter)
				if cfg.printMessages {
					fmt.Printf("connection dropped at message: %d\n", count)
				}
			}

			select {
			case <-time.After(cfg.delayBetweenMessages):
			case <-ctx.Done():
				fmt.Println("publisher done")
				return
			}
		}
	}()

	// Wait for a signal before exiting
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt)
	signal.Notify(sig, syscall.SIGTERM)

	<-sig
	fmt.Println("signal caught - exiting")
	cancel()

	wg.Wait()
	fmt.Println("shutdown complete")

	if cfg.debug { // Provide information on packets in store that may be sent on next connection
		fmt.Printf("client store content on exit: %s\n\n", clientStore)
		fmt.Printf("server store content on exit: %s\n\n", serverStore)
	}
}

// logger implements the paho.Logger interface
type logger struct {
	prefix string
}

// Println is the library provided NOOPLogger's
// implementation of the required interface function()
func (l logger) Println(v ...interface{}) {
	fmt.Println(append([]interface{}{l.prefix + ":"}, v...)...)
}

// Printf is the library provided NOOPLogger's
// implementation of the required interface function(){}
func (l logger) Printf(format string, v ...interface{}) {
	if len(format) > 0 && format[len(format)-1] != '\n' {
		format = format + "\n" // some log calls in paho do not add \n
	}
	fmt.Printf(l.prefix+":"+format, v...)
}
