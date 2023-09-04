Eclipse Paho MQTT Go client
===========================

This repository contains the source code for the [Eclipse Paho](http://eclipse.org/paho) MQTT Go client library.

----------------------------------------------------------------------------------------
Notes on changes from paho.golang@master

This *TEST* repo implements persistent session state support.

It's currently alpha code (which I'm actively testing), but I believe it shows one way `paho.golang` can implement 
persistent session states in a way that mostly maintains compatability with, and the simplicity of, the current client
whilst maximising flexibility (and allowing me to implement the functionality I personally need!)

This code will contain bugs (but it also fixes a number of bugs in the current code!), and there is plenty of room for 
improvements/optimisation. 

## Notes on implementation

I tried a wide range of approaches before settling on the current one. The `paho.golang/paho` interface is nice and 
simple but retailing this simplicity whilst implementing session state is difficult. The key change is `sessionState`
which is passed to `paho` via `ClientConfig`. This means that a `sessionState` can outlast a connection (i.e. an 
instance of `paho.Client`) which enables a call to `Publish` made with one instance to block until released within 
another instance (allowing `autopaho` to effectively hide reconnections).

The main test for the changes was using it within `autopaho`; if it's usable within that library then its probably
usable elsewhere. `autopaho` now implements `PublishViaQueue` which aims to attempt to publish the message whatever 
the connection state (never connected, currently connected, attempting to reconnect) and stores outgoing messages in
a queue (meaning messages get moved from a queue to the session state, but I think that's unavoidable).,

* `paho` closes the session state if it creates it. This means that most applications will run without modification 
because any calls blocking when `Client.stop` is closed will be released (and return the expected error).
* If a `sessionState` is provided then it is up to the user (e.g. `autopaho`) to close it (which will happen on shutdown).
This means that a user can call `Publish()` and receive a response despite the need to reconnect whilst the transaction
is in progress.

## TODO

// Load data from session store at startup
// Disk-based store and queue
// Review changes and tidy up code/comments
// Improve error handling (difficult to know what to do when the store fails - not one "best" answer)
// Add more tests
// Add more comments

## Issues

### Queued messages using Aliases

Topic aliases are not part of the session state. This means that if messages using a topic alias are queued when the 
connection drops and then sent when it comes up will not have the desired impact. Possible workaround would be to detect
these and cancel them all when the connection drops.

### Multiple Brokers

If a Client may connect to more than one broker, or with different ClientIDs, then the user will need to carefully manage
the store (because each store is specific to one broker/ClientID combination).

### SessionExpiryInterval

The client effectively ignores SessionExpiryInterval when it comes to managing state. I don't believe this is an issue 
because the servers `CONNACK` will include the Session Present flag, which will inform us if the session has expired. 
Users may wish to clear session information to save on storage, this is not something the library currently supports.

### Inflight Message tracking

This gets a bit tricky with a session because there are messages inflight before the connection comes up. As such the
semaphore has been moved into `sessionState`. Currently, the Receive Maximum from the first CONNACK is used regardless 
of any potential changes in future connections (it seems fairly unlikely that the value will change but its possible).
This is fixable but will need to be carefully managed to avoid races so is being left for now (there were issues with the
way this was handled previously too, so I don't see it as a big issue).

## Breaking changes:

* `paho` `ClientOptions.MIDs` has been removed. While it was possible to implement your own MIDService I suspect that 
no one has done so.
* `paho.Publish` when publishing at QOS1/2 the packet identifier (if acquired) was released if the context expired
regardless of whether the message had been sent (potentially leading to reuse). This has been changed such that 
once transmitted, the message will be acknowledged regardless of the publish context (but the Publish function will
only block until the context expires). The Errors returned now better indicate what occurred. 
* `autopaho` CleanSession flag. Previously the CleanSession was hardcoded to `true`; this is no longer the case and 
the default is `false`. Whilstt his is potentially a breaking change `SessionExpiryInterval` will default to 0 meaning 
the session will be removed when the connection drops. As a result this change should have no impact on most users; it 
may be a problem if another application has connected with `SessionExpiryInterval>0` meaning a session exists.

## General

### Why create a package for session state

I cannot foresee all the ways the session state might be stored, so giving users the freedom to create their own 
implementation seemed important. Moving the code into its own package ensures that there is a clean interface between
it and `paho`.

### Why integrate MIDs (previously in `ClientConfig`) into `Session`?

The main reason is that the session state may outlast a single connection.
When a new connection is established, the session state may (this depends upon clean start etc) still contain
packet IDs from the previous connection (e.g. unacknowledged QOS1 publish). This leaves us with a choice of:
  1. Reading from the store and creating a new map of packet IDs.
  2. Managing packet IDs as part of the session state that outlasts a connection.

Option 2 has a range of advantages:
  * There is no need to read the store when reconnecting (potentially an expensive option if its on disk)
  * Blocking calls to, for example, `Publish()` can remain blocked over a re-connect (providing better dev experience)
  * Reduces the interactions between the store and the client (easy to introduce deadlocks etc here)
  * MIDs can be populated when first needed rather than immediately (it may be expensive to load the data)

For this option to be workable information on the packet IDs needs to outlast the client; hence including in `Session`
makes sense. A further benefit of this is that code to load the information (from disk etc) will be in `Session` which
is a more logical place for it (as with this structure the info only needs to bel loaded once).

### Why are packet ids stored in a map and not a slice as previously?

The old MIDS used a slice (effectively `make([]*CPContext, 65535)`). This will consume approximately 500kb of ram 
(on a 64-bit machine); whilst this is not a lot on a PC it is significant on some lower resource devices on which 
this library may be used. In addition, most brokers now impose sensible defaults with mosquitto defaulting to 
"Receive Maximum" = 20. This means that allocating space for 65535 seems excessive. A map should use significantly
less RAM and I doubt the performance hit is significant (when compared to network latency etc).

### Why move semaphore's to `sessionState`?

With "Receive Maximum" the server details the "the number of QoS 1 and QoS 2 publications that it is willing to process 
concurrently for the Client". This leads to a couple of reasons for handling this in `sessionState`.
 * If a session state exists, then any publications within that state count towards this limit. `Client` does not need
   any knowledge of these transactions.
 * If we are waiting on link capacity, then Publish requests should really be stored whilst we wait (it's possible that
   the messages may be of a considerable size, so holding them in memory may not be ideal).

### Why handle packets within `sessionState`?

It would have been possible to notify `sessionState` when messages were sent/received but experiments with this approach 
led to code that was difficult to follow

### What is the issue with `Ack`?

It's unclear what `Ack` should do if it is called after the connection has terminated. See issue #160 for more info.

----------------------------------------------------------------------------------------

Installation and Build
----------------------

This client is designed to work with the standard Go tools, so installation is as easy as:

```bash
go get github.com/eclipse/paho.golang
```

Folder Structure
----------------

The main library is in the `paho` folder (so for general usage `import "github.com/eclipse/paho.golang/paho"`). There are 
examples off this folder in `paho/cmd` and extensions in `paho/extensions`.

`autopaho` (`import "github.com/eclipse/paho.golang/autopaho"`) is a fairly simple wrapper that automates the connection 
process (`mqtt` and `mqtts`) and will automatically reconnect should the connection drop. For many users this package
will provide a simple way to connect and publish/subscribe as well as demonstrating how to use the `paho.golang/paho`.
`autopaho/examples/docker` provides a full example using docker to run a publisher and subscriber (connecting to 
mosquitto).  


Reporting bugs
--------------

Please report bugs by raising issues for this project in github [https://github.com/eclipse/paho.golang/issues](https://github.com/eclipse/paho.golang/issues)

More information
----------------

Discussion of the Paho clients takes place on the [Eclipse paho-dev mailing list](https://dev.eclipse.org/mailman/listinfo/paho-dev).

General questions about the MQTT protocol are discussed in the [MQTT Google Group](https://groups.google.com/forum/?hl=en-US&fromgroups#!forum/mqtt).

There is much more information available via the [MQTT community site](http://mqtt.org).
