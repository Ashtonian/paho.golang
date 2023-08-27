AutoPaho
========

AutoPaho has a number of aims:

* Provide an easy-to-use MQTT v5 client that provides commonly requested functionality (e.g. connection, automatic reconnection, message queueing).
* Demonstrate the use of `paho.golang/paho`.
* Enable us to smoke test `paho.golang/paho` features (ensuring they are they usable in a real world situation)

## Basic Usage


## QOS 1 & 2


## Work in progress

Use case: I want to regularly publish messages and leave it to autopaho to ensure they are delivered (regardless of the connection state).

Acceptance tests:
   * Publish a message before connection is established; it should be queued and delivered when connection comes up.
   * Connection drops and messages are published whilst reconnection is in progress. They should be queued and delivered 
     when connection is available.
   * Publish messages at a rate in excess of Receive Maximum; they should be queued and sent, in order, when possible.
   * Application restarts during any of the above - queued messages are sent out when connection comes up.

Desired features:
   * Fire and forget - async publish; we trust the library to deliver the message so once its in the store the client can forget about it.
   * Minimal RAM use - the connection may be down for a long time and we may not have much ram. So messages should be on disk (ideally with
     no trace of them in RAM)
   
What is needed to deliver this>

### `paho.golang/paho`

Need to know when a message has been stored in session state (i.e. semaphore released, MID issued and stored). Once this
has occurred, we can remove the message from the queue. 

Current thought is to always have one message blocking at `Publish` and then remove from queue and publish another
(if applicable) when the message is in the session state store.

