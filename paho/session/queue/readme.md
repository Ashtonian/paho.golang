store
=====

Contains implementations of `session.queuer`. These are used to store Publish packets that could not be sent at
the time this was requested. A `Publish` request may go in the queue because:

* The connection to the broker is not currently available or
* There are "Receive Maximum" requests in progress (which prevents us sending another one immediately).
