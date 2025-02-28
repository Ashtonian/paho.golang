package state

import (
	"context"
	"errors"
	"sync"
)

// Implements send quota as described in section 4.9 of the spec. This is used to honour the receive-maximum
// received from the broker; each time a qos1/2 PUBLISH is to be sent `Acquire` must be called, and this will
// block until a slot is available (`Release` is called when the message is fully acknowledged).

// This function was previously performed by `golang.org/x/sync/semaphore` but, as per the MQTT spec:
//
// > The send quota is not incremented if it is already equal to the initial send quota. The attempt to increment above
// > the initial send quota might be caused by the re-transmission of a PUBREL packet after a new Network Connection is
// > established.
//
// The result of this happening with `semaphore` is a `panic` which is not ideal.
// It is also possible (as per issue #179) that bugs, or unexpected circumstances, may result in the same situation. For
// example: if the local session state is lost but there is a session state on the server (meaning it sends an unexpected
// PUBACK).
//
// Note: If the broker does not correctly acknowledge messages, then the quota will be consumed over time. There
// should probably be a process to drop the connection if there are no slots available and no acknowledgements have been
// received recently.

// ErrUnexpectedRelease is for logging only (to help identify if there are issues with state management)
var ErrUnexpectedRelease = errors.New("release called when quota at initial value")

// newSendQuota creates a new tracker limited to quota concurrent messages
func newSendQuota(quota uint16) *sendQuota {
	w := &sendQuota{initialQuota: quota, quota: quota}
	return w
}

// sendQuota provides a way to bound concurrent access to a resource.
// The callers can request access with a given weight.
type sendQuota struct {
	mu           sync.Mutex
	initialQuota uint16
	quota        uint16
	waiters      []chan<- struct{} // using a slice because would generally expect this to be small
}

// Retransmit takes a slot for a message that is being redelivered and will never block.
// This is not in compliance with the MQTT v5 spec and should be removed in the future.
func (s *sendQuota) Retransmit() error {
	return s.acquire(context.Background(), true)
}

// Acquire waits for a slot to become available so a message can be published
// If ctx is already done, Acquire may still succeed without blocking.
func (s *sendQuota) Acquire(ctx context.Context) error {
	return s.acquire(ctx, false)
}

// acquire attempts to allocate a slot for a message to be published
// If noWait is true quota will be ignored and the call will return immediately, otherwise acquire will block
// until a slot is available.
func (s *sendQuota) acquire(ctx context.Context, noWait bool) error {
	s.mu.Lock()
	if noWait || (s.quota > 0 && len(s.waiters) == 0) {
		s.quota-- // Note: can go < 0 if noWait used
		s.mu.Unlock()
		return nil
	}

	// We need to join the queue
	ready := make(chan struct{})
	s.waiters = append(s.waiters, ready)
	s.mu.Unlock()

	var err error
	select {
	case <-ctx.Done():
		err = ctx.Err()
		s.mu.Lock()
		select {
		case <-ready:
			// Acquired the semaphore after we were canceled.  Rather than trying to
			// fix up the queue, just pretend we didn't notice the cancellation.
			err = nil
		default:
		}
		s.mu.Unlock()
	case <-ready: // Note that quota already accounts for this item
	}
	return err
}

// Release releases slot for the specified message ID
// error is for logging only (ref issue #179)
func (s *sendQuota) Release() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.quota >= 0 && len(s.waiters) > 0 { // Possible quota could go negative when using noWait
		close(s.waiters[0])
		s.waiters = append(s.waiters[:0], s.waiters[1:]...)
	} else if s.quota < s.initialQuota {
		s.quota++
		return nil
	}
	return ErrUnexpectedRelease
}
