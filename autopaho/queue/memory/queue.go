package memory

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"sync"
)

// A queue implementation that stores all data in RAM
package memqueue

import (
	"sync"
	"sync/atomic"
	"unsafe"
)

type LockFreeQueue[T any] struct {
	head unsafe.Pointer
	tail unsafe.Pointer
	cond *sync.Cond
}

type node[T any] struct {
	value T
	next  unsafe.Pointer
}

func NewLockFree[T any]() *LockFreeQueue[T] {
	node := unsafe.Pointer(new(node[T]))
	return &LockFreeQueue[T]{
		head: node,
		tail: node,
		cond: sync.NewCond(&sync.Mutex{}),
	}
}

func (q *LockFreeQueue[T]) Enqueue(v T) {
	node := &node[T]{value: v}
	for {
		tail := load[T](&q.tail)
		next := load[T](&tail.next)
		if tail == load[T](&q.tail) {
			if next == nil {
				if cas(&tail.next, next, node) {
					cas(&q.tail, tail, node)

					// Notify waiting goroutines
					q.cond.Broadcast()

					return
				}
			} else {
				cas(&q.tail, tail, next)
			}
		}
	}
}

func (q *LockFreeQueue[T]) Dequeue() (v T, ok bool) {
	for {
		head := load[T](&q.head)
		tail := load[T](&q.tail)
		next := load[T](&head.next)
		if head == load[T](&q.head) {
			if head == tail {
				if next == nil {
					var zero T
					return zero, false
				}
				cas(&q.tail, tail, next)
			} else {
				v := next.value
				if cas(&q.head, head, next) {
					return v, true
				}
			}
		}
	}
}

func (q *LockFreeQueue[T]) Wait() chan struct{} {
	ch := make(chan struct{})
	go func() {
		q.cond.L.Lock()
		for {
			head := load[T](&q.head)
			tail := load[T](&q.tail)
			if head == tail {
				q.cond.Wait()
			} else {
				close(ch)
				q.cond.L.Unlock()
				return
			}
		}
	}()
	return ch
}

func (q *LockFreeQueue[T]) Peek() *T {
	head := load[T](&q.head)
	tail := load[T](&q.tail)
	next := load[T](&head.next)

	if head == tail && next == nil {
		return nil
	} else {
		return &next.value
	}
}

func load[T any](p *unsafe.Pointer) *node[T] {
	return (*node[T])(atomic.LoadPointer(p))
}

func cas[T any](p *unsafe.Pointer, old, new *node[T]) bool {
	return atomic.CompareAndSwapPointer(p,
		unsafe.Pointer(old), unsafe.Pointer(new))
}

func (q *LockFreeQueue[T]) Clear() {
	q.head = unsafe.Pointer(new(node[T]))
	q.tail = q.head
}

func (q *LockFreeQueue[T]) IsEmpty() bool {
	head := load[T](&q.head)
	tail := load[T](&q.tail)
	return head == tail && load[T](&head.next) == nil
}
