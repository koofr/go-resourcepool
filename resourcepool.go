package resourcepool

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
)

var ErrPoolClosed = errors.New("pool closed")

type Resource interface {
	Close()
	IsClosed() bool
}

type Factory func(ctx context.Context) (Resource, error)

type ResourcePool struct {
	factory      Factory
	idleCapacity int
	maxResources int64

	idleResources    ring
	numIdleResources int64
	numResources     int64
	activeWaits      []acquireMessage
	numActiveWaits   int64
	closed           bool
	closedMutex      sync.RWMutex
	acqchan          chan acquireMessage
	rchan            chan releaseMessage
	cchan            chan closeMessage
	echan            chan emptyMessage
}

func NewResourcePool(factory Factory, idleCapacity int, maxResources int) (rp *ResourcePool) {
	rp = &ResourcePool{
		factory:      factory,
		idleCapacity: idleCapacity,
		maxResources: int64(maxResources),
		closed:       false,

		acqchan: make(chan acquireMessage),
		rchan:   make(chan releaseMessage, 1),
		cchan:   make(chan closeMessage, 1),
		echan:   make(chan emptyMessage, 1),
	}

	go rp.mux()

	return
}

type releaseMessage struct {
	r Resource
}

type acquireMessage struct {
	ctx context.Context
	rch chan Resource
	ech chan error
}

type closeMessage struct {
}

type emptyMessage struct {
	rch chan interface{}
}

func (rp *ResourcePool) mux() {
loop:
	for {
		select {
		case acq := <-rp.acqchan:
			rp.acquire(acq)

		case rel := <-rp.rchan:
			rp.handleRelease(rel)

		case _ = <-rp.cchan:
			break loop

		case emp := <-rp.echan:
			rp.empty()
			emp.rch <- nil
			rp.drainEmptyChan()
		}
	}

	rp.empty()

	// rp.closedMutex is locked in Close
	rp.closed = true
	rp.closedMutex.Unlock()

	for _, aw := range rp.activeWaits {
		aw.ech <- ErrPoolClosed
	}

	rp.activeWaits = nil
	atomic.StoreInt64(&rp.numActiveWaits, 0)
}

func (rp *ResourcePool) acquire(acq acquireMessage) {
	for !rp.idleResources.Empty() {
		r := rp.idleResources.Dequeue()
		atomic.AddInt64(&rp.numIdleResources, -1)
		if !r.IsClosed() {
			select {
			case acq.rch <- r:
			case <-acq.ctx.Done():
				rp.idleResources.Enqueue(r)
				atomic.AddInt64(&rp.numIdleResources, 1)
			}
			return
		}
		// discard closed resources
		atomic.AddInt64(&rp.numResources, -1)
	}
	if rp.maxResources != -1 && atomic.LoadInt64(&rp.numResources) >= rp.maxResources {
		// we need to wait until something comes back in
		// acq.ctx.Done() will be handled on next release
		rp.activeWaits = append(rp.activeWaits, acq)
		atomic.AddInt64(&rp.numActiveWaits, 1)
		return
	}

	r, err := rp.factory(acq.ctx)
	if err != nil {
		acq.ech <- err
	} else {
		atomic.AddInt64(&rp.numResources, 1)
		acq.rch <- r
	}

	return
}

func (rp *ResourcePool) getNextActiveWait() (acquireMessage, bool) {
	for len(rp.activeWaits) > 0 {
		activeWait := rp.activeWaits[0]
		rp.activeWaits = rp.activeWaits[1:]
		atomic.AddInt64(&rp.numActiveWaits, -1)
		select {
		case <-activeWait.ctx.Done():
		default:
			return activeWait, true
		}
	}
	return acquireMessage{}, false
}

func (rp *ResourcePool) handleRelease(rel releaseMessage) {
	if activeWait, ok := rp.getNextActiveWait(); ok {
		// someone is waiting - give them the resource if we can
		if !rel.r.IsClosed() {
			select {
			case activeWait.rch <- rel.r:
			case <-activeWait.ctx.Done():
				// ctx canceled, release it for idling or closing
				rp.release(rel.r)
			}
		} else {
			// if we can't, discard the released resource and create a new one
			r, err := rp.factory(activeWait.ctx)
			if err != nil {
				// reflect the smaller number of existant resources
				atomic.AddInt64(&rp.numResources, -1)
				select {
				case activeWait.ech <- err:
				case <-activeWait.ctx.Done():
				}
			} else {
				select {
				case activeWait.rch <- r:
				case <-activeWait.ctx.Done():
					// ctx canceled, release it for idling or closing
					rp.release(r)
				}
			}
		}
	} else {
		// if no one is waiting, release it for idling or closing
		rp.release(rel.r)
	}
}

func (rp *ResourcePool) release(resource Resource) {
	if resource == nil || resource.IsClosed() {
		// don't put it back in the pool.
		atomic.AddInt64(&rp.numResources, -1)
		return
	}
	if rp.idleCapacity != -1 && rp.idleResources.Size() == rp.idleCapacity {
		resource.Close()
		atomic.AddInt64(&rp.numResources, -1)
		return
	}

	rp.idleResources.Enqueue(resource)
	atomic.AddInt64(&rp.numIdleResources, 1)
}

func (rp *ResourcePool) empty() {
	for !rp.idleResources.Empty() {
		rp.idleResources.Dequeue().Close()
		atomic.AddInt64(&rp.numIdleResources, -1)
		atomic.AddInt64(&rp.numResources, -1)
	}
}

func (rp *ResourcePool) drainEmptyChan() {
	for {
		select {
		case e := <-rp.echan:
			e.rch <- nil
		default:
			return
		}
	}
}

func (rp *ResourcePool) IsClosed() bool {
	rp.closedMutex.RLock()
	defer rp.closedMutex.RUnlock()

	return rp.closed
}

// Acquire will get one of the idle resources, or create a new one.
func (rp *ResourcePool) Acquire(ctx context.Context) (resource Resource, err error) {
	if rp.IsClosed() {
		return nil, ErrPoolClosed
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	acq := acquireMessage{
		ctx: ctx,
		rch: make(chan Resource),
		ech: make(chan error),
	}

	select {
	case rp.acqchan <- acq:
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	select {
	case resource := <-acq.rch:
		return resource, nil
	case err := <-acq.ech:
		return nil, err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Release will release a resource for use by others. If the idle queue is
// full, the resource will be closed.
func (rp *ResourcePool) Release(resource Resource) {
	if rp.IsClosed() {
		if !resource.IsClosed() {
			resource.Close()
		}

		atomic.AddInt64(&rp.numResources, -1)

		return
	}

	rel := releaseMessage{
		r: resource,
	}

	rp.rchan <- rel
}

// Close closes all the pool's resources.
func (rp *ResourcePool) Close() {
	rp.closedMutex.Lock()
	if rp.closed {
		rp.closedMutex.Unlock()
		return
	}

	rp.cchan <- closeMessage{}
}

// Empty removes idle pool's resources.
func (rp *ResourcePool) Empty() {
	if rp.IsClosed() {
		return
	}

	emp := emptyMessage{
		rch: make(chan interface{}),
	}

	rp.echan <- emp

	<-emp.rch
}

// NumResources is the number of resources known at this time
func (rp *ResourcePool) NumResources() int {
	return int(atomic.LoadInt64(&rp.numResources))
}

// NumIdleResources is the number of idle resources
func (rp *ResourcePool) NumIdleResources() int {
	return int(atomic.LoadInt64(&rp.numIdleResources))
}

// NumActiveWaits is the number of callers waiting for a resource
func (rp *ResourcePool) NumActiveWaits() int {
	return int(atomic.LoadInt64(&rp.numActiveWaits))
}
