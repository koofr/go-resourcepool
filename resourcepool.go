package resourcepool

import (
	"errors"
	"sync"
)

var PoolClosed = errors.New("Resource pool closed")

type Resource interface {
	Close()
	IsClosed() bool
}

type Factory func() (Resource, error)

type ResourcePool struct {
	factory                 Factory
	idleResources           ring
	idleCapacity            int
	maxResources            int
	numResources            int
	closed                  bool
	closedMutex             sync.RWMutex
	numResourcesClosedMutex sync.Mutex

	acqchan chan acquireMessage
	rchan   chan releaseMessage
	cchan   chan closeMessage
	echan   chan emptyMessage

	activeWaits []acquireMessage
}

func NewResourcePool(factory Factory, idleCapacity, maxResources int) (rp *ResourcePool) {
	rp = &ResourcePool{
		factory:      factory,
		idleCapacity: idleCapacity,
		maxResources: maxResources,
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
			if len(rp.activeWaits) != 0 {
				// someone is waiting - give them the resource if we can
				if !rel.r.IsClosed() {
					rp.activeWaits[0].rch <- rel.r
				} else {
					// if we can't, discard the released resource and create a new one
					r, err := rp.factory()
					if err != nil {
						// reflect the smaller number of existant resources
						rp.numResources--
						rp.activeWaits[0].ech <- err
					} else {
						rp.activeWaits[0].rch <- r
					}
				}
				rp.activeWaits = rp.activeWaits[1:]
			} else {
				// if no one is waiting, release it for idling or closing
				rp.release(rel.r)
			}

		case _ = <-rp.cchan:
			break loop

		case emp := <-rp.echan:
			rp.empty()
			emp.rch <- nil

			// drain the empty channel
		drainloop:
			for {
				select {
				case e := <-rp.echan:
					e.rch <- nil
				default:
					break drainloop
				}
			}
		}
	}

	rp.empty()

	rp.closedMutex.Lock()
	rp.closed = true
	rp.closedMutex.Unlock()

	for _, aw := range rp.activeWaits {
		aw.ech <- PoolClosed
	}

	rp.activeWaits = []acquireMessage{}
}

func (rp *ResourcePool) acquire(acq acquireMessage) {
	for !rp.idleResources.Empty() {
		r := rp.idleResources.Dequeue()
		if !r.IsClosed() {
			acq.rch <- r
			return
		}
		// discard closed resources
		rp.numResources--
	}
	if rp.maxResources != -1 && rp.numResources >= rp.maxResources {
		// we need to wait until something comes back in
		rp.activeWaits = append(rp.activeWaits, acq)
		return
	}

	r, err := rp.factory()
	if err != nil {
		acq.ech <- err
	} else {
		rp.numResources++
		acq.rch <- r
	}

	return
}

func (rp *ResourcePool) release(resource Resource) {
	if resource == nil || resource.IsClosed() {
		// don't put it back in the pool.
		rp.numResources--
		return
	}
	if rp.idleCapacity != -1 && rp.idleResources.Size() == rp.idleCapacity {
		resource.Close()
		rp.numResources--
		return
	}

	rp.idleResources.Enqueue(resource)
}

func (rp *ResourcePool) empty() {
	for !rp.idleResources.Empty() {
		rp.idleResources.Dequeue().Close()
		rp.numResources -= 1
	}
}

func (rp *ResourcePool) IsClosed() bool {
	rp.closedMutex.RLock()
	defer rp.closedMutex.RUnlock()

	return rp.closed
}

// Acquire() will get one of the idle resources, or create a new one.
func (rp *ResourcePool) Acquire() (resource Resource, err error) {
	if rp.IsClosed() {
		return nil, PoolClosed
	}

	acq := acquireMessage{
		rch: make(chan Resource),
		ech: make(chan error),
	}
	rp.acqchan <- acq

	select {
	case resource = <-acq.rch:
	case err = <-acq.ech:
	}

	return
}

// Release() will release a resource for use by others. If the idle queue is
// full, the resource will be closed.
func (rp *ResourcePool) Release(resource Resource) {
	if rp.IsClosed() {
		if !resource.IsClosed() {
			resource.Close()
		}

		rp.numResourcesClosedMutex.Lock()
		rp.numResources -= 1
		rp.numResourcesClosedMutex.Unlock()

		return
	}
	rel := releaseMessage{
		r: resource,
	}
	rp.rchan <- rel
}

// Close() closes all the pool's resources.
func (rp *ResourcePool) Close() {
	if rp.IsClosed() {
		return
	}
	rp.cchan <- closeMessage{}
}

// Empty() removes idle pool's resources.
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

// NumResources() the number of resources known at this time
func (rp *ResourcePool) NumResources() int {
	return rp.numResources
}
