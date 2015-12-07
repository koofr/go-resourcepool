package resourcepool

import (
	"errors"
	"runtime"
	"sync"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var created = 0
var active = 0
var activeMutex sync.Mutex

type testresource struct {
	closed bool
}

func NewTestresource() *testresource {
	activeMutex.Lock()
	defer activeMutex.Unlock()
	active += 1
	created += 1

	return &testresource{false}
}

func (r *testresource) Close() {
	activeMutex.Lock()
	defer activeMutex.Unlock()
	active -= 1

	r.closed = true
}

func (r *testresource) IsClosed() bool {
	return r.closed
}

func factory() (Resource, error) {
	return NewTestresource(), nil
}

var _ = Describe("ResourcePool", func() {
	runtime.GOMAXPROCS(1)

	BeforeEach(func() {
		created = 0
		Expect(active).To(Equal(0))
	})

	AfterEach(func() {
		Expect(active).To(Equal(0))
	})

	var closePool = func(pool *ResourcePool) {
		pool.Close()
		runtime.Gosched()
	}

	It("should create new pool", func() {
		pool := NewResourcePool(factory, 2, 10)
		defer pool.Close()

		Expect(pool.idleCapacity).To(Equal(2))
		Expect(pool.maxResources).To(Equal(10))

		Expect(pool.numResources).To(Equal(0))
		Expect(pool.idleResources.Size()).To(Equal(0))
		Expect(active).To(Equal(0))
	})

	Describe("Acquire", func() {
		It("should acquire new resource", func() {
			pool := NewResourcePool(factory, 2, 10)
			defer closePool(pool)

			r, err := pool.Acquire()
			Expect(err).NotTo(HaveOccurred())

			Expect(pool.numResources).To(Equal(1))
			Expect(active).To(Equal(1))

			r.Close()
			Expect(active).To(Equal(0))

			Expect(pool.numResources).To(Equal(1))

			pool.Release(r)
			runtime.Gosched()

			Expect(pool.numResources).To(Equal(0))
			Expect(pool.idleResources.Size()).To(Equal(0))
		})

		It("should not acquire new resource", func() {
			myfactory := func() (Resource, error) {
				return nil, errors.New("could not create new resource")
			}

			pool := NewResourcePool(myfactory, 2, 10)
			defer closePool(pool)

			_, err := pool.Acquire()
			Expect(err).To(HaveOccurred())

			Expect(pool.numResources).To(Equal(0))
		})

		It("should acquire new resource and keep it idle", func() {
			pool := NewResourcePool(factory, 2, 10)
			defer closePool(pool)

			r, err := pool.Acquire()
			Expect(err).NotTo(HaveOccurred())

			Expect(pool.numResources).To(Equal(1))
			Expect(pool.idleResources.Size()).To(Equal(0))
			Expect(active).To(Equal(1))

			pool.Release(r)
			runtime.Gosched()

			Expect(pool.numResources).To(Equal(1))
			Expect(pool.idleResources.Size()).To(Equal(1))
			Expect(active).To(Equal(1))
		})

		It("should acquire new resource and release it", func() {
			pool := NewResourcePool(factory, 0, 10)
			defer closePool(pool)

			r, err := pool.Acquire()
			Expect(err).NotTo(HaveOccurred())

			Expect(pool.numResources).To(Equal(1))
			Expect(pool.idleResources.Size()).To(Equal(0))
			Expect(active).To(Equal(1))

			pool.Release(r)
			runtime.Gosched()

			Expect(pool.numResources).To(Equal(0))
			Expect(pool.idleResources.Size()).To(Equal(0))
			Expect(active).To(Equal(0))
		})

		It("should acquire resource from idle pool", func() {
			pool := NewResourcePool(factory, 2, 10)
			defer closePool(pool)

			r, err := pool.Acquire()
			Expect(err).NotTo(HaveOccurred())

			pool.Release(r)
			runtime.Gosched()

			Expect(pool.numResources).To(Equal(1))
			Expect(pool.idleResources.Size()).To(Equal(1))
			Expect(created).To(Equal(1))
			Expect(active).To(Equal(1))

			r, err = pool.Acquire()
			Expect(err).NotTo(HaveOccurred())

			Expect(pool.numResources).To(Equal(1))
			Expect(pool.idleResources.Size()).To(Equal(0))
			Expect(created).To(Equal(1))
			Expect(active).To(Equal(1))

			pool.Release(r)
			runtime.Gosched()

			Expect(pool.numResources).To(Equal(1))
			Expect(pool.idleResources.Size()).To(Equal(1))
			Expect(created).To(Equal(1))
			Expect(active).To(Equal(1))
		})

		It("should acquire resource from idle pool and create new one", func() {
			pool := NewResourcePool(factory, 2, 10)
			defer closePool(pool)

			r, err := pool.Acquire()
			Expect(err).NotTo(HaveOccurred())

			pool.Release(r)
			runtime.Gosched()

			Expect(pool.numResources).To(Equal(1))
			Expect(pool.idleResources.Size()).To(Equal(1))
			Expect(created).To(Equal(1))
			Expect(active).To(Equal(1))

			r.Close()

			r, err = pool.Acquire()
			Expect(err).NotTo(HaveOccurred())

			Expect(pool.numResources).To(Equal(1))
			Expect(pool.idleResources.Size()).To(Equal(0))
			Expect(created).To(Equal(2))
			Expect(active).To(Equal(1))

			pool.Release(r)
			runtime.Gosched()

			Expect(pool.numResources).To(Equal(1))
			Expect(pool.idleResources.Size()).To(Equal(1))
			Expect(created).To(Equal(2))
			Expect(active).To(Equal(1))
		})

		It("should wait for available resource", func() {
			pool := NewResourcePool(factory, 1, 1)
			defer closePool(pool)

			r, err := pool.Acquire()
			Expect(err).NotTo(HaveOccurred())

			Expect(pool.numResources).To(Equal(1))
			Expect(pool.idleResources.Size()).To(Equal(0))
			Expect(active).To(Equal(1))

			waiting := false
			acquired := false
			var newR Resource

			go func() {
				waiting = true

				r, err := pool.Acquire()
				Expect(err).NotTo(HaveOccurred())

				acquired = true

				newR = r
			}()

			runtime.Gosched()

			Expect(waiting).To(BeTrue())
			Expect(acquired).To(BeFalse())

			pool.Release(r)
			runtime.Gosched()

			Expect(pool.numResources).To(Equal(1))
			Expect(pool.idleResources.Size()).To(Equal(0))
			Expect(active).To(Equal(1))

			Expect(acquired).To(BeTrue())

			pool.Release(newR)
			runtime.Gosched()

			Expect(pool.numResources).To(Equal(1))
			Expect(pool.idleResources.Size()).To(Equal(1))
			Expect(active).To(Equal(1))
		})

		It("should wait for available resource and create new one", func() {
			pool := NewResourcePool(factory, 1, 1)
			defer closePool(pool)

			r, err := pool.Acquire()
			Expect(err).NotTo(HaveOccurred())

			Expect(pool.numResources).To(Equal(1))
			Expect(pool.idleResources.Size()).To(Equal(0))
			Expect(active).To(Equal(1))
			Expect(created).To(Equal(1))

			waiting := false
			acquired := false
			var newR Resource

			go func() {
				defer GinkgoRecover()

				waiting = true

				r, err := pool.Acquire()
				Expect(err).NotTo(HaveOccurred())

				acquired = true

				newR = r
			}()

			runtime.Gosched()

			Expect(waiting).To(BeTrue())
			Expect(acquired).To(BeFalse())

			r.Close()
			pool.Release(r)
			runtime.Gosched()

			Expect(pool.numResources).To(Equal(1))
			Expect(pool.idleResources.Size()).To(Equal(0))
			Expect(active).To(Equal(1))
			Expect(created).To(Equal(2))

			Expect(acquired).To(BeTrue())

			pool.Release(newR)
			runtime.Gosched()

			Expect(pool.numResources).To(Equal(1))
			Expect(pool.idleResources.Size()).To(Equal(1))
			Expect(active).To(Equal(1))
			Expect(created).To(Equal(2))
		})

		It("should wait for available resource and get error", func() {
			factoryShouldFail := false

			myfactory := func() (Resource, error) {
				if factoryShouldFail {
					return nil, errors.New("could not create new resource")
				}

				return NewTestresource(), nil
			}

			pool := NewResourcePool(myfactory, 1, 1)
			defer closePool(pool)

			r, err := pool.Acquire()
			Expect(err).NotTo(HaveOccurred())

			Expect(pool.numResources).To(Equal(1))
			Expect(pool.idleResources.Size()).To(Equal(0))
			Expect(active).To(Equal(1))
			Expect(created).To(Equal(1))

			waiting := false
			acquired := false

			go func() {
				defer GinkgoRecover()

				waiting = true

				_, err := pool.Acquire()
				Expect(err).To(HaveOccurred())

				acquired = true
			}()

			runtime.Gosched()

			Expect(waiting).To(BeTrue())
			Expect(acquired).To(BeFalse())

			factoryShouldFail = true

			r.Close()
			pool.Release(r)
			runtime.Gosched()

			Expect(pool.numResources).To(Equal(0))
			Expect(pool.idleResources.Size()).To(Equal(0))
			Expect(active).To(Equal(0))
			Expect(created).To(Equal(1))

			Expect(acquired).To(BeTrue())
		})

		It("should wait for available resource and get closed pool error", func() {
			pool := NewResourcePool(factory, 1, 1)

			r, err := pool.Acquire()
			Expect(err).NotTo(HaveOccurred())

			Expect(pool.numResources).To(Equal(1))
			Expect(pool.idleResources.Size()).To(Equal(0))
			Expect(active).To(Equal(1))
			Expect(created).To(Equal(1))

			waiting := false
			acquired := false

			go func() {
				defer GinkgoRecover()

				waiting = true

				_, err := pool.Acquire()
				Expect(err).To(HaveOccurred())

				acquired = true
			}()

			runtime.Gosched()

			Expect(waiting).To(BeTrue())
			Expect(acquired).To(BeFalse())

			Expect(pool.activeWaits).To(HaveLen(1))

			pool.Close()
			runtime.Gosched()

			Expect(pool.activeWaits).To(HaveLen(0))

			Expect(pool.numResources).To(Equal(1))
			Expect(pool.idleResources.Size()).To(Equal(0))
			Expect(active).To(Equal(1))
			Expect(created).To(Equal(1))

			pool.Release(r)
			runtime.Gosched()

			Expect(acquired).To(BeTrue())

			Expect(pool.numResources).To(Equal(0))
			Expect(pool.idleResources.Size()).To(Equal(0))
			Expect(active).To(Equal(0))
		})
	})

	Describe("Empty", func() {
		It("should close only idle resources", func() {
			pool := NewResourcePool(factory, 2, 10)
			defer closePool(pool)

			r, err := pool.Acquire()
			Expect(err).NotTo(HaveOccurred())

			r1, err := pool.Acquire()
			Expect(err).NotTo(HaveOccurred())

			pool.Release(r)
			runtime.Gosched()

			Expect(pool.numResources).To(Equal(2))
			Expect(pool.idleResources.Size()).To(Equal(1))
			Expect(active).To(Equal(2))

			pool.Empty()
			runtime.Gosched()

			Expect(pool.numResources).To(Equal(1))
			Expect(pool.idleResources.Size()).To(Equal(0))
			Expect(active).To(Equal(1))

			r1.Close()
			pool.Release(r1)
			runtime.Gosched()

			Expect(pool.numResources).To(Equal(0))
			Expect(pool.idleResources.Size()).To(Equal(0))
			Expect(active).To(Equal(0))

			r, err = pool.Acquire()
			Expect(err).NotTo(HaveOccurred())

			Expect(pool.numResources).To(Equal(1))
			Expect(pool.idleResources.Size()).To(Equal(0))
			Expect(active).To(Equal(1))

			pool.Release(r)
			runtime.Gosched()

			Expect(pool.numResources).To(Equal(1))
			Expect(pool.idleResources.Size()).To(Equal(1))
			Expect(active).To(Equal(1))
		})

		It("should not do anything if pool is closed", func() {
			pool := NewResourcePool(factory, 2, 10)
			defer closePool(pool)

			pool.Close()
			runtime.Gosched()

			pool.Empty()
			runtime.Gosched()
		})
	})

	Describe("Close", func() {
		It("should close idle resources and close pool", func() {
			pool := NewResourcePool(factory, 2, 10)

			r, err := pool.Acquire()
			Expect(err).NotTo(HaveOccurred())

			pool.Release(r)
			runtime.Gosched()

			Expect(pool.numResources).To(Equal(1))
			Expect(pool.idleResources.Size()).To(Equal(1))
			Expect(active).To(Equal(1))

			pool.Close()
			runtime.Gosched()

			Expect(pool.numResources).To(Equal(0))
			Expect(pool.idleResources.Size()).To(Equal(0))
			Expect(active).To(Equal(0))

			_, err = pool.Acquire()
			Expect(err == PoolClosed).To(BeTrue())
		})

		It("should be idempotent", func() {
			pool := NewResourcePool(factory, 2, 10)

			r, err := pool.Acquire()
			Expect(err).NotTo(HaveOccurred())

			pool.Release(r)
			runtime.Gosched()

			Expect(pool.numResources).To(Equal(1))
			Expect(pool.idleResources.Size()).To(Equal(1))
			Expect(active).To(Equal(1))

			pool.Close()
			runtime.Gosched()

			Expect(pool.numResources).To(Equal(0))
			Expect(pool.idleResources.Size()).To(Equal(0))
			Expect(active).To(Equal(0))

			_, err = pool.Acquire()
			Expect(err == PoolClosed).To(BeTrue())

			pool.Close()
			runtime.Gosched()
		})
	})

	Describe("NumResources", func() {
		It("should return the number of resources known at this time", func() {
			pool := NewResourcePool(factory, 2, 10)
			defer closePool(pool)

			Expect(pool.NumResources()).To(Equal(0))
		})
	})

})
