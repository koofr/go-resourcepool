package resourcepool

import (
	"context"
	"errors"
	"sync/atomic"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var created int64
var active int64

func getCreated() int {
	return int(atomic.LoadInt64(&created))
}

func getActive() int {
	return int(atomic.LoadInt64(&active))
}

type testresource struct {
	closed     bool
	onClose    func()
	onIsClosed func()
}

func newTestresource() *testresource {
	atomic.AddInt64(&active, 1)
	atomic.AddInt64(&created, 1)

	return &testresource{
		closed: false,
	}
}

func (r *testresource) Close() {
	if r.onClose != nil {
		r.onClose()
	}

	atomic.AddInt64(&active, -1)

	r.closed = true
}

func (r *testresource) IsClosed() bool {
	if r.onIsClosed != nil {
		r.onIsClosed()
	}
	return r.closed
}

func factory(ctx context.Context) (Resource, error) {
	return newTestresource(), nil
}

var _ = Describe("ResourcePool", func() {
	var idleCapacity int
	var maxResources int
	var currentFactory Factory
	var pool *ResourcePool

	BeforeEach(func() {
		created = 0
		active = 0
	})

	AfterEach(func() {
		Eventually(getActive).Should(Equal(0))
	})

	createPool := func(factory Factory, createIdleCapacity int, createMaxResources int) {
		currentFactory = factory
		idleCapacity = createIdleCapacity
		maxResources = createMaxResources
		pool = NewResourcePool(func(ctx context.Context) (Resource, error) {
			return currentFactory(ctx)
		}, idleCapacity, maxResources)
	}

	closePool := func() {
		pool.Close()
	}

	It("should create new pool", func() {
		createPool(factory, 2, 10)
		defer closePool()

		Expect(pool.NumResources()).To(Equal(0))
		Expect(pool.NumIdleResources()).To(Equal(0))
		Expect(getActive()).To(Equal(0))
	})

	Describe("Acquire", func() {
		It("should acquire new resource", func() {
			createPool(factory, 2, 10)
			defer closePool()

			r, err := pool.Acquire(context.Background())
			Expect(err).NotTo(HaveOccurred())

			Eventually(pool.NumResources).Should(Equal(1))
			Eventually(getActive).Should(Equal(1))

			r.Close()
			Eventually(getActive).Should(Equal(0))

			Eventually(pool.NumResources).Should(Equal(1))

			pool.Release(r)

			Eventually(pool.NumResources).Should(Equal(0))
			Eventually(pool.NumIdleResources).Should(Equal(0))
		})

		It("should not acquire new resource", func() {
			myfactory := func(ctx context.Context) (Resource, error) {
				return nil, errors.New("could not create new resource")
			}

			createPool(myfactory, 2, 10)
			defer closePool()

			_, err := pool.Acquire(context.Background())
			Expect(err).To(HaveOccurred())

			Eventually(pool.NumResources).Should(Equal(0))
		})

		It("should acquire new resource and keep it idle", func() {
			createPool(factory, 2, 10)
			defer closePool()

			r, err := pool.Acquire(context.Background())
			Expect(err).NotTo(HaveOccurred())

			Eventually(pool.NumResources).Should(Equal(1))
			Eventually(pool.NumIdleResources).Should(Equal(0))
			Eventually(getActive).Should(Equal(1))

			pool.Release(r)

			Eventually(pool.NumResources).Should(Equal(1))
			Eventually(pool.NumIdleResources).Should(Equal(1))
			Eventually(getActive).Should(Equal(1))
		})

		It("should acquire new resource and release it", func() {
			createPool(factory, 0, 10)
			defer closePool()

			r, err := pool.Acquire(context.Background())
			Expect(err).NotTo(HaveOccurred())

			Eventually(pool.NumResources).Should(Equal(1))
			Eventually(pool.NumIdleResources).Should(Equal(0))
			Eventually(getActive).Should(Equal(1))

			pool.Release(r)

			Eventually(pool.NumResources).Should(Equal(0))
			Eventually(pool.NumIdleResources).Should(Equal(0))
			Eventually(getActive).Should(Equal(0))
		})

		It("should acquire resource from idle pool", func() {
			createPool(factory, 2, 10)
			defer closePool()

			r, err := pool.Acquire(context.Background())
			Expect(err).NotTo(HaveOccurred())

			pool.Release(r)

			Eventually(pool.NumResources).Should(Equal(1))
			Eventually(pool.NumIdleResources).Should(Equal(1))
			Eventually(getCreated).Should(Equal(1))
			Eventually(getActive).Should(Equal(1))

			r, err = pool.Acquire(context.Background())
			Expect(err).NotTo(HaveOccurred())

			Eventually(pool.NumResources).Should(Equal(1))
			Eventually(pool.NumIdleResources).Should(Equal(0))
			Eventually(getCreated).Should(Equal(1))
			Eventually(getActive).Should(Equal(1))

			pool.Release(r)

			Eventually(pool.NumResources).Should(Equal(1))
			Eventually(pool.NumIdleResources).Should(Equal(1))
			Eventually(getCreated).Should(Equal(1))
			Eventually(getActive).Should(Equal(1))
		})

		It("should acquire resource from idle pool (already canceled context)", func() {
			createPool(factory, 2, 10)
			defer closePool()

			r, err := pool.Acquire(context.Background())
			Expect(err).NotTo(HaveOccurred())

			pool.Release(r)

			Eventually(pool.NumResources).Should(Equal(1))
			Eventually(pool.NumIdleResources).Should(Equal(1))
			Eventually(getCreated).Should(Equal(1))
			Eventually(getActive).Should(Equal(1))

			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			_, err = pool.Acquire(ctx)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(context.Canceled))

			Eventually(pool.NumResources).Should(Equal(1))
			Eventually(pool.NumIdleResources).Should(Equal(1))
			Eventually(getCreated).Should(Equal(1))
			Eventually(getActive).Should(Equal(1))
		})

		It("should acquire resource from idle pool (canceled while waiting for acquire chan)", func() {
			isAcquiring := make(chan interface{})
			isCanceled := make(chan interface{})

			ctx, cancel := context.WithCancel(context.Background())

			createPool(func(ctx context.Context) (Resource, error) {
				isAcquiring <- nil
				time.Sleep(100 * time.Millisecond)
				cancel()
				<-isCanceled
				isAcquiring <- nil
				return factory(ctx)
			}, 2, 10)
			defer closePool()

			go func() {
				defer GinkgoRecover()
				r, err := pool.Acquire(context.Background())
				Expect(err).NotTo(HaveOccurred())
				pool.Release(r)
			}()

			<-isAcquiring

			_, err := pool.Acquire(ctx)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(context.Canceled))

			isCanceled <- nil
			<-isAcquiring
		})

		It("should acquire resource from idle pool (canceled while acquiring)", func() {
			createPool(factory, 2, 10)
			defer closePool()

			r, err := pool.Acquire(context.Background())
			Expect(err).NotTo(HaveOccurred())

			pool.Release(r)

			Eventually(pool.NumResources).Should(Equal(1))
			Eventually(pool.NumIdleResources).Should(Equal(1))
			Eventually(getCreated).Should(Equal(1))
			Eventually(getActive).Should(Equal(1))

			ctx, cancel := context.WithCancel(context.Background())

			r.(*testresource).onIsClosed = func() {
				cancel()
			}

			_, err = pool.Acquire(ctx)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(context.Canceled))

			Eventually(pool.NumResources).Should(Equal(1))
			Eventually(pool.NumIdleResources).Should(Equal(1))
			Eventually(getCreated).Should(Equal(1))
			Eventually(getActive).Should(Equal(1))
		})

		It("should acquire resource from idle pool and create new one", func() {
			createPool(factory, 2, 10)
			defer closePool()

			r, err := pool.Acquire(context.Background())
			Expect(err).NotTo(HaveOccurred())

			pool.Release(r)

			Eventually(pool.NumResources).Should(Equal(1))
			Eventually(pool.NumIdleResources).Should(Equal(1))
			Eventually(getCreated).Should(Equal(1))
			Eventually(getActive).Should(Equal(1))

			r.Close()

			r, err = pool.Acquire(context.Background())
			Expect(err).NotTo(HaveOccurred())

			Eventually(pool.NumResources).Should(Equal(1))
			Eventually(pool.NumIdleResources).Should(Equal(0))
			Eventually(getCreated).Should(Equal(2))
			Eventually(getActive).Should(Equal(1))

			pool.Release(r)

			Eventually(pool.NumResources).Should(Equal(1))
			Eventually(pool.NumIdleResources).Should(Equal(1))
			Eventually(getCreated).Should(Equal(2))
			Eventually(getActive).Should(Equal(1))
		})

		It("should wait for available resource", func() {
			createPool(factory, 1, 1)
			defer closePool()

			r, err := pool.Acquire(context.Background())
			Expect(err).NotTo(HaveOccurred())

			Eventually(pool.NumResources).Should(Equal(1))
			Eventually(pool.NumIdleResources).Should(Equal(0))
			Eventually(getActive).Should(Equal(1))

			waiting := make(chan interface{}, 1)
			acquired := make(chan interface{}, 1)

			newResourceChan := make(chan Resource)

			go func() {
				defer GinkgoRecover()

				waiting <- nil

				r, err := pool.Acquire(context.Background())
				Expect(err).NotTo(HaveOccurred())

				acquired <- nil

				newResourceChan <- r
			}()

			<-waiting

			pool.Release(r)

			Eventually(pool.NumResources).Should(Equal(1))
			Eventually(pool.NumIdleResources).Should(Equal(0))
			Eventually(getActive).Should(Equal(1))

			Eventually(acquired).Should(Receive())

			var newResource Resource
			Eventually(newResourceChan).Should(Receive(&newResource))

			pool.Release(newResource)

			Eventually(pool.NumResources).Should(Equal(1))
			Eventually(pool.NumIdleResources).Should(Equal(1))
			Eventually(getActive).Should(Equal(1))
		})

		It("should wait for available resource (cancel context)", func() {
			createPool(factory, 1, 1)
			defer closePool()

			r, err := pool.Acquire(context.Background())
			Expect(err).NotTo(HaveOccurred())

			Eventually(pool.NumResources).Should(Equal(1))
			Eventually(pool.NumIdleResources).Should(Equal(0))
			Eventually(getActive).Should(Equal(1))

			waiting := make(chan interface{}, 1)
			acquired := make(chan interface{}, 1)

			ctx, cancel := context.WithCancel(context.Background())

			go func() {
				defer GinkgoRecover()

				waiting <- nil

				_, err := pool.Acquire(ctx)
				Expect(err).To(HaveOccurred())
				Expect(err).To(Equal(context.Canceled))

				acquired <- nil
			}()

			<-waiting

			Eventually(pool.NumActiveWaits).Should(Equal(1))

			cancel()

			Eventually(acquired).Should(Receive())

			Expect(pool.NumActiveWaits()).To(Equal(1))

			pool.Release(r)

			Eventually(pool.NumResources).Should(Equal(1))
			Eventually(pool.NumIdleResources).Should(Equal(1))
			Eventually(pool.NumActiveWaits).Should(Equal(0))
		})

		It("should wait for available resource (cancel context while releasing)", func() {
			createPool(factory, 1, 1)
			defer closePool()

			r, err := pool.Acquire(context.Background())
			Expect(err).NotTo(HaveOccurred())

			Eventually(pool.NumResources).Should(Equal(1))
			Eventually(pool.NumIdleResources).Should(Equal(0))
			Eventually(getActive).Should(Equal(1))

			waiting := make(chan interface{}, 1)
			acquired := make(chan interface{}, 1)

			ctx, cancel := context.WithCancel(context.Background())

			go func() {
				defer GinkgoRecover()

				waiting <- nil

				_, err := pool.Acquire(ctx)
				Expect(err).To(HaveOccurred())
				Expect(err).To(Equal(context.Canceled))

				acquired <- nil
			}()

			<-waiting

			Eventually(pool.NumActiveWaits).Should(Equal(1))

			r.(*testresource).onIsClosed = func() {
				cancel()
			}

			pool.Release(r)

			Eventually(acquired).Should(Receive())

			Eventually(pool.NumResources).Should(Equal(1))
			Eventually(pool.NumIdleResources).Should(Equal(1))
			Eventually(pool.NumActiveWaits).Should(Equal(0))
		})

		It("should wait for available resource (cancel context while releasing closed, factory fails)", func() {
			createPool(factory, 1, 1)
			defer closePool()

			r, err := pool.Acquire(context.Background())
			Expect(err).NotTo(HaveOccurred())

			Eventually(pool.NumResources).Should(Equal(1))
			Eventually(pool.NumIdleResources).Should(Equal(0))
			Eventually(getActive).Should(Equal(1))

			waiting := make(chan interface{}, 1)
			acquired := make(chan interface{}, 1)

			ctx, cancel := context.WithCancel(context.Background())

			go func() {
				defer GinkgoRecover()

				waiting <- nil

				_, err := pool.Acquire(ctx)
				Expect(err).To(HaveOccurred())
				Expect(err).To(Equal(context.Canceled))

				acquired <- nil
			}()

			<-waiting

			Eventually(pool.NumActiveWaits).Should(Equal(1))

			r.Close()

			currentFactory = func(ctx context.Context) (Resource, error) {
				cancel()
				return nil, errors.New("could not create new resource")
			}

			pool.Release(r)

			Eventually(acquired).Should(Receive())

			Eventually(pool.NumResources).Should(Equal(0))
			Eventually(pool.NumIdleResources).Should(Equal(0))
			Eventually(pool.NumActiveWaits).Should(Equal(0))
		})

		It("should wait for available resource (cancel context while releasing closed)", func() {
			currentFactory := factory

			createPool(func(ctx context.Context) (Resource, error) {
				return currentFactory(ctx)
			}, 1, 1)
			defer closePool()

			r, err := pool.Acquire(context.Background())
			Expect(err).NotTo(HaveOccurred())

			Eventually(pool.NumResources).Should(Equal(1))
			Eventually(pool.NumIdleResources).Should(Equal(0))
			Eventually(getActive).Should(Equal(1))

			waiting := make(chan interface{}, 1)
			acquired := make(chan interface{}, 1)

			ctx, cancel := context.WithCancel(context.Background())

			go func() {
				defer GinkgoRecover()

				waiting <- nil

				_, err := pool.Acquire(ctx)
				Expect(err).To(HaveOccurred())
				Expect(err).To(Equal(context.Canceled))

				acquired <- nil
			}()

			<-waiting

			Eventually(pool.NumActiveWaits).Should(Equal(1))

			r.Close()

			currentFactory = func(ctx context.Context) (Resource, error) {
				cancel()
				return factory(ctx)
			}

			pool.Release(r)

			Eventually(acquired).Should(Receive())

			Eventually(pool.NumResources).Should(Equal(1))
			Eventually(pool.NumIdleResources).Should(Equal(1))
			Eventually(pool.NumActiveWaits).Should(Equal(0))
		})

		It("should wait for available resource and create new one", func() {
			createPool(factory, 1, 1)
			defer closePool()

			r, err := pool.Acquire(context.Background())
			Expect(err).NotTo(HaveOccurred())

			Eventually(pool.NumResources).Should(Equal(1))
			Eventually(pool.NumIdleResources).Should(Equal(0))
			Eventually(getActive).Should(Equal(1))
			Eventually(getCreated).Should(Equal(1))

			waiting := make(chan interface{}, 1)
			acquired := make(chan interface{}, 1)

			newResourceChan := make(chan Resource)

			go func() {
				defer GinkgoRecover()

				waiting <- nil

				r, err := pool.Acquire(context.Background())
				Expect(err).NotTo(HaveOccurred())

				acquired <- nil

				newResourceChan <- r
			}()

			<-waiting

			r.Close()
			pool.Release(r)

			Eventually(pool.NumResources).Should(Equal(1))
			Eventually(pool.NumIdleResources).Should(Equal(0))
			Eventually(getActive).Should(Equal(1))
			Eventually(getCreated).Should(Equal(2))

			Eventually(acquired).Should(Receive())

			var newResource Resource
			Eventually(newResourceChan).Should(Receive(&newResource))

			pool.Release(newResource)

			Eventually(pool.NumResources).Should(Equal(1))
			Eventually(pool.NumIdleResources).Should(Equal(1))
			Eventually(getActive).Should(Equal(1))
			Eventually(getCreated).Should(Equal(2))
		})

		It("should wait for available resource and get error", func() {
			createPool(factory, 1, 1)
			defer closePool()

			r, err := pool.Acquire(context.Background())
			Expect(err).NotTo(HaveOccurred())

			Eventually(pool.NumResources).Should(Equal(1))
			Eventually(pool.NumIdleResources).Should(Equal(0))
			Eventually(getActive).Should(Equal(1))
			Eventually(getCreated).Should(Equal(1))

			waiting := make(chan interface{}, 1)
			acquired := make(chan interface{}, 1)

			go func() {
				defer GinkgoRecover()

				waiting <- nil

				_, err := pool.Acquire(context.Background())
				Expect(err).To(HaveOccurred())

				acquired <- nil
			}()

			<-waiting

			currentFactory = func(context.Context) (Resource, error) {
				return nil, errors.New("could not create new resource")
			}

			r.Close()
			pool.Release(r)

			Eventually(pool.NumResources).Should(Equal(0))
			Eventually(pool.NumIdleResources).Should(Equal(0))
			Eventually(getActive).Should(Equal(0))
			Eventually(getCreated).Should(Equal(1))

			Eventually(acquired).Should(Receive())
		})

		It("should wait for available resource and get closed pool error", func() {
			createPool(factory, 1, 1)

			r, err := pool.Acquire(context.Background())
			Expect(err).NotTo(HaveOccurred())

			Eventually(pool.NumResources).Should(Equal(1))
			Eventually(pool.NumIdleResources).Should(Equal(0))
			Eventually(getActive).Should(Equal(1))
			Eventually(getCreated).Should(Equal(1))

			waiting := make(chan interface{}, 1)
			acquired := make(chan interface{}, 1)

			go func() {
				defer GinkgoRecover()

				waiting <- nil

				_, err := pool.Acquire(context.Background())
				Expect(err).To(HaveOccurred())
				Expect(err).To(Equal(ErrPoolClosed))

				acquired <- nil
			}()

			<-waiting

			Eventually(pool.NumActiveWaits).Should(Equal(1))

			Expect(acquired).NotTo(Receive())

			pool.Close()

			Eventually(pool.NumActiveWaits).Should(Equal(0))

			Eventually(pool.NumResources).Should(Equal(1))
			Eventually(pool.NumIdleResources).Should(Equal(0))
			Eventually(getActive).Should(Equal(1))
			Eventually(getCreated).Should(Equal(1))

			pool.Release(r)

			Eventually(acquired).Should(Receive())

			Eventually(pool.NumResources).Should(Equal(0))
			Eventually(pool.NumIdleResources).Should(Equal(0))
			Eventually(getActive).Should(Equal(0))
		})
	})

	Describe("Empty", func() {
		It("should close only idle resources", func() {
			createPool(factory, 2, 10)
			defer closePool()

			r, err := pool.Acquire(context.Background())
			Expect(err).NotTo(HaveOccurred())

			r1, err := pool.Acquire(context.Background())
			Expect(err).NotTo(HaveOccurred())

			pool.Release(r)

			Eventually(pool.NumResources).Should(Equal(2))
			Eventually(pool.NumIdleResources).Should(Equal(1))
			Eventually(getActive).Should(Equal(2))

			pool.Empty()

			Eventually(pool.NumResources).Should(Equal(1))
			Eventually(pool.NumIdleResources).Should(Equal(0))
			Eventually(getActive).Should(Equal(1))

			r1.Close()
			pool.Release(r1)

			Eventually(pool.NumResources).Should(Equal(0))
			Eventually(pool.NumIdleResources).Should(Equal(0))
			Eventually(getActive).Should(Equal(0))

			r, err = pool.Acquire(context.Background())
			Expect(err).NotTo(HaveOccurred())

			Eventually(pool.NumResources).Should(Equal(1))
			Eventually(pool.NumIdleResources).Should(Equal(0))
			Eventually(getActive).Should(Equal(1))

			pool.Release(r)

			Eventually(pool.NumResources).Should(Equal(1))
			Eventually(pool.NumIdleResources).Should(Equal(1))
			Eventually(getActive).Should(Equal(1))
		})

		It("should drain the empty channel", func() {
			createPool(factory, 2, 10)
			defer closePool()

			r, err := pool.Acquire(context.Background())
			Expect(err).NotTo(HaveOccurred())

			pool.Release(r)

			Eventually(pool.NumResources).Should(Equal(1))
			Eventually(pool.NumIdleResources).Should(Equal(1))
			Eventually(getActive).Should(Equal(1))

			r.(*testresource).onClose = func() {
				go pool.Empty()
				time.Sleep(20 * time.Millisecond)
			}

			pool.Empty()

			Eventually(pool.NumResources).Should(Equal(0))
			Eventually(pool.NumIdleResources).Should(Equal(0))
			Eventually(getActive).Should(Equal(0))
		})

		It("should not do anything if pool is closed", func() {
			createPool(factory, 2, 10)
			defer closePool()

			pool.Close()

			pool.Empty()
		})

		It("should empty the pool before acquires", func() {
			for j := 0; j < 10; j++ {
				func() {
					created = 0
					active = 0

					createPool(factory, 10, 20)
					defer closePool()

					rs := make([]Resource, 10)

					for i := range rs {
						r, err := pool.Acquire(context.Background())
						Expect(err).NotTo(HaveOccurred())
						rs[i] = r
					}

					for i := range rs {
						pool.Release(rs[i])
					}

					Eventually(pool.NumResources).Should(Equal(10))
					Eventually(pool.NumIdleResources).Should(Equal(10))
					Eventually(getActive).Should(Equal(10))
					Eventually(getCreated).Should(Equal(10))

					for i := 0; i < 10; i++ {
						r, err := pool.Acquire(context.Background())
						Expect(err).NotTo(HaveOccurred())

						r.Close()

						pool.Release(r)

						pool.Empty()
					}

					Eventually(getActive).Should(Equal(0))
					Eventually(getCreated).Should(Equal(19))

					pool.Empty()

					Eventually(pool.NumResources).Should(Equal(0))
					Eventually(pool.NumIdleResources).Should(Equal(0))
					Eventually(getActive).Should(Equal(0))
				}()
			}
		})
	})

	Describe("Close", func() {
		It("should close idle resources and close pool", func() {
			createPool(factory, 2, 10)

			r, err := pool.Acquire(context.Background())
			Expect(err).NotTo(HaveOccurred())

			pool.Release(r)

			Eventually(pool.NumResources).Should(Equal(1))
			Eventually(pool.NumIdleResources).Should(Equal(1))
			Eventually(getActive).Should(Equal(1))

			pool.Close()

			Eventually(pool.NumResources).Should(Equal(0))
			Eventually(pool.NumIdleResources).Should(Equal(0))
			Eventually(getActive).Should(Equal(0))

			_, err = pool.Acquire(context.Background())
			Expect(err).To(Equal(ErrPoolClosed))
		})

		It("should be idempotent", func() {
			createPool(factory, 2, 10)

			r, err := pool.Acquire(context.Background())
			Expect(err).NotTo(HaveOccurred())

			pool.Release(r)

			Eventually(pool.NumResources).Should(Equal(1))
			Eventually(pool.NumIdleResources).Should(Equal(1))
			Eventually(getActive).Should(Equal(1))

			pool.Close()

			Eventually(pool.NumResources).Should(Equal(0))
			Eventually(pool.NumIdleResources).Should(Equal(0))
			Eventually(getActive).Should(Equal(0))

			_, err = pool.Acquire(context.Background())
			Expect(err).To(Equal(ErrPoolClosed))

			pool.Close()
		})
	})

	Describe("NumResources", func() {
		It("should return the number of resources known at this time", func() {
			createPool(factory, 2, 10)
			defer closePool()

			Expect(pool.NumResources()).To(Equal(0))
		})
	})

})
