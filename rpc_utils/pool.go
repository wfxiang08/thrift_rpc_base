package rpc_utils

import (
	"container/list"
	"errors"
	"github.com/wfxiang08/go_thrift/thrift"
	"sync"
	"time"
)

//# redis
//参考: github.com/garyburd/redigo  836b6e58b3358112c8291565d01c35b8764070d7 Redis.Pool

var nowFunc = time.Now // for testing

// ErrPoolExhausted is returned from a pool connection method (Do, Send,
// Receive, Flush, Err) when the maximum number of database connections in the
// pool has been reached.
var ErrPoolExhausted = errors.New("RpcThrift: connection pool exhausted")

var (
	errPoolClosed = errors.New("RpcThrift: connection pool closed")
	errConnClosed = errors.New("RpcThrift: connection closed")
)

// Pool maintains a pool of connections. The application calls the Get method
// to get a connection from the pool and the connection's Close method to
// return the connection's resources to the pool.
//
// The following example shows how to use a pool in a web application. The
// application creates a pool at application startup and makes it available to
// request handlers using a global variable.
//
//	ThriftPool = &Pool{
//		Dial: func() (thrift.TTransport, error) {
//			// 如何创建一个Transport
//			t, err := thrift.NewTSocketTimeout(proxyAddress, time.Second*5)
//			trans := NewTFramedTransport(t)
//			return trans, err
//		},
//		MaxActive:   30,
//		MaxIdle:     30,
//		IdleTimeout: time.Second * 3600 * 24,
//		Wait:        true,
//	}
//
// A request handler gets a connection from the pool and closes the connection
// when the handler is done:
//
//  func serveHome(w http.ResponseWriter, r *http.Request) {
//      conn := ThriftPool.Get()
//      defer conn.Close()
//      ....
//  }
//
type Pool struct {

	// Dial is an application supplied function for creating and configuring a
	// connection.
	//
	// The connection returned from Dial must not be in a special state
	// (subscribed to pubsub channel, transaction started, ...).
	Dial func() (thrift.TTransport, error)

	// TestOnBorrow is an optional application supplied function for checking
	// the health of an idle connection before the connection is used again by
	// the application. Argument t is the time that the connection was returned
	// to the pool. If the function returns an error, then the connection is
	// closed.
	TestOnBorrow func(c thrift.TTransport, t time.Time) error

	// Maximum number of idle connections in the pool.
	MaxIdle int

	// Maximum number of connections allocated by the pool at a given time.
	// When zero, there is no limit on the number of connections in the pool.
	MaxActive int

	// Close connections after remaining idle for this duration. If the value
	// is zero, then idle connections are not closed. Applications should set
	// the timeout to a value less than the server's timeout.
	IdleTimeout time.Duration

	// If Wait is true and the pool is at the MaxActive limit, then Get() waits
	// for a connection to be returned to the pool before returning.
	Wait bool

	// mu protects fields defined below.
	mu     sync.Mutex
	cond   *sync.Cond
	closed bool
	active int

	// Stack of idleConn with most recently used at the front.
	idle list.List
}

type idleConn struct {
	c thrift.TTransport
	t time.Time
}

// NewPool creates a new pool.
//
// Deprecated: Initialize the Pool directory as shown in the example.
func NewPool(newFn func() (thrift.TTransport, error), maxIdle int) *Pool {
	return &Pool{Dial: newFn, MaxIdle: maxIdle}
}

// Get gets a connection. The application must close the returned connection.
// This method always returns a valid connection so that applications can defer
// error handling to the first use of the connection. If there is an error
// getting an underlying connection, then the connection Err, Do, Send, Flush
// and Receive methods return that error.
func (p *Pool) Get() thrift.TTransport {
	c, err := p.get()
	if err != nil {
		return &errorConnection{err}
	}
	return &pooledConnection{TTransport: c, p: p}
}

// ActiveCount returns the number of active connections in the pool.
func (p *Pool) ActiveCount() int {
	p.mu.Lock()
	active := p.active
	p.mu.Unlock()
	return active
}

// Close releases the resources used by the pool.
func (p *Pool) Close() error {
	p.mu.Lock()
	idle := p.idle
	p.idle.Init()
	p.closed = true
	p.active -= idle.Len()
	if p.cond != nil {
		p.cond.Broadcast()
	}
	p.mu.Unlock()
	for e := idle.Front(); e != nil; e = e.Next() {
		e.Value.(idleConn).c.Close()
	}
	return nil
}

// release decrements the active count and signals waiters. The caller must
// hold p.mu during the call.
func (p *Pool) release() {
	p.active -= 1
	if p.cond != nil {
		p.cond.Signal()
	}
}

// get prunes stale connections and returns a connection from the idle list or
// creates a new connection.
func (p *Pool) get() (thrift.TTransport, error) {
	p.mu.Lock()

	// Prune stale connections.

	if timeout := p.IdleTimeout; timeout > 0 {
		for i, n := 0, p.idle.Len(); i < n; i++ {
			e := p.idle.Back()
			if e == nil {
				break
			}
			ic := e.Value.(idleConn)
			if ic.t.Add(timeout).After(nowFunc()) {
				break
			}
			p.idle.Remove(e)
			p.release()
			p.mu.Unlock()
			ic.c.Close()
			p.mu.Lock()
		}
	}

	for {

		// Get idle connection.
		for i, n := 0, p.idle.Len(); i < n; i++ {
			e := p.idle.Front()
			if e == nil {
				break
			}
			ic := e.Value.(idleConn)
			p.idle.Remove(e)

			// 如何证明idle Connection是有效的呢?
			test := p.TestOnBorrow
			p.mu.Unlock()
			if test == nil || test(ic.c, ic.t) == nil {
				return ic.c, nil
			}
			ic.c.Close()
			p.mu.Lock()
			p.release()
		}

		// Check for pool closed before dialing a new connection.
		if p.closed {
			p.mu.Unlock()
			return nil, errors.New("RpcThrift: get on closed pool")
		}

		// Dial new connection if under limit.

		if p.MaxActive == 0 || p.active < p.MaxActive {
			// 尽早释放: mutext
			dial := p.Dial
			p.active += 1
			p.mu.Unlock()

			c, err := dial()
			if err != nil {
				p.mu.Lock()
				p.release()
				p.mu.Unlock()
				c = nil
			}
			return c, err
		}

		// 如果不等待，则直接返回
		if !p.Wait {
			p.mu.Unlock()
			return nil, ErrPoolExhausted
		}

		if p.cond == nil {
			p.cond = sync.NewCond(&p.mu)
		}
		p.cond.Wait()
	}
}

func (p *Pool) put(c thrift.TTransport, forceClose bool) error {
	p.mu.Lock()
	if !p.closed && !forceClose {
		p.idle.PushFront(idleConn{t: nowFunc(), c: c})

		// 控制: MaxIdle
		if p.idle.Len() > p.MaxIdle {
			c = p.idle.Remove(p.idle.Back()).(idleConn).c
		} else {
			c = nil
		}
	}

	if c == nil {
		// 如果存在有效归还，则通知: cond
		if p.cond != nil {
			p.cond.Signal()
		}
		p.mu.Unlock()
		return nil
	} else {
		// c 是一个多余的connection
		p.release()
		p.mu.Unlock()
		return c.Close()
	}
}

type pooledConnection struct {
	thrift.TTransport
	p *Pool
}

// 如何Close呢?
func (pc *pooledConnection) Close() error {
	c := pc.TTransport
	return pc.p.put(c, false)
}

type errorConnection struct {
	err error
}

func (this *errorConnection) Open() error                       { return this.err }
func (this *errorConnection) IsOpen() bool                      { return false }
func (this *errorConnection) Read(p []byte) (n int, err error)  { return 0, this.err }
func (this *errorConnection) Close() error                      { return this.err }
func (this *errorConnection) Write(p []byte) (n int, err error) { return 0, this.err }
func (this *errorConnection) Flush() (err error)                { return this.err }

func GetProtocolFromTransport(transport thrift.TTransport, serviceName string) (ip thrift.TProtocol, op thrift.TProtocol) {
	op = thrift.NewTMultiplexedProtocol(thrift.NewTBinaryProtocol(transport, false, true), serviceName)
	ip = thrift.NewTBinaryProtocol(transport, false, true)
	return
}
