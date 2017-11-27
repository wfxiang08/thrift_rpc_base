//// Copyright 2015 Spring Rain Software Compnay LTD. All Rights Reserved.
//// Licensed under the MIT (MIT-LICENSE.txt) license.
package rpc_utils

import (
	"context"
	log "github.com/wfxiang08/cyutils/utils/rolling_log"
	"github.com/wfxiang08/go_thrift/thrift"
	"net"
	"runtime/debug"
	"strings"
	"time"
)

type TGracefulServerSocket struct {
	listener      net.Listener
	clientTimeout time.Duration
}

func NewTGracefulServerSocket(listener net.Listener) *TGracefulServerSocket {
	return NewTGracefulServerSocketWithTimeout(listener, 0)
}

// listener 可以设置clientTimeout
func NewTGracefulServerSocketWithTimeout(listener net.Listener, clientTimeout time.Duration) *TGracefulServerSocket {

	return &TGracefulServerSocket{listener: listener, clientTimeout: clientTimeout}
}

func (p *TGracefulServerSocket) Accept() (thrift.TTransport, error) {
	listener := p.listener
	if listener == nil {
		return nil, thrift.NewTTransportException(thrift.NOT_OPEN, "No underlying server socket")
	}

	conn, err := listener.Accept()
	if err != nil {
		return nil, thrift.NewTTransportExceptionFromError(err)
	}
	return thrift.NewTSocketFromConnTimeout(conn, p.clientTimeout), nil
}

func processSession(c thrift.TTransport, processor thrift.TProcessor) {
	// 1. 打印异常信息
	defer func() {
		c.Close()
		if e := recover(); e != nil {
			log.Errorf("panic in processor: %v: %s", e, debug.Stack())
		}
	}()

	// 2. 注意协议类型: TBinaryProtocol & TFramedTransport
	ip := thrift.NewTBinaryProtocolTransport(thrift.NewTFramedTransport(c))
	op := thrift.NewTBinaryProtocolTransport(thrift.NewTFramedTransport(c))

	for {
		defaultContext := context.Background()
		// 3. 循环处理每一个请求
		ok, err := processor.Process(defaultContext, ip, op)

		// 总结:
		// 1. 返回false, 表示io过程出现异常，必须断开连接; err总是存在
		// 2. 返回true, 表示io没有异常，可以继续使用connection; 返回未知的异常，或为0
		//
		if ok {
			if err != nil {
				log.ErrorErrorf(err, "Unexpected exception found from service")
			}
		} else {
			// ok == false, 如果链路出现异常（必须结束)
			if err, ok := err.(thrift.TTransportException); ok && (err.TypeId() == thrift.END_OF_FILE ||
				strings.Contains(err.Error(), "use of closed network connection")) {
				// 正常的断开，不打日志
				return
			} else if err != nil {
				// 其他链路问题，直接报错，退出
				log.ErrorErrorf(err, "Error processing request, quit")
				return
			}

			// 其他错误
			log.ErrorErrorf(err, "Error processing request")
			return
		}
	}
}

func GracefulRunWithListener(listener net.Listener, processor thrift.TProcessor) {
	// 将listener包装成为transport
	transport := NewTGracefulServerSocket(listener)
	ch := make(chan thrift.TTransport, 4096)
	defer close(ch)

	go func() {
		for c := range ch {
			// c必须通过参数传递给go func, 不能在内部直接访问(否则异步请求看到的会有问题)
			go processSession(c, processor)
		}
	}()

	// Accept什么时候出错，出错之后如何处理呢?
	for {
		c, err := transport.Accept()
		if err != nil {
			break
		} else {
			ch <- c
		}
	}
}
