package rpc_utils

import (

	"github.com/wfxiang08/go_thrift/thrift"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

//
// go test rpc_utils -v -run "TestSmsRpc"
//
func TestSmsRpc(t *testing.T) {

	var ThriftPool *Pool

	//	proxyAddress := "/usr/local/rpc_proxy/proxy.sock"

	proxyAddress := "60.29.255.199:5550" // 60.29.255.199
	//	proxyAddress := "/usr/local/rpc_proxy/online_proxy.sock"

	ThriftPool = &Pool{
		Dial: func() (thrift.TTransport, error) {
			// 如何创建一个Transport
			trans, err := NewTFramedTransportWithTimeout(proxyAddress, time.Second*5)
			return trans, err
		},
		MaxActive:   30,
		MaxIdle:     30,
		IdleTimeout: time.Second * 3600 * 24,
		Wait:        true,
	}

	transport := ThriftPool.Get()
	defer transport.Close()

//	ip, op := GetProtocolFromTransport(transport, "sms")
//	client := chunyu_sms_service.NewSmsServiceClientProtocol(transport, ip, op)
//
//
//	r, err := client.AddSendSmsRequest("18611730934", "测试信息")
//	fmt.Printf("R: %v\n", r)
//	fmt.Printf("R: %v\n", err)
//	fmt.Printf("Response: %d, %s --> %v \n", r.Code, r.ErrorMsg, err)

	assert.True(t, true)
}
