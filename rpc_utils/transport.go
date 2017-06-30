package rpc_utils

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/wfxiang08/go_thrift/thrift"
	"io"
	"strings"
	"time"
)

const DEFAULT_MAX_LENGTH = 16384000

//
// 具有自动重连的 FramedTransport
//
type TFramedTransport struct {
	transport thrift.TTransport
	buf       bytes.Buffer
	reader    *bufio.Reader
	frameSize int //Current remaining size of the frame. if ==0 read next frame header
	buffer    [4]byte
	maxLength int
}

var _ thrift.TTransport = &TFramedTransport{}

//
// 创建一个TFramedTranport, 支持: tcp socket，unix domain socket
// 1. tcp socket的格式: 60.29.255.199:5550
// 2. unix domain的格式: /usr/local/rpc_proxy/proxy.sock
//
func NewTFramedTransportWithTimeout(addr string, timeout time.Duration) (transport thrift.TTransport, err error) {
	if strings.Contains(addr, ":") {
		transport, err = thrift.NewTSocketTimeout(addr, timeout)
	} else {
		transport, err = NewTUnixDomainTimeout(addr, timeout)
	}
	return
}

func NewTFramedTransport(transport thrift.TTransport) *TFramedTransport {
	return &TFramedTransport{transport: transport, reader: bufio.NewReader(transport), maxLength: DEFAULT_MAX_LENGTH}
}

func NewTFramedTransportMaxLength(transport thrift.TTransport, maxLength int) *TFramedTransport {
	return &TFramedTransport{transport: transport, reader: bufio.NewReader(transport), maxLength: maxLength}
}

func (p *TFramedTransport) Open() error {
	return p.transport.Open()
}

func (p *TFramedTransport) IsOpen() bool {
	return p.transport.IsOpen()
}

func (p *TFramedTransport) Close() error {
	return p.transport.Close()
}

func (p *TFramedTransport) Read(buf []byte) (l int, err error) {
	// 确保Transport是打开的
	if !p.IsOpen() {
		err = p.Open()
		if err != nil {
			return 0, err
		}
	}

	if p.frameSize == 0 {
		p.frameSize, err = p.readFrameHeader()
		if err != nil {
			p.Close()
			return
		}
	}
	if p.frameSize < len(buf) {
		frameSize := p.frameSize
		tmp := make([]byte, p.frameSize)
		l, err = p.Read(tmp)
		copy(buf, tmp)
		if err == nil {
			p.Close()
			err = thrift.NewTTransportExceptionFromError(fmt.Errorf("Not enough frame size %d to read %d bytes", frameSize, len(buf)))
			return
		}
	}
	got, err := p.reader.Read(buf)
	p.frameSize = p.frameSize - got
	//sanity check
	if p.frameSize < 0 {
		p.Close()
		return 0, thrift.NewTTransportException(thrift.UNKNOWN_TRANSPORT_EXCEPTION, "Negative frame size")
	}
	if err != nil {
		p.Close()
	}
	return got, thrift.NewTTransportExceptionFromError(err)
}

func (p *TFramedTransport) ReadByte() (c byte, err error) {
	// 确保Transport是打开的
	if !p.IsOpen() {
		err = p.Open()
		if err != nil {
			return 0, err
		}
	}

	if p.frameSize == 0 {
		p.frameSize, err = p.readFrameHeader()
		if err != nil {
			p.Close()
			return
		}
	}
	if p.frameSize < 1 {
		p.Close()
		return 0, thrift.NewTTransportExceptionFromError(fmt.Errorf("Not enough frame size %d to read %d bytes", p.frameSize, 1))
	}
	c, err = p.reader.ReadByte()
	if err == nil {
		p.frameSize--
	} else {
		p.Close()
	}
	return
}

func (p *TFramedTransport) Write(buf []byte) (int, error) {
	// 确保Transport是打开的
	if !p.IsOpen() {
		err := p.Open()
		if err != nil {
			return 0, err
		}
	}

	n, err := p.buf.Write(buf)
	if err != nil {
		p.Close()
	}
	return n, thrift.NewTTransportExceptionFromError(err)
}

func (p *TFramedTransport) WriteByte(c byte) error {
	// 确保Transport是打开的
	if !p.IsOpen() {
		err := p.Open()
		if err != nil {
			return err
		}
	}
	err := p.buf.WriteByte(c)
	if err != nil {
		p.Close()
	}
	return err
}

func (p *TFramedTransport) WriteString(s string) (n int, err error) {
	// 确保Transport是打开的
	if !p.IsOpen() {
		err = p.Open()
		if err != nil {
			return 0, err
		}
	}

	n, err = p.buf.WriteString(s)
	if err != nil {
		p.Close()
	}
	return n, err
}

func (p *TFramedTransport) Flush() error {

	// 确保Transport是打开的
	if !p.IsOpen() {
		err := p.Open()
		if err != nil {
			return err
		}
	}

	size := p.buf.Len()
	buf := p.buffer[:4]
	binary.BigEndian.PutUint32(buf, uint32(size))
	_, err := p.transport.Write(buf)
	if err != nil {
		p.Close()
		return thrift.NewTTransportExceptionFromError(err)
	}
	if size > 0 {
		if n, err := p.buf.WriteTo(p.transport); err != nil {
			print("Error while flushing write buffer of size ", size, " to transport, only wrote ", n, " bytes: ", err.Error(), "\n")

			p.Close()
			return thrift.NewTTransportExceptionFromError(err)
		}
	}
	err = p.transport.Flush()
	if err != nil {
		p.Close()
	}
	return thrift.NewTTransportExceptionFromError(err)
}

func (p *TFramedTransport) readFrameHeader() (int, error) {
	buf := p.buffer[:4]
	if _, err := io.ReadFull(p.reader, buf); err != nil {
		return 0, err
	}
	size := int(binary.BigEndian.Uint32(buf))
	if size < 0 || size > p.maxLength {
		return 0, thrift.NewTTransportException(thrift.UNKNOWN_TRANSPORT_EXCEPTION, fmt.Sprintf("Incorrect frame size (%d)", size))
	}
	return size, nil
}

func (p *TFramedTransport) RemainingBytes() (num_bytes uint64) {
	return uint64(p.frameSize)
}
