package memcache

import (
	"net"
)

type Conn struct {
	addr string
	conn net.Conn
}

func NewConn(address string) (conn *Conn, err error) {
	c, err := net.Dial("tcp", address)
	if err != nil {
		return
	}
	conn = &Conn{
		addr: address,
		conn: c,
	}
	return
}

func (c *Conn) connect() (err error) {
	if c.conn != nil {
		return
	}
	conn, err := net.Dial("tcp", c.addr)
	if err != nil {
		return
	}
	c.conn = conn
	return
}

func (c *Conn) reconnect() error {
	if c.conn != nil {
		c.conn.Close()
		c.conn = nil
	}
	return c.connect()
}

func (c *Conn) Write(b []byte) (n int, err error) {
	n, err = c.conn.Write(b)
	if err != nil {
		if err = c.reconnect(); err != nil {
			return
		}
		n, err = c.conn.Write(b)
		return
	}
	return
}

func (c *Conn) Read(p []byte) (n int, err error) {
	n, err = c.conn.Read(p)
	if err != nil {
		if err = c.reconnect(); err != nil {
			return
		}
		n, err = c.conn.Read(p)
		return
	}
	return
}
