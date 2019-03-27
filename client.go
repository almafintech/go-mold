// +build !rawSocket

package MoldUDP

import (
	"net"
	"strings"
	"time"

	"github.com/kjx98/golib/to"
)

// Client struct for MoldUDP client
//	Running		bool
//	LastRecv	int64	last time recv UDP
type Client struct {
	dst    net.UDPAddr
	conn   *net.UDPConn
	reqSrv []*net.UDPAddr
	ClientBase
}

func (c *Client) Close() error {
	if c.conn == nil {
		return errClosed
	}
	err := c.conn.Close()
	c.conn = nil
	return err
}

func NewClient(udpAddr string, port int, opt *Option) (*Client, error) {
	var err error
	client := Client{ClientBase: ClientBase{seqNo: opt.NextSeq}}
	ifn := client.ClientBase.initClientBase(opt)
	client.dst.IP = net.ParseIP(udpAddr)
	client.dst.Port = port
	if !client.dst.IP.IsMulticast() {
		log.Info(client.dst.IP, " is not multicast IP")
		client.dst.IP = net.IPv4(224, 0, 0, 1)
	}
	var fd int = -1
	//client.conn, err = net.ListenMulticastUDP("udp", ifn, &client.dst)
	laddr := net.UDPAddr{IP: net.IPv4(0, 0, 0, 0), Port: port}
	client.conn, err = net.ListenUDP("udp", &laddr)
	if err != nil {
		return nil, err
	}
	if ff, err := client.conn.File(); err == nil {
		fd = int(ff.Fd())
	} else {
		log.Error("Get UDPConn fd", err)
	}
	if err := JoinMulticast(fd, client.dst.IP, ifn); err != nil {
		log.Info("add multicast group", err)
	}
	for _, daddr := range opt.Srvs {
		ss := strings.Split(daddr, ":")
		udpA := net.UDPAddr{IP: net.ParseIP(ss[0])}
		if len(ss) == 1 {
			udpA.Port = port
		} else {
			udpA.Port = to.Int(ss[1])
		}
		client.reqSrv = append(client.reqSrv, &udpA)
	}
	client.Running = true
	client.LastRecv = time.Now().Unix()
	return &client, nil
}

// Read			Get []Message in order
//	[]Message	messages received in order
//	return   	nil,nil   for end of session or finished
func (c *Client) Read() ([]Message, error) {
	for c.Running {
		n, remoteAddr, err := c.conn.ReadFromUDP(c.buff)
		if err != nil {
			log.Error("ReadFromUDP from", remoteAddr, " ", err)
			return nil, err
		}
		if res, req, err := c.ClientBase.gotBuff(n); err != nil {
			log.Error("Packet from", remoteAddr, " error:", err)
			return nil, err
		} else {
			if len(c.reqSrv) == 0 {
				c.reqSrv = append(c.reqSrv, remoteAddr)
			}
			if req != nil {
				// need send Request
				c.request(req)
			}
			if res != nil {
				return res, nil
			}
		}
	}
	return nil, nil
}

func (c *Client) request(buff []byte) {
	if len(c.reqSrv) == 0 {
		return
	}
	c.nRequest++
	if c.nRequest < 5 {
		log.Info("Send reTrans seq:", c.seqNo, " req to", c.reqSrv[c.robinN])
	}
	if _, err := c.conn.WriteToUDP(buff[:], c.reqSrv[c.robinN]); err != nil {
		log.Error("Req reTrans", err)
	}
	c.robinN++
	if c.robinN >= len(c.reqSrv) {
		c.robinN = 0
	}
}
