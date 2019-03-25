// +build !rawSocket

package MoldUDP

import (
	"io"
	"net"
	"strings"
	"time"

	"github.com/kjx98/golib/to"
)

const (
	reqInterval = 5
	maxMessages = 1024
)

type Client struct {
	dst                      net.UDPAddr
	conn                     *net.UDPConn
	Running                  bool
	seqNo                    uint64
	reqLast                  int64
	nRecvs, nError, nRequest int
	robinN                   int
	reqSrv                   []*net.UDPAddr
	session                  string
	buff                     []byte
}

type Option struct {
	Srvs    []string
	IfName  string
	nextSeq uint64
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
	client := Client{seqNo: opt.nextSeq}
	client.dst.IP = net.ParseIP(udpAddr)
	client.dst.Port = port
	if !client.dst.IP.IsMulticast() {
		log.Info(client.dst.IP, " is not multicast IP")
		client.dst.IP = net.IPv4(224, 0, 0, 1)
	}
	var ifn *net.Interface
	if opt.IfName != "" {
		if ifn, err = net.InterfaceByName(opt.IfName); err != nil {
			log.Errorf("Ifn(%s) error: %v\n", opt.IfName, err)
			ifn = nil
		}
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
	if err := UDPMulticast(fd, client.dst.IP, ifn); err != nil {
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
	client.buff = make([]byte, 2048)
	return &client, nil
}

func (c *Client) Read() ([]Message, error) {
	for c.Running {
		n, remoteAddr, err := c.conn.ReadFromUDP(c.buff)
		if err != nil {
			log.Error("ReadFromUDP from", remoteAddr, " ", err)
			continue
		}
		c.nRecvs++
		head, err := DecodeHead(c.buff[:n])
		if err != nil {
			log.Error("DecodeHead from", remoteAddr, " ", err)
			c.nError++
			continue
		}
		if nMsg := head.MessageCnt; nMsg != 0xffff && nMsg >= maxMessages {
			log.Errorf("Invalid MessageCnt(%d) from %s", nMsg, remoteAddr)
			c.nError++
			continue
		}
		if len(c.reqSrv) == 0 {
			c.reqSrv = append(c.reqSrv, remoteAddr)
		}
		if c.session == "" {
			c.session = head.Session
		} else if c.session != head.Session {
			log.Errorf("Session dismatch %s got %s", c.session, head.Session)
			c.nError++
			continue
		}
		if head.SeqNo != c.seqNo {
			// should request for retransmit
			c.request(head.SeqNo)
		}
		switch head.MessageCnt {
		case 0xffff:
			return nil, io.EOF
		case 0:
			// got heartbeat
			continue
		}
		if head.SeqNo != c.seqNo {
			// cache or not
			continue
		}
		if headSize == n {
			c.nError++
			return nil, errMessageCnt
		}
		ret, err := Unmarshal(c.buff[headSize:n])
		if err != nil {
			c.nError++
			return nil, err
		}
		c.seqNo += uint64(len(ret))
		return ret, nil
	}
	return nil, nil
}

func (c *Client) request(seqNo uint64) {
	if len(c.reqSrv) == 0 {
		return
	}
	tt := time.Now().Unix()
	if c.reqLast+reqInterval > tt {
		return
	}
	c.reqLast = tt
	cnt := seqNo - c.seqNo
	if cnt > 60000 {
		cnt = 60000
	}
	head := Header{Session: c.session, SeqNo: c.seqNo}
	head.MessageCnt = uint16(cnt)
	buff := make([]byte, headSize)
	if err := EncodeHead(buff, &head); err != nil {
		log.Error("EncodeHead for Req reTrans", err)
		return
	}
	c.nRequest++
	if _, err := c.conn.WriteToUDP(buff, c.reqSrv[c.robinN]); err != nil {
		log.Error("Req reTrans", err)
	}
	c.robinN++
	if c.robinN >= len(c.reqSrv) {
		c.robinN = 0
	}
}

func (c *Client) DumpStats() {
	log.Infof("Total Recv: %d, errors: %d, sent Request: %d",
		c.nRecvs, c.nError, c.nRequest)
}
