// +build !rawSocket

package MoldUDP

import (
	"net"
	"runtime"
	"sync/atomic"
	"time"
)

const (
	maxUDPsize   = 1472
	heartBeatInt = 2
	maxGoes      = 1000
	PPms         = 100 // packets per ms
)

// Server struct for MoldUDP server
//	Running		bool
//	Session		session for all messages
type Server struct {
	Session       string
	dst           net.UDPAddr
	conn          *net.UDPConn
	PPms          int
	Running       bool
	endSession    bool
	seqNo         uint64
	endTime       int64
	waits         int // wait for 5 seconds end of session
	nRecvs, nSent int
	nError        int
	nResent       int64
	nGoes         int32
	nMaxGoes      int
	nHeartBB      int
	nSleep        int
	msgs          []Message
	buff          []byte
}

func (c *Server) Close() error {
	if c.conn == nil {
		return errClosed
	}
	err := c.conn.Close()
	c.conn = nil
	return err
}

func (c *Server) EndSession(nWaits int) {
	c.endSession = true
	if nWaits > c.waits {
		c.waits = nWaits
	}
}

func (c *Server) FeedMessages(feeds []Message) {
	c.msgs = append(c.msgs, feeds...)
}

func NewServer(udpAddr string, port int, ifName string, bLoop bool) (*Server, error) {
	var err error
	server := Server{seqNo: 1, waits: 5, PPms: PPms}
	// sequence number is 1 based
	server.dst.IP = net.ParseIP(udpAddr)
	server.dst.Port = port
	if !server.dst.IP.IsMulticast() {
		log.Info(server.dst.IP, " is not multicast IP")
		server.dst.IP = net.IPv4(224, 0, 0, 1)
	}
	var ifn *net.Interface
	if ifName != "" {
		if ifn, err = net.InterfaceByName(ifName); err != nil {
			log.Errorf("Ifn(%s) error: %v\n", ifName, err)
			ifn = nil
		}
	}
	var fd int = -1
	laddr := net.UDPAddr{IP: net.IPv4(0, 0, 0, 0), Port: port}
	server.conn, err = net.ListenUDP("udp", &laddr)
	if err != nil {
		return nil, err
	}
	if ff, err := server.conn.File(); err == nil {
		fd = int(ff.Fd())
	} else {
		log.Error("Get UDPConn fd", err)
	}
	/*
		if err := JoinMulticast(fd, server.dst.IP, ifn); err != nil {
			log.Info("add multicast group", err)
		}
	*/
	if err := SetMulticastInterface(fd, ifn); err != nil {
		log.Info("set multicast interface", err)
	}
	if bLoop {
		if err := SetMulticastLoop(fd, true); err != nil {
			log.Info("set multicast loopback", err)
		}
	}
	server.buff = make([]byte, 2048)
	server.Running = true
	return &server, nil
}

type hostControl struct {
	remote   net.UDPAddr
	seqAcked uint64
	seqNext  uint64 // max nak sequence
	running  int32
}

// RequestLoop		go routine process request retrans
func (c *Server) RequestLoop() {
	hostMap := map[string]*hostControl{}
	var nResends int

	doReq := func(hc *hostControl) {
		//seqNo uint64, cnt uint16, remoteA net.UDPAddr
		// only retrans one UDP packet
		// proce reTrans
		var buff [maxUDPsize]byte
		seqNo := hc.seqAcked

		defer atomic.AddInt32(&c.nGoes, -1)
		nResends++
		if nResends%10 == 0 {
			log.Infof("Resend packets to %s Seq: %d -- %d", hc.remote.IP,
				seqNo, hc.seqNext)
		}
		sHead := Header{Session: c.Session, SeqNo: seqNo}
		i := 0
		for seqNo < atomic.LoadUint64(&hc.seqNext) {
			lastS := int(atomic.LoadUint64(&hc.seqNext))
			msgCnt, bLen := Marshal(buff[headSize:], c.msgs[seqNo-1:lastS-1])
			sHead.MessageCnt = uint16(msgCnt)
			if err := EncodeHead(buff[:headSize], &sHead); err != nil {
				log.Error("EncodeHead for proccess reTrans", err)
				continue
			}
			atomic.AddInt64(&c.nResent, 1)
			if _, err := c.conn.WriteToUDP(buff[:headSize+bLen], &hc.remote); err != nil {
				log.Error("Res WriteToUDP", hc.remote, err)
				break
			}
			i++
			if i >= 10 {
				i = 0
				time.Sleep(time.Millisecond)
			}
			seqNo += uint64(msgCnt)
			sHead.SeqNo = seqNo
		}
		atomic.StoreInt32(&hc.running, 0)
	}
	for c.Running {
		n, remoteAddr, err := c.conn.ReadFromUDP(c.buff)
		if err != nil {
			log.Error("ReadFromUDP from", remoteAddr, " ", err)
			continue
		}
		c.nRecvs++
		//c.LastRecv = time.Now().Unix()
		if n != headSize {
			c.nError++
			continue
		}
		var head Header
		if err := DecodeHead(c.buff[:n], &head); err != nil {
			log.Error("DecodeHead from", remoteAddr, " ", err)
			c.nError++
			continue
		}
		if head.SeqNo >= c.seqNo {
			log.Errorf("Invalid seq %d, server seqNo: %d", head.SeqNo, c.seqNo)
			c.nError++
			continue
		}
		if nMsg := head.MessageCnt; nMsg == 0xffff || nMsg == 0 {
			log.Errorf("Seems msg from server MessageCnt(%d) from %s", nMsg, remoteAddr)
			c.nError++
			continue
		}
		rAddr := remoteAddr.IP.String()
		var hc *hostControl
		seqNext := head.SeqNo + uint64(head.MessageCnt)
		if hh, ok := hostMap[rAddr]; ok {
			hc = hh
			if atomic.LoadInt32(&hc.running) == 0 {
				hc.seqAcked = head.SeqNo
			}
			log.Info(rAddr, "in process retrans for", hc.seqAcked, hc.seqNext)
		} else {
			hc = &hostControl{seqAcked: head.SeqNo, remote: *remoteAddr}
			hc.running = 1
			hostMap[rAddr] = hc
		}
		if atomic.LoadUint64(&hc.seqNext) < seqNext {
			atomic.StoreUint64(&hc.seqNext, seqNext)
		}

		if atomic.LoadInt32(&hc.running) == 0 {
			if atomic.LoadInt32(&c.nGoes) > maxGoes {
				continue
			}
			nGoes := int(atomic.AddInt32(&c.nGoes, 1))
			if nGoes > c.nMaxGoes {
				c.nMaxGoes = nGoes
			}
			atomic.StoreInt32(&hc.running, 1)
			go doReq(hc)
		}

	}
}

// ServerLoop	go routine multicast UDP and heartbeat
func (c *Server) ServerLoop() {
	var buff [maxUDPsize]byte
	head := Header{Session: c.Session}
	lastSend := time.Now()
	hbInterval := time.Second * heartBeatInt
	mcastBuff := func(bLen int) {
		if err := EncodeHead(buff[:headSize], &head); err != nil {
			log.Error("EncodeHead for proccess mcast", err)
		} else {
			if _, err := c.conn.WriteToUDP(buff[:headSize+bLen], &c.dst); err != nil {
				log.Error("mcast send", err)
			}
			lastSend = time.Now()
			c.nSent++
		}
	}
	for c.Running {
		st := time.Now()
		seqNo := int(c.seqNo)
		if seqNo > len(c.msgs) {
			// check for heartbeat sent
			if st.Sub(lastSend) >= hbInterval {
				head.SeqNo = c.seqNo
				head.MessageCnt = 0
				c.nHeartBB++
				mcastBuff(0)
			}
			if c.endTime != 0 {
				if c.endTime < time.Now().Unix() {
					c.Running = false
					break
				}
			} else if c.endSession {
				c.endTime = time.Now().Unix()
				c.endTime += int64(c.waits)
				// send End of Session packet
				head.SeqNo = c.seqNo
				head.MessageCnt = 0xffff
				mcastBuff(0)
			}
			runtime.Gosched()
			continue
		}
		for i := 0; i < c.PPms; i++ {
			if seqNo > len(c.msgs) {
				break
			}
			msgCnt, bLen := Marshal(buff[headSize:], c.msgs[seqNo-1:])
			if msgCnt == 0 {
				break
			}
			head.SeqNo = uint64(seqNo)
			head.MessageCnt = uint16(msgCnt)
			mcastBuff(bLen)
			seqNo += msgCnt
		}
		c.seqNo = uint64(seqNo)
		dur := time.Now().Sub(st)
		// sleep to 1 ms
		if dur < time.Microsecond*900 {
			c.nSleep++
			time.Sleep(time.Millisecond - dur)
		}
	}
}

func (c *Server) SeqNo() int {
	return int(c.seqNo)
}

func (c *Server) DumpStats() {
	log.Infof("Total Sent: %d HeartBeat: %d seqNo: %d, sleep: %d\n"+
		"Recv: %d, errors: %d, reSent: %d, maxGoes: %d",
		c.nSent, c.nHeartBB, c.seqNo, c.nSleep, c.nRecvs, c.nError,
		c.nResent, c.nMaxGoes)
}
