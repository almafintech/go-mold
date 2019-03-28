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

// RequestLoop		go routine process request retrans
func (c *Server) RequestLoop() {
	//seqNoHost := map[string]uint64{}
	doReq := func(seqNo uint64, cnt uint16, remoteA net.UDPAddr) {
		// only retrans one UDP packet
		// proce reTrans
		var buff [maxUDPsize]byte

		firstS := int(seqNo) - 1
		lastS := firstS + int(cnt)
		defer atomic.AddInt32(&c.nGoes, -1)
		/*
			rAddr := remoteA.IP.String()
					if savedSeq, ok := seqNoHost[rAddr]; ok {
						log.Info(rAddr, "already in process retrans for", savedSeq)
						return
					}
					seqNoHost[rAddr] = seqNo
				defer delete(seqNoHost, rAddr)
		*/

		log.Infof("Resend packets to %s Seq: %d -- %d", remoteA.IP, firstS+1, lastS+1)
		sHead := Header{Session: c.Session, SeqNo: seqNo}
		for firstS < lastS {
			msgCnt, bLen := Marshal(buff[headSize:], c.msgs[firstS:lastS])
			sHead.MessageCnt = uint16(msgCnt)
			if err := EncodeHead(buff[:headSize], &sHead); err != nil {
				log.Error("EncodeHead for proccess reTrans", err)
				continue
			}
			atomic.AddInt64(&c.nResent, 1)
			if _, err := c.conn.WriteToUDP(buff[:headSize+bLen], &remoteA); err != nil {
				log.Error("Res WriteToUDP", remoteA, err)
				break
			}
			firstS += msgCnt
			sHead.SeqNo += uint64(msgCnt)
		}
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
		if atomic.LoadInt32(&c.nGoes) > maxGoes {
			continue
		}
		nGoes := int(atomic.AddInt32(&c.nGoes, 1))
		if nGoes > c.nMaxGoes {
			c.nMaxGoes = nGoes
		}
		go doReq(head.SeqNo, head.MessageCnt, *remoteAddr)
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
