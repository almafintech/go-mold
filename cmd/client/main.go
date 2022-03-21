package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	ats "github.com/kjx98/go-ats"
	MoldUDP "github.com/kjx98/go-mold"
	logging "github.com/op/go-logging"
)

var log = logging.MustGetLogger("mold-client")

var opt MoldUDP.Option

var coder = binary.BigEndian

type contents struct {
	trackingN uint16
	timeStamp uint64
	stock string
	secClass string
	bidPrice uint32
	bidSize uint32
	askPrice uint32
	askSize uint32
}


func main() {
	var maddr string
	var port int
	var waits int
	var netMode string
	var firstTic, lastTic *ats.TickFX
	var fTic, lTic ats.TickFX

	flag.StringVar(&maddr, "m", "239.192.168.1", "Multicast IPv4 to listen")
	flag.StringVar(&opt.IfName, "i", "", "Interface name for multicast")
	flag.IntVar(&port, "p", 5858, "UDP port to listen")
	flag.IntVar(&waits, "w", 30, "seconds wait for UDP packet, 0 unlimited")
	flag.StringVar(&netMode, "net", "net", "Multicast Recv network interface, net/sock/zsock")
	var reqServ string
	flag.StringVar(&reqServ, "req", "", "Multicast Req address:port")
	opt.Srvs = []string{reqServ}
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: client [options]\n")
		flag.PrintDefaults()
		os.Exit(2)
	}
	flag.Parse()
	netif := MoldUDP.NewIf(netMode)
	log.Info("Client listen", maddr, "via", netif)
	cc, err := MoldUDP.NewClient(maddr, port, &opt, netif)
	if err != nil {
		log.Error("NewClient", err)
		os.Exit(1)
	}
	defer cc.Close()
	// catch  SIGTERM, SIGINT, SIGUP
	sigC := make(chan os.Signal, 10)
	signal.Notify(sigC)
	go func() {
		for s := range sigC {
			switch s {
			case os.Kill, os.Interrupt, syscall.SIGTERM:
				log.Info("退出", s)
				cc.Running = false
				ExitFunc()
			case syscall.SIGQUIT:
				log.Info("Quit", s)
				cc.Running = false
				ExitFunc()
			default:
				//log.Info("Got signal", s)
			}
		}
	}()

	cc.Running = true
	lastSeq := uint64(0)
	go func() {
		for cc.Running {
			mess, lastS, err := cc.Read()
			log.Infof("Got %d messages", len(mess))
			if err != nil {
				log.Error("Client Read", err)
				continue
			}
			if mess == nil {
				break
			}
			if len(mess) == 0 {
				continue
			}
			lastSeq = lastS
			if firstTic == nil {
				log.Infof("Got first %d messages", len(mess))
				firstTic = ats.Bytes2TickFX(mess[0].Data)
				if firstTic != nil {
					fTic = *firstTic
				}
			}
			if n := len(mess); n > 0 {
				lastTic = ats.Bytes2TickFX(mess[n-1].Data)
				if lastTic != nil {
					lTic = *lastTic
				} else {
					log.Errorf("last %d /%d Data is null", n, cc.SeqNo())
				}
			}

			var j int
			for j = 0; j < len(mess); j++ {
				msg := mess[j]
				msgType := msg.Data[0:1]
				if string(msgType) == "Q" {
					if len(msg.Data) != 34 {
						log.Fatal("Wrong message length!")
					}

					var timestamp uint64
					factor := 1
					for k := 8; k >= 3; k-- {
						timestamp = uint64(int(msg.Data[k])*factor)
						factor *= 256
					}
					cnts := contents{
						trackingN : coder.Uint16(msg.Data[1:3]),
						timeStamp : timestamp,
						stock : string(msg.Data[9:17]),
						secClass : string(msg.Data[17:18]),
						bidPrice : coder.Uint32(msg.Data[18:22]),
						bidSize : coder.Uint32(msg.Data[22:26]),
						askPrice : coder.Uint32(msg.Data[26:30]),
						askSize : coder.Uint32(msg.Data[30:34]),
					}
					log.Infof("  Message: %d \n    Contents: %+v \n ", j, cnts)
				}
			}
		}
		// should we stop?
		cc.Running = false
	}()
	tick := time.NewTicker(time.Second)
	if cc.LastRecv == 0 {
		cc.LastRecv = time.Now().Unix()
	}
	nextDisp := int64(0)
	for cc.Running {
		var tt int64
		select {
		case <-tick.C:
			tt = time.Now().Unix()
			if waits > 0 && cc.LastRecv+int64(waits) < tt {
				cc.Running = false
				log.Errorf("No UDP recv for %d seconds", waits)
			}
		}
		if nextDisp == 0 {
			if firstTic != nil {
				log.Info("First message:", fTic)
				nextDisp = tt + 30
			}
		} else if nextDisp < tt {
			cc.DumpStats()
			nextDisp = tt + 30
		}
	}
	if lastTic != nil {
		log.Info("Last message:", lTic)
	}
	lastSeqN, lastN := cc.LastSeq()
	log.Infof("Last Block seqNo: %d/%d number: %d", lastSeq, lastSeqN, lastN)
	cc.DumpStats()
	log.Info("exit client")
	//os.Exit(0);
}

func ExitFunc() {
	//beeep.Alert("orSync Quit!", "try to exit", "")
	log.Warning("开始退出...")
	log.Warning("执行退出...")
	log.Warning("结束退出...")
	// wait 5 seconds
	time.Sleep(time.Second * 5)
	os.Exit(1)
}

/*
//  `%{color}%{time:15:04:05.000} %{shortfunc} ▶ %{level:.4s} %{id:03x}%{color:reset} %{message}`,
func init() {
	var format = logging.MustStringFormatter(
		`%{color}%{time:01-02 15:04:05}  ▶ %{level:.4s} %{color:reset} %{message}`,
	)

	logback := logging.NewLogBackend(os.Stderr, "", 0)
	logfmt := logging.NewBackendFormatter(logback, format)
	logging.SetBackend(logfmt)
}
*/
