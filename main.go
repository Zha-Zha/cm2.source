package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

var (
	l         string
	p         string
	gListener *net.TCPListener
	runState  int32
)

func usage() {
	_, _ = fmt.Fprintf(os.Stderr, "./main [-l :8081] -p hello \n")
	flag.PrintDefaults()
}

func init() {
	flag.StringVar(&l, "l", "", "listen")
	flag.StringVar(&p, "p", "", "prefix")
	flag.Usage = usage
}

func checkUsage() bool {
	if p == "" {
		return false
	}
	return true
}

func main() {
	flag.Parse()
	if !checkUsage() {
		flag.Usage()
		return
	}

	var (
		err error
		ud  net.Conn
	)
	// 平滑关闭
	wg := &sync.WaitGroup{}
	stopChan := make(chan bool)
	transChan := make(chan bool)
	sig := make(chan os.Signal)
	signal.Notify(sig, syscall.SIGINT)

	// 如果是继承过来的，那么等待旧进程传输旧的 listener 过来
	if IsInherit() {
		gListener, ud, err = GetInheritListeners(time.Duration(15) * time.Second)
		if err != nil {
			log.Printf("net listen inheritlistener failed %s\n", err)
			return
		}
		log.Printf("get inherit listener sucessfully")
		wg.Add(1)
		go GetInheritConnections(time.Duration(15)*time.Second, wg, stopChan, transChan)
	} else {
		li, err := net.Listen("tcp", l)
		if err != nil {
			log.Printf("net listen failed %s\n", l)
			return
		}
		log.Printf("net listen %s\n", l)
		gListener = li.(*net.TCPListener)
	}

	// 通知旧进程上一次继承开关通知已经结束，通知完成之后，才能关闭前一个listener
	if ud != nil {
		_, _ = ud.Write([]byte{0})
	}

	wg.Add(1)
	go Server(gListener, wg, stopChan, transChan)

	// 监听下一次的继承事件，需要等待此次继承事件完全完成；进程关闭前，等待5s看是否有新进程过来获取数据
	wg.Add(1)
	go ListenNextInherit(wg, stopChan, transChan)

	// 如果需要平滑将 数据/listener/connections 传递出去，那么就稍微等待一下
	select {
	case <-transChan:
		log.Printf("master gorotinue wait for 15s")
		<-time.After(15 * time.Second)
	case <-sig:
	}

	_ = gListener.Close()
	close(stopChan)
	wg.Wait()
}
