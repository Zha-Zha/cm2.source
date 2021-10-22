package main

import (
	"bufio"
	"bytes"
	"io"
	"log"
	"net"
	"sync"
	"sync/atomic"
)

// 不能使用 io.Reader/bufio.NewReader/bytes.NewBytes，因为都不能进行暂停
func HandleReply(wg *sync.WaitGroup, dataBuf []byte, conn net.Conn, stopChan chan bool, transChan chan bool) {
	defer wg.Done()
	tcp := conn.(*net.TCPConn)
	// !!!! 这个很重要，否则新进程无法保证存活，导致read eof
	_ = tcp.SetKeepAlive(true)

	r := bufio.NewReader(io.MultiReader(bytes.NewBuffer(dataBuf), tcp))
	w := bufio.NewWriter(conn)

	var (
		line []byte
		err  error
	)

	if len(dataBuf) > 0 {
		log.Printf("handle reply from old process for conn %s", conn.RemoteAddr().String())
	}

	waitReadStop := make(chan bool)
	go func() {
		for {
			line, err = r.ReadSlice(Separator)
			if err != nil {
				if err == io.EOF {
					log.Printf("disconnect from client eof\n")
				} else if ope, ok := err.(*net.OpError); ok && ope.Op == "read" {
					log.Printf("disconnect from client, can't read\n")
				} else {
					log.Printf("tcp net read err %s\n", err)
				}
				if 1 == atomic.LoadInt32(&runState) {
					close(waitReadStop)
				}
				return
			}
			line = bytes.Join(bytes.Split(line, []byte{'\n'}), []byte{})
			line = bytes.Join(bytes.Split(line, []byte{'\r'}), []byte{})
			_, _ = w.Write(append(append(append(append([]byte(p), []byte(" reply : ")...), line...)), '\n'))
			line = nil
			_ = w.Flush()
		}
	}()

	// 控制开关
	select {
	case <-stopChan:
		_ = conn.Close()
		return
	case <-transChan:
		uc, err := connTransferSendSock()
		if err != nil {
			log.Printf("conn transfer send sock, %s", err)
			return
		}
		defer uc.Close()
		log.Printf("transChan l,r %s %s", uc.LocalAddr(), uc.RemoteAddr())
		connId, err := transferSendReadConnFd(uc, conn)
		if err != nil {
			log.Printf("trans transfer send read type, %s", err)
			return
		}

		_ = conn.Close()
		<-waitReadStop

		// 等待读取完成
		m := len(line)
		n := r.Buffered()

		if n+m != 0 {
			log.Printf("reader need be transferred\n")
			leafData := make([]byte, n+m)

			if 0 != m {
				copy(leafData, line)
			}

			if 0 != n {
				_, err := io.ReadFull(r, leafData[m:])
				if err != nil {
					log.Printf("leaf read failed %s, err %s", leafData, err)
					return
				}
			}
			err = transferSendReadData(uc, connId, leafData)
			if err != nil {
				log.Printf("transfer read faile err %s", err)
				return
			} else {
				log.Printf("reader transferred sucefully\n")
			}
		} else {
			log.Printf("reader no need be transferred\n")
		}
		return
	}
}

func Server(li net.Listener, wg *sync.WaitGroup, stopChan chan bool, transChan chan bool) {
	defer wg.Done()

	wg.Add(1)
	go func() {
		defer wg.Done()
		conn, err := li.Accept()
		if ope, ok := err.(*net.OpError); ok && (ope.Op == "accept") {
			log.Printf("tcp %#v listener close", li.Addr().String())
			return
		}
		if err == nil {
			log.Printf("accept sucessfully %#v %#v", conn.LocalAddr().String(), conn.RemoteAddr().String())
		} else {
			log.Printf("accept failed %s, %s", li.Addr(), err)
			return
		}
		wg.Add(1)
		go HandleReply(wg, nil, conn, stopChan, transChan)
	}()

	// 控制开关
	select {
	case <-stopChan:
	case <-transChan:
	}
	_ = li.Close()
	return
}
