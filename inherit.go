package main

import (
	"golang.org/x/sys/unix"
	"log"
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

var (
	TransferListenDomainSocket = filepath.Join("./", "listen.sock")
	TransferConnDomainSocket   = filepath.Join("./", "conn.sock")
	TransSwitchDomainSocket    = filepath.Join("./", "switch.sock")
	Separator                  = byte('#')
)

// 尝试连接旧进程的switch.sock，如果通信成功，并且解析内容成功，就知道需要传输旧的listener 和 connection
func IsInherit() bool {
	var (
		unixConn net.Conn
		err      error
	)
	unixConn, err = net.DialTimeout("unix", TransSwitchDomainSocket, 1*time.Second)
	if err != nil {
		log.Printf("[warning]: isReconfigure connect failed: %s\n", err)
		return false
	}
	defer unixConn.Close()

	buf := make([]byte, 1)
	n, _ := unixConn.Read(buf)
	if n != 1 {
		return false
	}
	return true
}

// 继承旧进程的 listener，监听listen.sock，希望旧进程传输数据过来，这里面加一个超时
func GetInheritListeners(timeout time.Duration) (*net.TCPListener, net.Conn, error) {
	_ = syscall.Unlink(TransferListenDomainSocket)

	l, err := net.Listen("unix", TransferListenDomainSocket)
	if err != nil {
		log.Printf("GetInheritListeners listen %s failed: %s\n", TransferListenDomainSocket, err)
		return nil, nil, err
	}
	defer l.Close()

	ul := l.(*net.UnixListener)
	_ = ul.SetDeadline(time.Now().Add(timeout))
	uc, err := ul.AcceptUnix()
	if err != nil {
		if ope, ok := err.(*net.OpError); ok && ope.Op == "accept" {
			log.Printf("getInheritListeners accept unix timeout, %s", timeout)
			return nil, nil, err
		}
		log.Printf("GetInheritListeners AcceptUnix failed: %s\n", err)
		return nil, nil, err
	}

	buf := make([]byte, 1)
	oob := make([]byte, 1024)
	_, oobn, _, _, err := uc.ReadMsgUnix(buf, oob)
	if err != nil {
		log.Printf("GetInheritListeners read msg unix failed: %s\n", err)
		return nil, nil, err
	}
	scms, err := unix.ParseSocketControlMessage(oob[0:oobn])
	if err != nil {
		log.Printf("GetInheritListeners ParseSocketControlMessage failed: %s\n", err)
		return nil, nil, err
	}
	if len(scms) != 1 {
		log.Printf("GetInheritListeners ParseSocketControlMessage failed: expected 1 but get scms = %#v\n", scms)
		return nil, nil, err
	}
	gotFds, err := unix.ParseUnixRights(&scms[0])
	if err != nil {
		log.Printf("GetInheritListeners parse unix right: %s\n", err)
		return nil, nil, err
	}

	fd := uintptr(gotFds[0])
	file := os.NewFile(fd, "")
	if file == nil {
		log.Printf("GetInheritListeners create new file failed fd %d\n", fd)
		return nil, nil, err
	}
	defer file.Close()
	fileListener, err := net.FileListener(file)
	if err != nil {
		log.Printf("GetInheritListeners recover listener from fd %d failed: %s\n", fd, err)
		return nil, nil, err
	}
	listener, ok := fileListener.(*net.TCPListener)
	if !ok {
		log.Printf("GetInheritListeners listener recovered from fd %d is not a tcp listener\n", fd)
		return nil, nil, nil

	}
	return listener, uc, nil
}

func GetInheritConnections(timeout time.Duration, wg *sync.WaitGroup, stopChan chan bool, transChan chan bool) {
	defer wg.Done()
	_ = syscall.Unlink(TransferConnDomainSocket)

	l, err := net.Listen("unix", TransferConnDomainSocket)
	if err != nil {
		log.Printf("getInheritConnections listen %s failed: %s\n", TransferConnDomainSocket, err)
		return
	}
	defer l.Close()

	defer wg.Done()
	go func() {
		wg.Add(1)
		for {
			ul := l.(*net.UnixListener)
			_ = ul.SetDeadline(time.Now().Add(timeout))
			uc, err := ul.AcceptUnix()
			if err != nil {
				if ope, ok := err.(*net.OpError); ok && ope.Op == "accept" {
					log.Printf("getInheritConnections accpet unix timeout, %s", timeout)
					return
				}
				log.Printf("getInheritConnections AcceptUnix failed: %s\n", err)
				return
			}
			// 这里可以加个协程
			log.Printf("transChan l,r %s %s", uc.LocalAddr(), uc.RemoteAddr())
			transferConnHandler(uc, wg, stopChan, transChan, HandleReply)
		}
	}()

	log.Printf("wait %s for old process connections", timeout)
	<-time.After(timeout)
	return
}

func sendInheritListeners() (net.Conn, error) {
	lf, err := gListener.File()
	if err != nil {
		return nil, err
	}
	defer lf.Close()
	var unixConn net.Conn
	// retry 10 time
	for i := 0; i < 10; i++ {
		unixConn, err = net.DialTimeout("unix", TransferListenDomainSocket, 1*time.Second)
		if err == nil {
			break
		}
		log.Printf("sendInheritListeners, dial failed, try %d times", i+1)
		time.Sleep(1 * time.Second)
	}
	if err != nil {
		log.Printf("sendInheritListeners failed %s\n", err)
		return nil, err
	}
	log.Printf("sendInheritListeners dial sucessfully")
	uc := unixConn.(*net.UnixConn)
	buf := make([]byte, 1)
	rights := syscall.UnixRights(int(lf.Fd()))
	n, oobn, err := uc.WriteMsgUnix(buf, rights, nil)
	if err != nil {
		log.Printf("writeMsgUnix failed %s", err)
		return nil, err
	}
	if n != len(buf) || oobn != len(rights) {
		log.Printf("writeMsgUnix = %d, %d; want 1, %d", n, oobn, len(rights))
		return nil, err
	}
	return uc, nil
}

// 一旦发现有新进程过来通信，就立刻 close(transChan) 通知其他协程进行transfer
func ListenNextInherit(wg *sync.WaitGroup, stopChan chan bool, transChan chan bool) {
	defer wg.Done()
	// 进程进入关闭状态，并且相关listener已经发送了，那么就可以关闭之前的listener
	defer gListener.Close()

	_ = syscall.Unlink(TransSwitchDomainSocket)

	l, err := net.Listen("unix", TransSwitchDomainSocket)
	if err != nil {
		log.Printf("ListenNextInherit faield %s\n", err)
		return
	}
	defer l.Close()

	ul := l.(*net.UnixListener)

	// 主循环进程
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			uc, err := ul.AcceptUnix()
			if err != nil {
				if ope, ok := err.(*net.OpError); ok && (ope.Op == "accept") {
					log.Printf("ListenNextInherit unix socket listener closed")
				} else {
					log.Printf("ListenNextInherit Accept error :%v\n", err)
				}
				return
			}
			_, err = uc.Write([]byte{0}) // 是几无所谓，一个字节就行
			if err != nil {
				log.Printf("ListenNextInherit Accept error :%v\n", err)
				continue
			}
			_ = uc.Close()
			notify, err := sendInheritListeners()
			if err != nil {
				log.Printf("ListenNextInherit sendInheritListeners failed:%s\n", err)
				continue
			}
			var buf [1]byte
			n, _ := notify.Read(buf[:])
			if n != 1 {
				log.Printf("ListenNextInherit new main start failed n = %d\n", n)
				continue
			}
			atomic.StoreInt32(&runState, 1)
			close(transChan)
		}
	}()

	// 控制开关
	select {
	case <-transChan:
	case <-stopChan:
	}
}
