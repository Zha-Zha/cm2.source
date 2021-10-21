package main

import (
	"fmt"
	"golang.org/x/sys/unix"
	"log"
	"net"
	"os"
	"sync"
	"sync/atomic"
)

// 注意，同一个conn一定是先有读再有写，这样才能在新进程中生成一个conntion ID，write buffer过来的时候，直接着地conntion ID
// 当然这里只考虑 read buffer
/**
 *  transfer read protocol
 *  header (8 bytes) + (readBuffer data)
 *
 * 0                       4
 * +-----+-----+-----+-----+
 * |      data length      |
 * +-----+-----+-----+-----+
 * |        data           |
 * +-----+-----+-----+-----+
 *
*
 *  transfer write protocol
 *  header (8 bytes) + (writeBuffer data)
 *
 * 0                       4                       8
 * +-----+-----+-----+-----+-----+-----+-----+-----+
 * |      data length      |    connection  ID     |
 * +-----+-----+-----+-----+-----+-----+-----+-----+
 * |                     data                      |
 * +-----+-----+-----+-----+-----+-----+-----+-----+
 *
**/

var connMap sync.Map
var id = uint32(0)

func transferConnHandler(uc *net.UnixConn, wg *sync.WaitGroup, stopChan chan bool, transChan chan bool, handler func(*sync.WaitGroup, []byte, net.Conn, chan bool, chan bool)) {
start:
	t, oob, err := transferRecvType(uc)
	if err != nil {
		log.Printf("transerConnHandler transferRecvType error :%v", err)
		return
	}

	/**
	 * type (1 bytes)
	 *  0 : transfer read and FD
	 *  1 : transfer write
	 *  2 : transfer read data
	 **/

	switch t {
	case 1:
		log.Printf("not support write!!!!!!!!")
		return
	case 0:
		conn, err := expressrecvFd(oob)
		if err != nil {
			log.Printf("expressrecvFd failed, %s", err)
			return
		}
		cid := atomic.AddUint32(&id, 1)
		connMap.Store(cid, conn)
		err = transferSendID(uc, cid)
		if err != nil {
			log.Printf("transfer send id failed, %s", err)
			return
		}
		goto start
	case 2:
		cid, dataBuf, err := transferReadRecvData(uc)
		if err != nil {
			log.Printf("transferReadRecvData failed, %s", err)
			return
		}
		if v, ok := connMap.Load(cid); ok {
			conn := v.(net.Conn)
			wg.Add(1)
			go handler(wg, dataBuf, conn, stopChan, transChan)
		} else {
			log.Printf("connMap load failed")
		}
	}
}

func expressrecvFd(oob []byte) (net.Conn, error) {
	scms, err := unix.ParseSocketControlMessage(oob)
	if err != nil {
		return nil, fmt.Errorf("ParseSocketControlMessage: %v", err)
	}
	if len(scms) != 1 {
		return nil, fmt.Errorf("expected 1 SocketControlMessage; got scms = %#v", scms)
	}
	scm := scms[0]
	gotFds, err := unix.ParseUnixRights(&scm)
	if err != nil {
		return nil, fmt.Errorf("unix.ParseUnixRights: %v", err)
	}
	if len(gotFds) != 1 {
		return nil, fmt.Errorf("wanted 1 fd; got %#v", gotFds)
	}
	f := os.NewFile(uintptr(gotFds[0]), "fd-from-old")
	defer f.Close()
	conn, err := net.FileConn(f)
	if err != nil {
		return nil, fmt.Errorf("FileConn error :%v", gotFds)
	}
	return conn, nil
}
