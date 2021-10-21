package main

import (
	"encoding/binary"
	"fmt"
	"log"
	"net"
	"syscall"
	"time"
)

/**
 * type (1 bytes)
 *  0 : transfer read and FD
 *  1 : transfer write
 *  2 : transfer read data
 **/

func transferSendMsg(uc *net.UnixConn, bs []byte) error {
	// send header
	l := len(bs)

	buf := make([]byte, 4+l)
	binary.BigEndian.PutUint32(buf[0:], uint32(l))
	copy(buf[4:], bs)

	if _, err := uc.Write(buf); err != nil {
		return err
	}
	return nil
}

func connTransferSendSock() (*net.UnixConn, error) {
	var (
		unixConn net.Conn
		err      error
	)
	for i := 0; i < 10; i++ {
		unixConn, err = net.DialTimeout("unix", TransferConnDomainSocket, time.Duration(1)*time.Second)
		if err == nil {
			break
		}
		log.Printf("[warning] connTransferSendSock Dial unix failed err:%v, try %d times\n", err, i+1)
	}
	if err != nil {
		return nil, err
	}
	log.Printf("connTransferSendSock  Dial sucessfully")
	uc := unixConn.(*net.UnixConn)
	return uc, nil
}

// transfer read, send FD
func transferSendReadConnFd(uc *net.UnixConn, c net.Conn) (uint32, error) {
	// change conn into fd
	conn, ok := c.(*net.TCPConn)
	if !ok {
		return 0, fmt.Errorf("transfer read change *net.TCPConn failed c:%p", c)
	}
	file, err := conn.File()
	if err != nil {
		log.Printf("transfer read File failed c:%p, err:%v", c, err)
		return 0, fmt.Errorf("TCP File failed %v", err)
	}
	defer file.Close()

	// transfer read
	buf := make([]byte, 1)
	buf[0] = 0
	rights := syscall.UnixRights(int(file.Fd()))
	n, oobn, err := uc.WriteMsgUnix(buf, rights, nil)
	if err != nil {
		return 0, fmt.Errorf("WriteMsgUnix: %v", err)
	}
	if n != len(buf) || oobn != len(rights) {
		return 0, fmt.Errorf("WriteMsgUnix = %d, %d; want 1, %d", n, oobn, len(rights))
	}

	// 阻塞等待 返回新进程的 conn id
	id := transferRecvID(uc)
	if id == 0 {
		return 0, fmt.Errorf("transferRecvID failed, %d", id)
	}
	return id, nil
}

func transferSendReadData(uc *net.UnixConn, connId uint32, data []byte) error {
	buf := make([]byte, 1)
	buf[0] = 2
	_, err := uc.Write(buf)
	if err != nil {
		return err
	}
	idBs := make([]byte, 4)
	binary.BigEndian.PutUint32(idBs, connId)
	err = transferSendMsg(uc, append(idBs, data...))
	if err != nil {
		return err
	}
	return nil
}

func transferSendBytes(uc *net.UnixConn, b []byte) error {
	if len(b) == 0 {
		return nil
	}
	_, err := uc.Write(b)
	if err != nil {
		return fmt.Errorf("transferSendMsg failed: %v", err)
	}
	return nil
}

func transferSendID(uc *net.UnixConn, id uint32) error {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, uint32(id))
	return transferSendBytes(uc, b)
}
