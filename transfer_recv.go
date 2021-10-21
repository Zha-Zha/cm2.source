package main

import (
	"encoding/binary"
	"fmt"
	"net"
)

// 新进程从unix socket中读取 旧进程中 tcp conn 的fd，并转化为tcp conn
func transferRecvType(uc *net.UnixConn) (byte, []byte, error) {
	buf := make([]byte, 1)
	oob := make([]byte, 32)
	n, oobn, _, _, err := uc.ReadMsgUnix(buf, oob)
	if err != nil {
		return 0, nil, fmt.Errorf("ReadMsgUnix error: %v, n: %d", err, n)
	}

	return buf[0], oob[0:oobn], nil
}

func transferRecvMsg(uc *net.UnixConn, size int) ([]byte, error) {
	if size == 0 {
		return nil, nil
	}
	b := make([]byte, size)
	var n, off int
	var err error
	for {
		n, err = uc.Read(b[off:])
		if err != nil {
			return nil, fmt.Errorf("transferRecvMsg failed: %v", err)
		}
		off += n
		if off == size {
			return b, nil
		}
	}
}

func transferRecvHead(uc *net.UnixConn) (int, error) {
	buf, err := transferRecvMsg(uc, 4)
	if err != nil {
		return 0, fmt.Errorf("ReadMsgUnix error: %v", err)
	}
	size := int(binary.BigEndian.Uint32(buf))
	return size, nil
}

// 新进程从unix socket 中读取 旧进程中 conn reader 的 buffer
func transferReadRecvData(uc *net.UnixConn) (uint32, []byte, error) {
	// recv header
	dataSize, err := transferRecvHead(uc)
	if err != nil {
		return 0, nil, err
	}
	// recv read buffer
	buf, err := transferRecvMsg(uc, dataSize)
	if err != nil {
		return 0, nil, err
	}

	if len(buf) < 4 {
		return 0, nil, fmt.Errorf("transfer read recv len(buf) < 4, %v", buf)
	}

	return binary.BigEndian.Uint32(buf[:4]), buf[4:], nil
}

func transferRecvID(uc *net.UnixConn) uint32 {
	b, err := transferRecvMsg(uc, 4)
	if err != nil {
		return 0
	}
	return binary.BigEndian.Uint32(b)
}
