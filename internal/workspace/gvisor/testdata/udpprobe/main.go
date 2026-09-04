package main

import (
	"bytes"
	"fmt"
	"net"
	"os"
	"time"
)

func main() {
	if len(os.Args) != 3 {
		fmt.Fprintln(os.Stderr, "usage: udpprobe HOST PORT")
		os.Exit(2)
	}
	conn, err := net.DialTimeout("udp4", net.JoinHostPort(os.Args[1], os.Args[2]), time.Second)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer conn.Close()
	if err := conn.SetDeadline(time.Now().Add(2 * time.Second)); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	payload := []byte("remount-udp-probe")
	if _, err := conn.Write(payload); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	reply := make([]byte, len(payload))
	if _, err := conn.Read(reply); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if !bytes.Equal(reply, payload) {
		fmt.Fprintln(os.Stderr, "unexpected UDP reply")
		os.Exit(1)
	}
}
