package MembershipServer

import (
	"bytes"
	"log"
	"net"
	"testing"
)

func TestWriteProtoMarshalToUDP(t *testing.T) {
	t.Run("Write little things", func(t *testing.T) {
		listenAddr, err := net.ResolveUDPAddr("udp", "127.0.0.1:8888")
		if err != nil {
			t.Fatal(err)
		}

		listenConn, err := net.ListenUDP("udp", listenAddr)
		if err != nil {
			t.Fatal(err)
		}
		sendConn, err := net.DialUDP("udp", nil, listenAddr)
		expect := []byte("Hello")
		if err != nil {
			t.Fatal(err)
		}
		go WriteProtoMarshalToUDP(sendConn, expect)
		t.Log("Start sending")
		result, ok := ReadProtoMarshalFromUDP(listenConn)
		log.Println("gg")
		if ok {
			if !bytes.Equal(result, expect) {
				t.Error("result is not matched")
				t.Fail()
			}
		} else {
			t.Error("Can't get result from listen udp connection")
			t.Fail()
		}
	})

}
