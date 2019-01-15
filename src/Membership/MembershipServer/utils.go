package MembershipServer

import (
	mmsg "Membership/MembershipMessage"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"

	"github.com/golang/protobuf/proto"
)

// Struct for parsing server and self configuration.
type hostConfigJson struct {
	Hostname string `json:"hostname"`
	IP       string `json:"ip"`
}

type hostConfig struct {
	Hostname string
	IP       string
	Port     int
}

// Parsing server configuration and return a list of parsed ServerConfig struct.
func GetKnownHosts(confpath string, port int) []hostConfig {
	f, err := os.Open(confpath)
	if err != nil {
		log.Fatalf("Error open file! %s\n", err)
	}

	srvConf := make([]hostConfigJson, 0)
	decoder := json.NewDecoder(f)

	if err = decoder.Decode(&srvConf); err != nil {
		log.Fatalf("Error decoding json! %s\n", err)
	}
	var servers []hostConfig
	for _, sc := range srvConf {
		server := hostConfig{
			Hostname: sc.Hostname,
			IP:       sc.IP,
			Port:     port,
		}
		servers = append(servers, server)
	}
	return servers
}

func GetSelfInfo(confpath string, port int) *hostConfig {
	f, err := os.Open(confpath)
	if err != nil {
		log.Fatalf("Error open file! %s\n", err)
	}

	myInfoJson := new(hostConfigJson)
	decoder := json.NewDecoder(f)
	if err = decoder.Decode(&myInfoJson); err != nil {
		log.Fatalf("Error decoding json! %s\n", err)
	}
	myInfo := &hostConfig{
		Hostname : myInfoJson.Hostname,
		IP: myInfoJson.IP,
		Port: port,
	}
	return myInfo
}

func GetAddrHash(addr string) uint64 {
	retHash := sha256.New()
	retHash.Write([]byte(addr))
	ret := binary.BigEndian.Uint64(retHash.Sum(nil)[:8])
	return ret
}

func HostConfToUDP(hc *hostConfig) (*net.UDPAddr, bool) {
	ipStr := fmt.Sprintf("%s:%d", hc.IP, hc.Port)
	if myAddr, err := net.ResolveUDPAddr("udp", ipStr); err != nil {
		log.Panicf("Resolve UDP address fail! %s\n", err)
		return nil, false
	} else {
		return myAddr, true
	}
}

func GetProtoFromMemberList(ms []*MemberInfo) []*mmsg.MembershipListItem {
	mps := make([]*mmsg.MembershipListItem, len(ms))
	for i, m := range ms {
		mps[i] = &mmsg.MembershipListItem{
			Id:        m.id,
			Ip:        string(m.addr.IP.String()),
			Port:      uint32(m.addr.Port),
			Timestamp: m.timestamp,
			Status:    m.status,
		}
	}
	return mps
}

func GetMemberListFromProto(proto []*mmsg.MembershipListItem) []*MemberInfo {
	mInfos := make([]*MemberInfo, len(proto))
	for i, m := range proto {
		updAddr, err := net.ResolveUDPAddr("udp",
			fmt.Sprintf("%s:%d", m.GetIp(), m.GetPort()))
		if err == nil {
			mInfos[i] = &MemberInfo{
				m.GetId(),
				updAddr,
				m.GetTimestamp(),
				m.GetStatus(),
			}
		} else {
			log.Panic(err)
		}
	}
	return mInfos
}

func ReadProtoMarshalFromUDP(conn *net.UDPConn) ([]byte, bool) {
	msg := make([]byte, 2000)
	_, err := conn.Read(msg)
	if err != nil {
		log.Println(err)
		log.Printf("Read from %+v failed!", conn)
		return nil, false
	}
	msgSizeUint := binary.BigEndian.Uint64(msg[:8])
	return msg[8 : 8+msgSizeUint], true
}

// Prepares the marshalled proto message based on the membership list of s.
// Make sure having the updated membershiplist before calling this func.
func PrepareMsg(s *MembershipServer, rtype mmsg.MsgType) ([]byte, bool) {

	// Do not send membership list for ping and ack message
	var sentMemberListProto []*mmsg.MembershipListItem
	if rtype == mmsg.MsgType_PING || rtype == mmsg.MsgType_ACK {
		sentMemberListProto = nil
	} else {
		memberList := s.others.ToList()
		sentMemberListProto = GetProtoFromMemberList(memberList)
	}
	msg := &mmsg.MembershipMessage{
		Type:    rtype,
		SrcPort: uint32(s.myself.addr.Port),
		SrcIp:   string(s.myself.addr.IP.String()),
		List:    sentMemberListProto,
	}
	if msgMarshal, err := proto.Marshal(msg); err != nil {
		log.Printf("Can't marshal join message, %s\n", err)
		return nil, false
	} else {

		// Add message length to body
		msgSize := make([]byte, 8)
		binary.BigEndian.PutUint64(msgSize, uint64(len(msgMarshal)))
		msgMarshal = append(msgSize, msgMarshal...)

		return msgMarshal, true
	}
}
