package SDFSNode

import (
	mmsg "Membership/MembershipMessage"
	smsg "SDFS/SDFSMessage"
	"io"
	"log"
	"os"
	"runtime"
)

func (s *SDFSServer) HandleDelete(fn string) {
	allVersions, exist := s.files[fn]
	if !exist {
		log.Fatalf("%s doesn't exist on %s",
			fn, s.myself.IP.String())
	}
	for _, f := range *allVersions {
		err := os.Remove(f.localPath)
		if err != nil {
			log.Fatalf("Delete file error! %s", err)
		}
	}
	delete(s.files, fn)
}

func (s *SDFSServer) HandlePut(in *smsg.FileOpRequest, f *os.File) {
	_, err := f.Write(in.File)
	if err != nil {
		log.Fatalf("Write to file error! %s", err)
	}
}

func (s *SDFSServer) HandleGet(
	in *smsg.FileOpRequest, singleVersion bool,
	stream *smsg.Sdfs_FileOpServer) {

	allVersions, exist := s.files[in.SdfsFilename]
	if !exist {
		log.Fatalf("%s doesn't exist on %s",
			in.SdfsFilename, s.myself.IP.String())
	}

	buf := make([]byte, 1024*512)
	if singleVersion {
		newest := len(*allVersions)
		retFile := (*allVersions)[newest-1]
		f, err := os.Open(retFile.localPath)
		if err != nil {
			log.Fatalf("Open local file fail! %s", err)
		}
		for {
			_, err = f.Read(buf)
			if err == io.EOF {
				break
			} else if err != nil {
				log.Fatalf("Read local file fail! %s", err)
			} else {
				s.sendFileOpResponse(
					smsg.OpStatus_OK, buf, stream)
			}
		}
		log.Print("[get] Finish sending file.")
	} else {
		nv := int(in.Versions)   // num versions
		ava := len(*allVersions) // all version available
		var start = 0
		if nv <= ava && nv > 0 {
			start = ava - nv
		} else {
			log.Printf("[get-versions] There are only "+
				"%s version of this file.", ava)
		}
		log.Printf("[get-versions] Version %d to %d", start, ava)
		for _, retFile := range (*allVersions)[start:] {
			f, err := os.Open(retFile.localPath)
			if err != nil {
				log.Fatalf("Open local file fail! %s", err)
			}

			for {
				_, err = f.Read(buf)
				if err == io.EOF {
					break
				} else if err != nil {
					log.Fatalf("Read local file fail! %s", err)
				} else {
					s.sendFileOpResponse(
						smsg.OpStatus_OK, buf, stream)
				}
				// Goroutine tight loop walk around
				runtime.Gosched()

			}
			sep := []byte("\n%%%%%%%%%%%%%%%%%%%%\n\n")
			s.sendFileOpResponse(smsg.OpStatus_OK, sep, stream)
		}
		log.Print("[get-versions] Finish sending file.")
	}
}

func (s *SDFSServer) sendFileOpResponse(
	status smsg.OpStatus, ctnt []byte, stream *smsg.Sdfs_FileOpServer) {
	res := &smsg.FileOpResponse{
		Status: status,
		File:   ctnt,
	}
	(*stream).Send(res)
}

func (s *SDFSServer) getActiveServerList() []smsg.Node {
	// Ugly but I just don't want to change Membership struct.
	var iptoh = map[string]string{
		"172.22.158.128": "h1",
		"172.22.154.129": "h2",
		"172.22.156.129": "h3",
		"172.22.158.129": "h4",
		"172.22.154.130": "h5",
		"172.22.156.130": "h6",
		"172.22.158.130": "h7",
		"172.22.154.131": "h8",
		"172.22.156.131": "h9",
		"172.22.158.131": "h10",
	}
	var activeServers []smsg.Node
	for _, server := range s.mServer.GetOthers() {
		if server.GetStatus() == mmsg.SrvStatus_ACTIVE {
			ipstr := server.GetAddr().IP.String()
			node := smsg.Node{
				HostName: iptoh[ipstr],
				HostIp:   ipstr,
				Port:     int64(server.GetAddr().Port),
			}
			activeServers = append(activeServers, node)
		}
	}
	return activeServers
}
