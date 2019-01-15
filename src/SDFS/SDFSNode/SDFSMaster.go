package SDFSNode

import (
	smsg "SDFS/SDFSMessage"
	"context"
	"log"
	"math/rand"
	"runtime"
	"time"
)

func (s *SDFSServer) MetadataOp(
	ctx context.Context, req *smsg.MetadataOpRequest,
) (*smsg.MetadataOpResponse, error) {
	log.Printf("[Metadata] Get %s request at node %s",
		req.Op, req.Requestor.HostName)
	res := &smsg.MetadataOpResponse{}
	// For PUT
	// - if the file doesn't exist before, select 1 nodes at random,
	//   put file in this and following 3 nodes,
	//   record the selection in masterInfo and return
	// - if the file exists, treat it like other op types
	//   and return all nodes holding this file
	sfn := req.SdfsFilename
	servers, ok := s.masterInfo.fileToServer[sfn]

	if req.Op == smsg.OpType_PUT {
		if !ok {
			res.Status = smsg.OpStatus_CREATE
			curActSrv := s.getActiveServerList()
			numSrv := len(curActSrv)
			r := rand.New(rand.NewSource(time.Now().UnixNano()))
			idx := r.Intn(numSrv)
			var newHosts []*smsg.Node
			// This assumes that we have at least 4 active nodes
			for i := 0; i < 4; i++ {
				host := curActSrv[idx]
				newHosts = append(newHosts, &host)
				idx += 1
				if idx >= numSrv {
					idx = 0
				}
			}
			res.Versions = 0
			res.Hosts = newHosts
		} else {
			res.Status = smsg.OpStatus_OK
			res.Hosts = servers
			reps := (*servers[0]).HostIp
			avoh := s.masterInfo.serverToFile[reps][sfn]
			res.Versions = int64(len(avoh))
		}
	} else if req.Op == smsg.OpType_DELETE {
		if !ok {
			log.Print("[delete] File doesn't exist according to metadata.")
		} else {
			// Remove metadata for that file first.
			for _, h := range servers {
				delete(s.masterInfo.serverToFile[h.HostIp], sfn)
			}
			delete(s.masterInfo.fileToServer, req.SdfsFilename)
		}
		res.Status = smsg.OpStatus_DEL
		res.Hosts = servers
	} else {
		res.Status = smsg.OpStatus_OK
		res.Hosts = servers
	}
	log.Printf("[Metadata] Request handling done.")
	return res, nil
}

func (s *SDFSServer) UpdateMeta(
	ctx context.Context, req *smsg.UpdateMetaRequest,
) (*smsg.OpResponse, error) {
	res := &smsg.OpResponse{}
	hip := req.UpdateNode.HostIp
	fn := req.SdfsFilename
	v := req.Versions

	newFile := &SDFSFile{
		filename: fn,
		version:  int(v),
	}

	newHost := req.UpdateNode
	s.masterInfo.lock.Lock()
	defer s.masterInfo.lock.Unlock()

	_, serverHasFile := s.masterInfo.serverToFile[hip]
	if !serverHasFile {
		s.masterInfo.serverToFile[hip] = map[string][]*SDFSFile{}
	}
	oldFileList := s.masterInfo.serverToFile[hip][fn]
	newFileList := append(oldFileList, newFile)
	s.masterInfo.serverToFile[hip][fn] = newFileList

	oldHostList, exist := s.masterInfo.fileToServer[fn]
	hostListed := false
	for _, h := range oldHostList {
		if h.HostIp == newHost.HostIp {
			hostListed = true
			break
		}
	}
	if (!exist) || (!hostListed) {
		newHostList := append(oldHostList, newHost)
		s.masterInfo.fileToServer[fn] = newHostList
	}
	res.Status = smsg.OpStatus_OK
	return res, nil
}

func (s *SDFSServer) FailureReport(
	ctx context.Context, req *smsg.FailureReportRequest,
) (*smsg.OpResponse, error) {
	failIp := req.FailedNode.HostIp
	log.Printf("[Failure Report] Node %s failed. ", failIp)

	// Get all files this server holds
	s.masterInfo.lock.Lock()
	defer s.masterInfo.lock.Unlock()
	avf, exist := s.masterInfo.serverToFile[failIp]
	if exist {
		delete(s.masterInfo.serverToFile, failIp)
		// Get current active server list
		casl := s.getActiveServerList()

		// New Random source
		r := rand.New(rand.NewSource(time.Now().UnixNano()))

		// For each file stored at that node
		for fn, _ := range avf {
			asrv := s.masterInfo.fileToServer[fn]

			// Remove the failed server
			for i, h := range asrv {
				if h.HostIp == failIp {
					newSrvList := append(asrv[:i],
						asrv[i+1:]...)
					s.masterInfo.fileToServer[fn] = newSrvList
					break
				}
			}
			primary := *s.masterInfo.fileToServer[fn][0]
			var nsi = 0 // new server index
			for {
				nip := casl[nsi].HostIp
				if nip != failIp {
					_, hasServer := s.masterInfo.serverToFile[nip]
					if !hasServer {
						break
					} else {
						_, hasFile := s.masterInfo.serverToFile[nip][fn]
						if !hasFile {
							break
						}
					}
				}
				nsi = r.Intn(len(casl))
				runtime.Gosched()

			}
			newReplica := casl[nsi]

			// udpate metadata info
			oldSrv := s.masterInfo.fileToServer[fn]
			newSrv := append(oldSrv, &newReplica)
			s.masterInfo.fileToServer[fn] = newSrv

			_, exist := s.masterInfo.serverToFile[newReplica.HostIp]
			if !exist {
				s.masterInfo.serverToFile[newReplica.HostIp] =
					map[string][]*SDFSFile{}
			}

			s.masterInfo.serverToFile[newReplica.HostIp][fn] = []*SDFSFile{
				&SDFSFile{
					filename: fn,
					version:  0,
				},
			}

			go s.SendReplicateRequest(primary, newReplica, fn)

		}
		log.Print("[Failure Report] Remove metadata done.")
		log.Print("[Failure Report] Re-replicate requests sent.")
	} else {
		log.Printf("[Failure Report] No file currently stored there.")
	}

	return &smsg.OpResponse{}, nil
}

func (s *SDFSServer) SendReplicateRequest(src smsg.Node, dst smsg.Node, fn string) {
	log.Printf("[Replicate] Copy file from %s to %s", src.HostName, dst.HostName)
	cur_time := time.Now()
	clnt, conn := CreateRpcClient(dst.HostIp, int(dst.Port), time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer conn.Close()
	defer cancel()

	req := &smsg.ReplicateRequest{
		Src:      &src,
		Dst:      &dst,
		Filename: fn,
	}
	_, err := clnt.Replicate(ctx, req)
	if err != nil {
		log.Fatalf("Replicate request fails! %s", err)
	} else {
		log.Printf("[Replicate] Finished for file %s", fn)
	}

	time_elapsed := time.Since(cur_time)
	log.Printf("[Failure Replicate time] %s", time_elapsed)

	// Update metadata
	return
}
