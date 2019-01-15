package SDFSNode

import (
	msrv "Membership/MembershipServer"
	smsg "SDFS/SDFSMessage"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"os"
	"path"
	"runtime"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
)

type SDFSFile struct {
	filename  string
	version   int
	localPath string
}

type SDFSServer struct {
	hostname   string                 // A human readable string for hostname
	conn       net.Listener           // TCP connection for grpc server
	myself     *net.TCPAddr           // Myself id-ed by TCPAddr
	master     *net.TCPAddr           // Master id-ed by TCPAddr
	isMaster   bool                   // Am I master
	masterInfo *SDFSMaster            // Master metadata info for all hosts
	mServer    *msrv.MembershipServer // Underline membership server
	files      map[string]*[]SDFSFile // File Stored at this server
}

type SDFSMaster struct {
	lock *sync.Mutex
	// Key: server IP; Val: filename -> all versions of that file
	serverToFile map[string](map[string][]*SDFSFile)
	// Key: filename;  Val: all servers storing that file
	fileToServer map[string][]*smsg.Node
}

func (s *SDFSServer) SetMemberServer(mServer *msrv.MembershipServer) {
	s.mServer = mServer
}

func (s *SDFSServer) SetHostname(hn string) {
	s.hostname = hn
}

func (s *SDFSServer) SetConn(conn net.Listener) {
	s.conn = conn
}

func (s *SDFSServer) SetMaster(m *net.TCPAddr) {
	s.master = m
}

func (s *SDFSServer) SetIsMaster(im bool) {
	s.isMaster = im
}

func (s *SDFSServer) SetMyself(myself *net.TCPAddr) {
	s.myself = myself
}

func (s *SDFSServer) HandleCommand(cmd string) {
	cmd = strings.TrimSuffix(cmd, "\n")
	switch cmd {
	case "join":
		s.StartServer()
	case "list_self":
		s.mServer.ListSelf()
	case "list_members":
		s.mServer.ListMember()
	case "leave":
		s.LeaveSdfs()
	default:
		log.Printf("%s is not a valid command\n", cmd)
	}
}

func (s *SDFSServer) StartServer() {
	// Setup membership Server
	s.mServer.JoinGroup(s.myself.Port)
	log.Print("Membership join complete.")
	log.Printf("Master node is at %s", s.master.IP.String())

	// Init master struct
	if s.isMaster {
		s.masterInfo = &SDFSMaster{}
		s.masterInfo.serverToFile = map[string](map[string][]*SDFSFile){}
		s.masterInfo.fileToServer = map[string][]*smsg.Node{}
		s.masterInfo.lock = &sync.Mutex{}
	}

	// Init server file list
	s.files = make(map[string]*[]SDFSFile)

	// Setup SDFS Server
	grpcServer := grpc.NewServer()
	smsg.RegisterSdfsServer(grpcServer, s)

	// Remove all files under /sdfs
	dir, err := ioutil.ReadDir("/sdfs")
	if err != nil {
		log.Fatalf("Read dir fails! %v", err)
	}
	for _, d := range dir {
		os.RemoveAll(path.Join([]string{"/sdfs", d.Name()}...))
	}
	go grpcServer.Serve(s.conn)

	// Receive failure message from membership layer
	go s.MonitorFailure()

	log.Print("Finish setting up server")
}

func (s *SDFSServer) LeaveSdfs() {
	// Clean up SDFS related stuff

	// Clean up Membership Server related stuff
	s.mServer.LeaveGroup()
}

func (s *SDFSServer) MonitorFailure() {
	for {
		if failIp, ok := <-s.mServer.Frc; ok {
			log.Printf("SDFS Server %s failed!", failIp)

			// Assuming all hosts listens to the same port
			failNode := &smsg.Node{
				HostIp: failIp,
				Port:   int64(s.myself.Port),
			}
			clnt, conn := CreateRpcClient(s.master.IP.String(),
				s.master.Port, time.Second)
			defer conn.Close()
			ctx, cancel := context.WithTimeout(
				context.Background(), 300*time.Second)
			defer cancel()
			req := &smsg.FailureReportRequest{
				FailedNode: failNode,
			}
			_, err := clnt.FailureReport(ctx, req)
			if err != nil {
				log.Fatalf("Failure report failed! %s", err)
			} else {
				log.Printf("[Failure Report] Finish reporting."+
					" %s", err)
			}

		} else {
			log.Panic("Failure report channel is closed!")
		}
		runtime.Gosched()
	}
}

func (s *SDFSServer) requestFileOp(dst smsg.Node, metaReq *smsg.MetadataOpRequest,
	wg *sync.WaitGroup) {
	if wg != nil {
		defer wg.Done()
	}
	clnt, conn := CreateRpcClient(dst.HostIp, int(dst.Port), time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()
	defer conn.Close()

	req := &smsg.FileOpRequest{
		Op:           metaReq.Op,
		SdfsFilename: metaReq.SdfsFilename,
		Versions:     metaReq.Versions,
	}
	stream, err := clnt.FileOp(ctx)
	if err != nil {
		log.Fatalf("FileOp request fail! %s", err)
	}

	// Read FileOp response
	waitc := make(chan struct{})
	go func() {
		var f *os.File
		if metaReq.Op == smsg.OpType_GET ||
			metaReq.Op == smsg.OpType_GET_VERSIONS {
			f, err = os.Create(metaReq.LocalFilename)
			if err != nil {
				log.Fatalf("Create GET local file failed! %v", err)
			}
			log.Printf("[get] File created at %s, start writing.",
				metaReq.LocalFilename)
			defer f.Close()
		}

		for {
			in, err := stream.Recv()
			if err == io.EOF {
				close(waitc)
				break
			}

			if err != nil {
				log.Fatalf("Fail to receive fileOp response! %v",
					err)
			}
			// Handle FileOp response
			// If we send a GET or GET-VERSIONS request,
			// we are expecting to receive file bytes,
			// Otherwise we will only get a return status.
			if metaReq.Op == smsg.OpType_GET ||
				metaReq.Op == smsg.OpType_GET_VERSIONS {
				if len(in.File) == 0 {
					log.Print("File len 0! Weird")
				}
				_, err = f.Write(in.File)
				if err != nil {
					log.Fatalf("Write to local file failed!"+
						" %s", err)
				}
			}

			// Goroutine tight loop walk around
			runtime.Gosched()
		}
		return
	}()

	// Send FileOp request
	// PUT request send as a stream, transferring file.
	if metaReq.Op == smsg.OpType_PUT {
		file, err := os.Open(metaReq.LocalFilename)
		if err != nil {
			log.Fatalf("Open local file for put failed! %s", err)
		}

		buf := make([]byte, 1024*512)
		for {
			n, err := file.Read(buf)
			if err == io.EOF {
				break
			} else if err != nil {
				log.Fatalf("Reading from file err! %s", err)
			}
			// log.Printf("[put] Read %d bytes from file", n)
			req.File = buf[:n]
			stream.Send(req)

			// Goroutine tight loop walk around
			runtime.Gosched()
		}
	} else {
		// Other requests doesn't have the *File* field
		stream.Send(req)
	}

	// Finish sending, wait for read to complete
	stream.CloseSend()

	log.Printf("[%s] Operation request sent.", metaReq.Op)

	<-waitc
	log.Printf("[%s] Operation response received.", metaReq.Op)

	// Inform master that the PUT op completes for this server.
	// Modify master's metadata at the end to prevent inconsistency
	//   caused by the failing of this server.
	if metaReq.Op == smsg.OpType_PUT {
		log.Print("[put] Updating master about metadata.")
		clnt, conn := CreateRpcClient(s.master.IP.String(),
			s.master.Port, time.Second)
		ctx, cancel2 := context.WithTimeout(context.Background(),
			5*time.Second)
		defer cancel2()
		defer conn.Close()
		umReq := &smsg.UpdateMetaRequest{
			UpdateNode: &smsg.Node{
				HostName: dst.HostName,
				HostIp:   dst.HostIp,
				Port:     dst.Port},
			SdfsFilename: metaReq.SdfsFilename,
			Versions:     metaReq.Versions,
		}
		umRes, err := clnt.UpdateMeta(ctx, umReq)
		if err != nil {
			log.Fatalf("Update metadata failed! %v", err)
		} else {
			log.Printf("[put] Finish updating metadata. %v", umRes.Status)
		}
	}
	return
}

func (s *SDFSServer) FileOp(stream smsg.Sdfs_FileOpServer) error {
	initRound := true
	var f *os.File
	var lpath string // Local path for the operated file
	var curV = 0
	var opType smsg.OpType
	var fn string
	for {
		in, err := stream.Recv()
		if err == io.EOF {
			// Update information about the file being PUT
			if opType == smsg.OpType_PUT {
				log.Printf("[put] New file %s stored here", fn)
				newFile := SDFSFile{
					filename:  fn,
					version:   curV,
					localPath: lpath,
				}
				log.Printf("[put] Localpath for this files is %s",
					newFile.localPath)
				oldFileList, exist := s.files[fn]
				var newFiles []SDFSFile
				if exist {
					newFiles = append(*oldFileList, newFile)
				} else {
					newFiles = append(newFiles, newFile)
				}
				s.files[fn] = &newFiles
			}
			return nil
		} else if err != nil {
			return err
		}

		opType = in.Op
		fn = in.SdfsFilename
		if opType == smsg.OpType_DELETE {
			s.HandleDelete(fn)
			log.Printf("[delete] Delete done!")
			return nil
		} else if opType == smsg.OpType_PUT {
			if initRound {
				initRound = false
				allVersions, exist := s.files[fn]
				if exist {
					curV = len(*allVersions)
				}

				// Check consistency of the metadata and node
				if curV != int(in.Versions) {
					log.Print("Metadata inconsistent!")
					log.Fatalf("meta: %d, node: %d",
						in.Versions, curV)
				}
				// Create the file to write to.
				// Slash (/) in filename is replaced by %2F
				// All file is stored in /sdfs
				newFn := strings.Replace(fn,
					"/", "%2F", -1)
				lpath = fmt.Sprintf("/sdfs/%s_%d", newFn, curV)

				f, err = os.Create(lpath)
				if err != nil {
					log.Fatalf("Create file error! %s", err)
				}
				defer f.Close()
			}

			s.HandlePut(in, f)

		} else if opType == smsg.OpType_GET ||
			opType == smsg.OpType_GET_VERSIONS {
			singleVersion := (opType == smsg.OpType_GET)
			s.HandleGet(in, singleVersion, &stream)
			return nil
		} else {
			log.Fatalf("FileOp doesn't handle this type! %s", opType)
		}

		// Goroutine tight loop walk around
		runtime.Gosched()
	}
}

func (s *SDFSServer) Replicate(
	ctx context.Context, req *smsg.ReplicateRequest,
) (*smsg.OpResponse, error) {
	fileSrc := *req.Src
	filename := req.Filename
	newFn := strings.Replace(filename, "/", "%2F", -1)

	lpath := fmt.Sprintf("/sdfs/%s_0", newFn)

	newReq := &smsg.MetadataOpRequest{
		Op:            smsg.OpType_GET,
		SdfsFilename:  filename,
		LocalFilename: lpath,
	}
	s.requestFileOp(fileSrc, newReq, nil)
	s.files[filename] = &[]SDFSFile{
		SDFSFile{filename: filename, version: 0, localPath: lpath},
	}
	return &smsg.OpResponse{}, nil
}

func (s *SDFSServer) HandleLocalClient(
	ctx context.Context, req *smsg.MetadataOpRequest,
) (*smsg.OpResponse, error) {
	res := &smsg.OpResponse{}
	var err error
	main_start := time.Now()
	req.Requestor = &smsg.Node{
		HostName: s.hostname,
		HostIp:   s.myself.IP.String(),
		Port:     int64(s.myself.Port),
	}
	if req.Op == smsg.OpType_STORE {
		// List local file
		if len(s.files) == 0 {
			log.Print("[store] No file currently stored here.")
		} else {
			log.Print("[store] Files stored at this server:")
			for fn, f := range s.files {
				log.Printf("[store] * %s, %d versions",
					fn, len(*f))
			}
		}
		res.Status = smsg.OpStatus_OK
	} else {
		// Send operation to master if I'm not master,
		// Otherwise call local function
		var metaRes *smsg.MetadataOpResponse
		if !s.isMaster {
			clnt, conn := CreateRpcClient(s.master.IP.String(),
				s.master.Port, 10*time.Second)
			defer conn.Close()
			metaRes, err = clnt.MetadataOp(ctx, req)
		} else {
			metaRes, err = s.MetadataOp(ctx, req)
		}
		if err != nil {
			log.Fatalf("Metadata operation failed! %s", err)
		}
		log.Print("[Metadata] Get back metadata information.")

		if req.Op == smsg.OpType_LS {
			// LS is done after getting back metadata response
			if len(metaRes.Hosts) == 0 {
				log.Printf("[ls] File %s is not in SDFS.",
					req.SdfsFilename)
			} else {
				log.Printf("[ls] File %s is stored at: ",
					req.SdfsFilename)
				for _, h := range metaRes.Hosts {
					log.Printf("[ls] * %s at %s",
						h.HostName, h.HostIp)
				}
			}

		} else if req.Op == smsg.OpType_PUT ||
			req.Op == smsg.OpType_DELETE {
			// PUT and DELETE should send to all replicas
			var wg sync.WaitGroup
			log.Printf("[%s] Send to hosts: ", req.Op)
			req.Versions = metaRes.Versions
			for _, h := range metaRes.Hosts {
				wg.Add(1)
				log.Printf("[%s] %s at %s",
					req.Op, h.HostName, h.HostIp)
				go s.requestFileOp(*h, req, &wg)
			}
			wg.Wait()

		} else {
			// GET and GET_VERSIONS send to only 1 replica
			var wg2 sync.WaitGroup
			for _, h := range metaRes.Hosts {
				if h.HostIp != s.myself.IP.String() {
					wg2.Add(1)
					go s.requestFileOp(*h, req, &wg2)
					break
				}
			}
			wg2.Wait()
		}
		main_elapsed := time.Since(main_start)
		log.Printf("[Time elapsed] %s\n", main_elapsed)
	}
	return res, err
}
