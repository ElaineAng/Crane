package main

// CClient sets up a RPC client, and submits a job (topology) to Crane Master
// A job consists of the following:
//  * component name
//  * func name to run for this component
//  * grouping (A set a grouping of B meaning A reads input from B)

// Func definition for each func should be implemented in ./Jobs, and compiled with
// go build -buildmode=plugin -o /dst/path/filename.so /path/to/jobs/source.go

// Invoking client by:
//     CClient <topo_config> <jobs.so>
// Client finishes when the job completes (all data processed)

import (
	cmsg "Crane/CraneMessage"
	snode "SDFS/SDFSNode"
	"bufio"
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"time"

	"google.golang.org/grpc"
)

func parseTopoConfig(fn string) []*cmsg.TopoItem {
	var newTopo []*cmsg.TopoItem
	f, err := os.Open(fn)
	if err != nil {
		log.Fatalf("Open file failed! %s", err)
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		token := strings.Fields(line)

		var compName cmsg.CompType
		if token[0] == "S" {
			compName = cmsg.CompType_SPOUT
		} else if token[0] == "B" {
			compName = cmsg.CompType_BOLT
		} else {
			log.Fatal("Component name only accepts S or B!")
		}

		// Add grouping, leave ip and port field empty
		// Master will assign those fields later
		var grouping []*cmsg.Component
		for _, gname := range token[3:] {
			grouping = append(
				grouping, &cmsg.Component{ItemName: gname})
		}

		topoEle := cmsg.TopoItem{
			ComponentType: compName,
			Myself:        &cmsg.Component{ItemName: token[1]},
			FuncName:      token[2],
			Grouping:      grouping,
		}
		newTopo = append(newTopo, &topoEle)
	}
	return newTopo
}

func main() {
	topoConf := os.Args[1]
	jobConf := os.Args[2]

	masterNode := flag.String(
		"m", "fa18-cs425-g39-06.cs.illinois.edu", "...")
	masterPort := flag.Int("masterPort", 9000, "...")
	sdfsPort := flag.Int("SDFSPort", 8080, "...")
	jobBinName := flag.String("JobBinaryName", "jobs.so", "...")
	flag.Parse()

	// Parse out .topo file into cmsg.Topology struct
	newTopo := parseTopoConfig(topoConf)
	// Upload .so job definition to SDFS
	sclnt := snode.SDFSClient{}
	sclnt.CreateRpcClient(*sdfsPort)
	sclnt.HandleCommand(fmt.Sprintf("put %s %s", jobConf, *jobBinName))

	// Make an RPC request to master with the topology
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithInsecure(), grpc.WithBlock(),
		grpc.WithTimeout(2*time.Second))
	conn, err := grpc.Dial(fmt.Sprintf("%s:%d", *masterNode, *masterPort),
		opts...)
	if err != nil {
		log.Panicf("[Client] Error connecting to Master! %s", err)
	}

	defer conn.Close()

	clnt := cmsg.NewCraneClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()

	topo := &cmsg.Topology{
		JobBinName: *jobBinName,
		Components: newTopo,
	}

	log.Print(topo)
	stream, err := clnt.UploadTopo(ctx, topo)

	if err != nil {
		log.Fatalf("[UploadTopo] Unable to request Master. %s", err)
	} else {
		for {
			res, err := stream.Recv()
			if err == io.EOF {
				log.Printf("[UploadTopo] Request finished.")
				break
			}

			if err != nil {
				log.Printf("[UploadTopo] Master return err: %s",
					err)
			}
			log.Print(res.RetMessage)
		}
	}
}
