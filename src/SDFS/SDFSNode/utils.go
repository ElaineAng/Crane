package SDFSNode

import (
	"fmt"
	"log"
	"time"

	"google.golang.org/grpc"
	smsg "SDFS/SDFSMessage"
)

func CreateRpcClient(ip string, port int, timeout time.Duration,
) (smsg.SdfsClient, *grpc.ClientConn) {
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithInsecure(), grpc.WithBlock(),
		grpc.WithTimeout(timeout))
	conn, err := grpc.Dial(fmt.Sprintf("%s:%d", ip,
		port), opts...)
	if err != nil {
		log.Panicf("Error connecting server %s! %v", ip, err)
	}
	return smsg.NewSdfsClient(conn), conn
}
