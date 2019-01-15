package SDFSNode

import (
	smsg "SDFS/SDFSMessage"
	"context"
	"log"
	"strconv"
	"strings"
	"time"
)

type SDFSClient struct {
	rpcClient smsg.SdfsClient
}

func (c SDFSClient) HandleCommand(command string) {
	command = strings.TrimSuffix(command, "\n")
	cmd_list := strings.Fields(command)
	switch cmd_list[0] {
	case "put":
		c.PutFile(cmd_list)
	case "get":
		c.GetFile(cmd_list)
	case "delete":
		c.DeleteFile(cmd_list)
	case "ls":
		c.ListFileLocation(cmd_list)
	case "store":
		c.ListLocalStore()
	case "get-versions":
		c.GetVersions(cmd_list)
	default:
		log.Printf("%s is not a valid command.\n", cmd_list[0])
	}
}

func (c *SDFSClient) CreateRpcClient(port int) {
	c.rpcClient, _ = CreateRpcClient("127.0.0.1", port, time.Second)
}

// put localfilename sdfsfilename
func (c SDFSClient) PutFile(cmd_list []string) {
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()

	req := &smsg.MetadataOpRequest{
		Op:            smsg.OpType_PUT,
		SdfsFilename:  cmd_list[2],
		LocalFilename: cmd_list[1],
	}
	_, err := c.rpcClient.HandleLocalClient(ctx, req)
	if err != nil {
		log.Fatalf("[Local C to S] put file failed. %s", err)
	}
}

// get sdfsfilename localfilename
func (c SDFSClient) GetFile(cmd_list []string) {
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()

	req := &smsg.MetadataOpRequest{
		Op:            smsg.OpType_GET,
		SdfsFilename:  cmd_list[1],
		LocalFilename: cmd_list[2],
	}
	_, err := c.rpcClient.HandleLocalClient(ctx, req)
	if err != nil {
		log.Fatalf("[Local C to S] get file failed. %s", err)
	}

}

// delete sdfsfilename
func (c SDFSClient) DeleteFile(cmd_list []string) {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Second)
	defer cancel()

	req := &smsg.MetadataOpRequest{
		Op:           smsg.OpType_DELETE,
		SdfsFilename: cmd_list[1],
	}
	_, err := c.rpcClient.HandleLocalClient(ctx, req)
	if err != nil {
		log.Fatalf("[Local C to S] delete file failed. %s", err)
	}
}

// ls sdfsfilename
func (c SDFSClient) ListFileLocation(cmd_list []string) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	req := &smsg.MetadataOpRequest{
		Op:           smsg.OpType_LS,
		SdfsFilename: cmd_list[1],
	}
	_, err := c.rpcClient.HandleLocalClient(ctx, req)
	if err != nil {
		log.Fatalf("[Local C to S] ls file failed. %s", err)
	}
}

// store
func (c SDFSClient) ListLocalStore() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	req := &smsg.MetadataOpRequest{
		Op: smsg.OpType_STORE,
	}
	_, err := c.rpcClient.HandleLocalClient(ctx, req)
	if err != nil {
		log.Fatalf("[Local C to S] store failed. %s", err)
	}

}

// get-versions sdfsfilename num-versions localfilename
func (c SDFSClient) GetVersions(cmd_list []string) {
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()
	version, _ := strconv.ParseInt(cmd_list[2], 10, 64)
	req := &smsg.MetadataOpRequest{
		Op:            smsg.OpType_GET_VERSIONS,
		SdfsFilename:  cmd_list[1],
		LocalFilename: cmd_list[3],
		Versions:      int64(version),
	}
	_, err := c.rpcClient.HandleLocalClient(ctx, req)
	if err != nil {
		log.Fatalf("[Local C to S] get versions failed. %s", err)
	}


}
