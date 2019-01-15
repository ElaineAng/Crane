package CraneNode

import (
	snode "SDFS/SDFSNode"
	"fmt"
	"log"
	"plugin"
)

// Worker executes func of components, and handles communication between them
// It starts with a RPC server, listen to requests from Master
// Each request is expected to contain info about one component
// Worker should:
//  1. starts a new goroutine (task) for each component received
//  2. setup RPC client or server for data flow based on the grouping

// One worker could have multiple components, each with type Spout or Bolt
// Function expected in Spout:
//  - func NextTuple() CraneTuple {}
//    * Notice that each emitted tuple should be associated with an message id
// Function expected in Bolt:
//  - func Execute(CraneTuple tp) CraneTuple {}
// Worker lookup and call these functions defined by client in .so files

func (w *CraneWorker) GetJobBinary(bname string, ljp string) *plugin.Plugin {
	sclnt := snode.SDFSClient{}
	sclnt.CreateRpcClient(SDFSPort)
	sclnt.HandleCommand(fmt.Sprintf("get %s %s", bname, ljp))

	log.Printf("[Task] Loading plugin from %s", ljp)
	p, err := plugin.Open(ljp)
	if err != nil {
		log.Fatalf("[Task] Cannot open job binary! %s", err)
	}
	return p

}
