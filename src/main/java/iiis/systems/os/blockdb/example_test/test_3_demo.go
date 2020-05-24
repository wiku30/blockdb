package main

// Test 3: Extended test 2 - kill after different time delays.

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"time"
	pb "blockdb_go/protobuf/go"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

var dataDir = "/tmp/test_3/"
var address = "127.0.0.1:50051"
var config = `
{
	"1":{
		"ip":"127.0.0.1",
		"port":"50051",
		"dataDir":"/tmp/test_3/"
	},
	"nservers":1
}
`

func prepareDir() {
	os.RemoveAll(dataDir)
	os.Mkdir(dataDir, 0777)
}

func id(i int) string {
	return fmt.Sprintf("T2U%05d", i)
}

const criticalFrom = "CRITI001"
const criticalTo = "CRITI002"

func test_recovery(paddingOps int, sleepMS int) bool {
	// Reset Env, start experiment
	prepareDir()
	// Start server
	cmd := exec.Command("./start.sh")
	err := cmd.Start()
	if err != nil {
		log.Printf("./start.sh returned error: %v", err)
		return false
	}
	defer cmd.Process.Kill()

	// wait for a while
	time.Sleep(time.Second * 2)

	// gRPC setup
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		log.Printf("Failed to establish connection to server (1s after startup)")
		return false
	}
	c := pb.NewBlockDatabaseClient(conn)
	defer conn.Close()

	ctx := context.Background()

	// Check To account, !=0 -> not a clean start.
	r0, err := c.Get(ctx, &pb.GetRequest{UserID: criticalTo})
	if err != nil {
		log.Printf("Error during initial GET")
		return false
	} else if r0.Value != 0 {
		log.Printf("Fatal error: not a clean start, please verify if the instance is killed correctly! Value=%d", r0.Value)
		return false
	}

	log.Printf("Test 3 ready: #%d op, kill after %d ms...\n", paddingOps+2, sleepMS)

	// some padding PUT ops (to non-interesting account)
	for i := 0; i < paddingOps; i++ {
		r, err := c.Put(ctx, &pb.Request{UserID: id(i % 5), Value: 10})
		if err != nil {
			log.Printf("Error during padding request? %v", err)
			return false
		}
		if !r.Success {
			log.Printf("Padding PUT request Failed?")
			return false
		}
	}

	// Prepare the FROM account
	r, err := c.Deposit(ctx, &pb.Request{UserID: criticalFrom, Value: 100})
	if err != nil || !r.Success {
		log.Printf("Error during padding Deposit request? %v", err)
		return false
	}

	// Simultaneously start request and sleep counter.
	ch := time.After(time.Millisecond * time.Duration(sleepMS))
	go func() {
		_, _ = c.Transfer(ctx, &pb.TransferRequest{FromID: criticalFrom, ToID: criticalTo, Value: 50})
	}()

	_ = <-ch
	// Kill after sleepMS
	cmd.Process.Kill()

	time.Sleep(1 * time.Millisecond)
	log.Println("Recovery...")

	// Recover data
	cmd2 := exec.Command("./start.sh")
	err = cmd2.Start()
	if err != nil {
		log.Printf("./start.sh returned error: %v", err)
		return false
	}
	defer cmd2.Process.Kill()

	// wait for data to be recovered
	time.Sleep(time.Millisecond * 3000)

	// gRPC setup
	conn2, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		log.Printf("Failed to establish connection to server (1s after startup)")
		return false
	}
	c2 := pb.NewBlockDatabaseClient(conn2)
	defer conn2.Close()

	r2, err := c2.Get(ctx, &pb.GetRequest{UserID: criticalFrom})
	if err != nil {
		log.Printf("GET failed after crash-recovery")
		return false
	}
	fromVal := r2.Value

	r2, err = c2.Get(ctx, &pb.GetRequest{UserID: criticalTo})
	if err != nil {
		log.Printf("GET failed after crash-recovery")
		return false
	}
	toVal := r2.Value
	if fromVal == 100 && toVal == 0 {
		log.Printf("Case %d %d: Transaction rejected. Good!", paddingOps, sleepMS)
	} else if fromVal == 50 && toVal == 50 {
		log.Printf("Case %d %d: Transaction commited. Good!", paddingOps, sleepMS)
	} else {
		log.Printf("Case %d %d: Weird error? %d %d", paddingOps, sleepMS, fromVal, toVal)
		return false
	}

	// DB should not fall into readonly mode:
	r3, err := c2.Put(ctx, &pb.Request{UserID: criticalTo, Value: 9999})
	if err != nil || !r3.Success {
		log.Printf("PUT failed after recovery. Readonly mode?")
		return false
	}

	cmd2.Process.Kill()
	return true
}

func main() {
	fmt.Println("Writing config JSON...", config)
	ioutil.WriteFile("./config.json", []byte(config), 0644)

	for _, i := range []int{5} {//can be extended here, this is just a demo
		for _, t := range []int{20} {//can be extended here, this is just a demo
			resp := test_recovery(i, t)
			if !resp {
				os.Exit(-1)
			}
			resp = test_recovery(i, t)
			if !resp {
				os.Exit(-1)
			}
			//try everything twice to remove randomness
		}
	}

}
