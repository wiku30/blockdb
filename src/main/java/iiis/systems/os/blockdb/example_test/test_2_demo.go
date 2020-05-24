package main

// Test 2: simple Kill-and-Restore test

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

var dataDir = "/tmp/test_2/"
var address = "127.0.0.1:50051"
var config = `
{
	"1":{
		"ip":"127.0.0.1",
		"port":"50051",
		"dataDir":"/tmp/test_2/"
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

const criticalID = "CRITICAL"

func test_recovery(paddingOps int) bool {
	// Reset Env, start experiment
	prepareDir()
	// Start server
	cmd := exec.Command("./start.sh")
	err := cmd.Start()
	defer cmd.Process.Kill()
	if err != nil {
		log.Printf("./start.sh returned error: %v", err)
		return false
	}
	// wait for a while? 
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

	log.Println("Adding critical operation at op#", paddingOps+1)
	r, err := c.Deposit(ctx, &pb.Request{UserID: criticalID, Value: 10})
	// Kill immediately after it returns.
	cmd.Process.Kill()
	if err != nil || !r.Success {
		log.Printf("Critical DEPOSIT request failed? %v", err)
		return false
	}

	time.Sleep(2 * time.Millisecond)
	log.Println("Recovery...")

	// Recover data
	cmd2 := exec.Command("./start.sh")
	err = cmd2.Start()
	defer cmd2.Process.Kill()
	if err != nil {
		log.Printf("./start.sh returned error: %v", err)
		return false
	}
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

	r2, err := c2.Get(ctx, &pb.GetRequest{UserID: criticalID})
	if err != nil {
		log.Printf("GET failed after crash-recovery: %v",err)
		return false
	}
	if r2.Value != 10 {
		log.Printf("GET returned incorrect value:%d, recovery of critical operation did not succeed?", r2.Value)
		return false
	}
	cmd2.Process.Kill()
	return true
}

func main() {
	fmt.Println("Writing config JSON...", config)
	ioutil.WriteFile("./config.json", []byte(config), 0644)

	for _, i := range []int{5} {//can be extended here, this is just a demo
		resp := test_recovery(i)
		if !resp {
			os.Exit(-1)
		}
	}
}
