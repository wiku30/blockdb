package main

// Test 5_3: Check loglength

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

var dataDir = "/tmp/test_5/"
var address = "127.0.0.1:50051"
var config = `
{
	"1":{
		"ip":"127.0.0.1",
		"port":"50051",
		"dataDir":"/tmp/test_5/"
	},
	"nservers":1
}
`

func prepareDir() {
	os.RemoveAll(dataDir)
	os.Mkdir(dataDir, 0777)
}

func id(i int) string {
	return fmt.Sprintf("ACCT%04d", i)
}

func testrun() bool {
	// JSON config
	fmt.Println("Writing config JSON...", config)
	ioutil.WriteFile("./config.json", []byte(config), 0644)

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

	// Send 1002 good+bad requests.
	r, err:=c.Put(ctx, &pb.Request{UserID: id(0), Value: 20})
	if err!=nil || !r.Success{
		log.Printf("Request Error/Failed: %v", err); return false
	}
	for i := 0; i <= 1001; i++ {
		if i%2==0{
			r, err=c.Deposit(ctx, &pb.Request{UserID: id(i%5), Value: 20})
		}else{
			r, err=c.Withdraw(ctx, &pb.Request{UserID: id(i%5), Value: 3000})
		}
		if err!=nil{
			log.Printf("Request Error/Failed: %v", err); return false
		}
	}

	rl, err:= c.LogLength(context.Background(), &pb.Null{})
	if err!=nil{
		log.Printf("LogLength Error/Failed: %v", err); return false
	}
	log.Println("LogLength:",rl.Value)
	return rl.Value<75
}

func main() {
	result := testrun()
	if !result {
		os.Exit(-1)
	}
	log.Println("Test 5_3 Passed.")
}
