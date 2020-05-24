package main

// Test 0: start and shutdown

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os/exec"
	"time"
	pb "blockdb_go/protobuf/go"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

var address = func() string {
	conf, err := ioutil.ReadFile("config.json")
	if err != nil {
		panic(err)
	}
	var dat map[string]interface{}
	err = json.Unmarshal(conf, &dat)
	if err != nil {
		panic(err)
	}
	dat = dat["1"].(map[string]interface{})
	return fmt.Sprintf("%s:%s", dat["ip"], dat["port"])
}()

func main() {
	// Step 0: compile
	_, _ = exec.Command("./compile.sh").Output()
	// No compile exit code checking

	// Step 1: start
	cmd := exec.Command("./start.sh")
	err := cmd.Start()
	if err != nil {
		log.Fatalf("./start.sh returned error: %v", err)
	}
	// wait for a while
	time.Sleep(time.Second * 2)

	// Set up a connection to the server.
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Failed to establish connection to server (1s after startup)")
	}
	c := pb.NewBlockDatabaseClient(conn)
	defer conn.Close()

	ctx := context.Background()


	r, err := c.Get(ctx, &pb.GetRequest{UserID: "NONEXIST"})
	if err != nil {
		log.Fatalf("Error requesting server (1s after startup)")
	}
	if r.Value != 0 {
		log.Fatalf("Value!=0. Not a clean start?")
	}

	// Step 2: kill process
	err = cmd.Process.Kill()
	if err != nil {
		log.Fatalf("Failed to kill process.")
	}

	time.Sleep(time.Millisecond * 1)

	r, err = c.Get(ctx, &pb.GetRequest{UserID: "NONEXIST"})
	if err == nil {
		log.Fatalf("Failed to kill process; server still responding.")
	}
	log.Println("Test 0 Passed.")
}
