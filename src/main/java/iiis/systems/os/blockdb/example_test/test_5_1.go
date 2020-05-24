package main

// Test 5: Simple Block output check. Check on 51st op.

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"reflect"
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
var example_1_json = `{"BlockID":1,"PrevHash":"00000000","Transactions":[{"Type":"PUT","UserID":"TEST---0","Value":10},{"Type":"PUT","UserID":"TEST---1","Value":10},{"Type":"PUT","UserID":"TEST---2","Value":10},{"Type":"PUT","UserID":"TEST---3","Value":10},{"Type":"PUT","UserID":"TEST---4","Value":10},{"Type":"PUT","UserID":"TEST---5","Value":10},{"Type":"PUT","UserID":"TEST---6","Value":10},{"Type":"PUT","UserID":"TEST---7","Value":10},{"Type":"PUT","UserID":"TEST---8","Value":10},{"Type":"PUT","UserID":"TEST---9","Value":10},{"Type":"DEPOSIT","UserID":"TEST---0","Value":5},{"Type":"DEPOSIT","UserID":"TEST---1","Value":5},{"Type":"DEPOSIT","UserID":"TEST---2","Value":5},{"Type":"DEPOSIT","UserID":"TEST---3","Value":5},{"Type":"DEPOSIT","UserID":"TEST---4","Value":5},{"Type":"DEPOSIT","UserID":"TEST---5","Value":5},{"Type":"DEPOSIT","UserID":"TEST---6","Value":5},{"Type":"DEPOSIT","UserID":"TEST---7","Value":5},{"Type":"DEPOSIT","UserID":"TEST---8","Value":5},{"Type":"DEPOSIT","UserID":"TEST---9","Value":5},{"Type":"TRANSFER","FromID":"TEST---0","ToID":"TEST--TX","Value":10},{"Type":"TRANSFER","FromID":"TEST---1","ToID":"TEST--TX","Value":10},{"Type":"TRANSFER","FromID":"TEST---2","ToID":"TEST--TX","Value":10},{"Type":"TRANSFER","FromID":"TEST---3","ToID":"TEST--TX","Value":10},{"Type":"TRANSFER","FromID":"TEST---4","ToID":"TEST--TX","Value":10},{"Type":"TRANSFER","FromID":"TEST---5","ToID":"TEST--TX","Value":10},{"Type":"TRANSFER","FromID":"TEST---6","ToID":"TEST--TX","Value":10},{"Type":"TRANSFER","FromID":"TEST---7","ToID":"TEST--TX","Value":10},{"Type":"TRANSFER","FromID":"TEST---8","ToID":"TEST--TX","Value":10},{"Type":"TRANSFER","FromID":"TEST---9","ToID":"TEST--TX","Value":10},{"Type":"TRANSFER","FromID":"TEST--TX","ToID":"TEST---0","Value":5},{"Type":"TRANSFER","FromID":"TEST--TX","ToID":"TEST---1","Value":5},{"Type":"TRANSFER","FromID":"TEST--TX","ToID":"TEST---2","Value":5},{"Type":"TRANSFER","FromID":"TEST--TX","ToID":"TEST---3","Value":5},{"Type":"TRANSFER","FromID":"TEST--TX","ToID":"TEST---4","Value":5},{"Type":"TRANSFER","FromID":"TEST--TX","ToID":"TEST---5","Value":5},{"Type":"TRANSFER","FromID":"TEST--TX","ToID":"TEST---6","Value":5},{"Type":"TRANSFER","FromID":"TEST--TX","ToID":"TEST---7","Value":5},{"Type":"TRANSFER","FromID":"TEST--TX","ToID":"TEST---8","Value":5},{"Type":"TRANSFER","FromID":"TEST--TX","ToID":"TEST---9","Value":5},{"Type":"WITHDRAW","UserID":"TEST---0","Value":5},{"Type":"WITHDRAW","UserID":"TEST---1","Value":5},{"Type":"WITHDRAW","UserID":"TEST---2","Value":5},{"Type":"WITHDRAW","UserID":"TEST---3","Value":5},{"Type":"WITHDRAW","UserID":"TEST---4","Value":5},{"Type":"WITHDRAW","UserID":"TEST---5","Value":5},{"Type":"WITHDRAW","UserID":"TEST---6","Value":5},{"Type":"WITHDRAW","UserID":"TEST---7","Value":5},{"Type":"WITHDRAW","UserID":"TEST---8","Value":5},{"Type":"WITHDRAW","UserID":"TEST---9","Value":5}],"Nonce":"00000000"}`

func prepareDir() {
	os.RemoveAll(dataDir)
	os.Mkdir(dataDir, 0777)
}

func id(i int) string {
	return fmt.Sprintf("TEST---%d", i)
}

const txID = "TEST--TX"

func jsonEqual(a []byte, b []byte) bool {
	var o1 interface{}
	var o2 interface{}

	var err error
	err = json.Unmarshal(a, &o1)
	if err != nil {
		fmt.Errorf("Error mashalling string 1 :: %s", err.Error())
		return false
	}
	err = json.Unmarshal(b, &o2)
	if err != nil {
		fmt.Errorf("Error mashalling string 2 :: %s", err.Error())
		return false
	}

	return reflect.DeepEqual(o1, o2)
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

	// quickly send all requests, the same as test_run.sh
	for i := 0; i <= 9; i++ {
		r, err:=c.Put(ctx, &pb.Request{UserID: id(i), Value: 10})
		if err!=nil || !r.Success{
			log.Printf("Request Error/Failed: %v", err); return false
		}
	}
	for i := 0; i <= 9; i++ {
		r, err:=c.Deposit(ctx, &pb.Request{UserID: id(i), Value: 5})
		if err!=nil || !r.Success{
			log.Printf("Request Error/Failed: %v", err); return false
		}
	}

	// One GET
	r, err:=c.Get(ctx, &pb.GetRequest{UserID: id(3)})
	if err!=nil || r.Value !=15{
		log.Printf("GET Request Error/Failed: %v", err); return false
	}
	// One invalid op
	r2, err:=c.Withdraw(ctx, &pb.Request{UserID: id(3), Value: 50})
	if err!=nil || r2.Success {
		log.Printf("Illegal WITHDRAW Error/Succeeded: %v",err); return false
	}

	for i := 0; i <= 9; i++ {
		r, err:=c.Transfer(ctx, &pb.TransferRequest{FromID: id(i), ToID: txID, Value: 10})
		if err!=nil || !r.Success{
			log.Printf("Request Error/Failed: %v", err); return false
		}
	}
	for i := 0; i <= 9; i++ {
		r, err:=c.Transfer(ctx, &pb.TransferRequest{FromID: txID, ToID: id(i), Value: 5})
		if err!=nil || !r.Success{
			log.Printf("Request Error/Failed: %v", err); return false
		}
	}
	for i := 0; i <= 9; i++ {
		r, err:=c.Withdraw(ctx, &pb.Request{UserID: id(i), Value: 5})
		if err!=nil || !r.Success{
			log.Printf("Request Error/Failed: %v", err); return false
		}
	}
	// overdraft
	for i := 0; i <= 9; i++ {
		c.Withdraw(ctx, &pb.Request{UserID: id(i), Value: 10})
	}
	// a dummy op
	c.Put(ctx, &pb.Request{UserID: id(233), Value: 1})

	// should have JSON now!
	fileStr, err := ioutil.ReadFile(dataDir + "1.json")
	if err != nil {
		fmt.Printf("Failed to open data block file? %v\n", err)
		return false
	}
	fmt.Println("Obtained JSON: ", string(fileStr))

	res:=jsonEqual([]byte(fileStr), []byte(example_1_json))
	if !res{
		log.Println("JSON missing/mismatch.")
	}
	return res
}

func main() {
	result := testrun()
	if !result {
		os.Exit(-1)
	}
	log.Println("Test 5_1 Passed.")
}
