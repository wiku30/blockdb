package main

// Test 5_2: Block Generation: just give 100 txs, and wait

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
var block_2_json = `{"BlockID":2,"PrevHash":"00000000","Transactions":[{"Type":"TRANSFER","FromID":"ACCT0010","ToID":"ACCT0030","Value":10},{"Type":"TRANSFER","FromID":"ACCT0011","ToID":"ACCT0031","Value":10},{"Type":"TRANSFER","FromID":"ACCT0012","ToID":"ACCT0032","Value":10},{"Type":"TRANSFER","FromID":"ACCT0013","ToID":"ACCT0033","Value":10},{"Type":"TRANSFER","FromID":"ACCT0014","ToID":"ACCT0034","Value":10},{"Type":"TRANSFER","FromID":"ACCT0015","ToID":"ACCT0035","Value":10},{"Type":"TRANSFER","FromID":"ACCT0016","ToID":"ACCT0036","Value":10},{"Type":"TRANSFER","FromID":"ACCT0017","ToID":"ACCT0037","Value":10},{"Type":"TRANSFER","FromID":"ACCT0018","ToID":"ACCT0038","Value":10},{"Type":"TRANSFER","FromID":"ACCT0019","ToID":"ACCT0039","Value":10},{"Type":"WITHDRAW","UserID":"ACCT0000","Value":5},{"Type":"WITHDRAW","UserID":"ACCT0001","Value":5},{"Type":"WITHDRAW","UserID":"ACCT0002","Value":5},{"Type":"WITHDRAW","UserID":"ACCT0003","Value":5},{"Type":"WITHDRAW","UserID":"ACCT0004","Value":5},{"Type":"WITHDRAW","UserID":"ACCT0005","Value":5},{"Type":"WITHDRAW","UserID":"ACCT0006","Value":5},{"Type":"WITHDRAW","UserID":"ACCT0007","Value":5},{"Type":"WITHDRAW","UserID":"ACCT0008","Value":5},{"Type":"WITHDRAW","UserID":"ACCT0009","Value":5},{"Type":"WITHDRAW","UserID":"ACCT0010","Value":5},{"Type":"WITHDRAW","UserID":"ACCT0011","Value":5},{"Type":"WITHDRAW","UserID":"ACCT0012","Value":5},{"Type":"WITHDRAW","UserID":"ACCT0013","Value":5},{"Type":"WITHDRAW","UserID":"ACCT0014","Value":5},{"Type":"WITHDRAW","UserID":"ACCT0015","Value":5},{"Type":"WITHDRAW","UserID":"ACCT0016","Value":5},{"Type":"WITHDRAW","UserID":"ACCT0017","Value":5},{"Type":"WITHDRAW","UserID":"ACCT0018","Value":5},{"Type":"WITHDRAW","UserID":"ACCT0019","Value":5},{"Type":"PUT","UserID":"ACCT0000","Value":5},{"Type":"PUT","UserID":"ACCT0001","Value":5},{"Type":"PUT","UserID":"ACCT0002","Value":5},{"Type":"PUT","UserID":"ACCT0003","Value":5},{"Type":"PUT","UserID":"ACCT0004","Value":5},{"Type":"PUT","UserID":"ACCT0005","Value":5},{"Type":"PUT","UserID":"ACCT0006","Value":5},{"Type":"PUT","UserID":"ACCT0007","Value":5},{"Type":"PUT","UserID":"ACCT0008","Value":5},{"Type":"PUT","UserID":"ACCT0009","Value":5},{"Type":"PUT","UserID":"ACCT0010","Value":5},{"Type":"PUT","UserID":"ACCT0011","Value":5},{"Type":"PUT","UserID":"ACCT0012","Value":5},{"Type":"PUT","UserID":"ACCT0013","Value":5},{"Type":"PUT","UserID":"ACCT0014","Value":5},{"Type":"PUT","UserID":"ACCT0015","Value":5},{"Type":"PUT","UserID":"ACCT0016","Value":5},{"Type":"PUT","UserID":"ACCT0017","Value":5},{"Type":"PUT","UserID":"ACCT0018","Value":5},{"Type":"PUT","UserID":"ACCT0019","Value":5}],"Nonce":"00000000"}`

func prepareDir() {
	os.RemoveAll(dataDir)
	os.Mkdir(dataDir, 0777)
}

func id(i int) string {
	return fmt.Sprintf("ACCT%04d", i)
}

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

	// Send 100 requests.
	for i := 0; i <= 19; i++ {
		r, err:=c.Put(ctx, &pb.Request{UserID: id(i), Value: 20})
		if err!=nil || !r.Success{
			log.Printf("Request Error/Failed: %v", err); return false
		}
	}

	for i := 20; i <= 39; i++ {
		r, err:=c.Deposit(ctx, &pb.Request{UserID: id(i), Value: 20})
		if err!=nil || !r.Success{
			log.Printf("Request Error/Failed: %v", err); return false
		}
	}

	for i := 0; i <= 19; i++ {
		r, err:=c.Transfer(ctx, &pb.TransferRequest{FromID: id(i), ToID: id(i+20), Value: 10})
		if err!=nil || !r.Success{
			log.Printf("Request Error/Failed: %v", err); return false
		}
	}

	for i := 0; i <= 19; i++ {
		r, err:=c.Withdraw(ctx, &pb.Request{UserID: id(i), Value: 5})
		if err!=nil || !r.Success{
			log.Printf("Request Error/Failed: %v", err); return false
		}
	}

	for i := 0; i <= 19; i++ {
		r, err:=c.Put(ctx, &pb.Request{UserID: id(i), Value: 5})
		if err!=nil || !r.Success{
			log.Printf("Request Error/Failed: %v", err); return false
		}
	}

	// Sleep for 500ms
	time.Sleep(time.Millisecond * 500)

	// should have JSON now!
	fileStr, err := ioutil.ReadFile(dataDir + "2.json")
	if err != nil {
		fmt.Printf("Failed to open data block file? %v\n", err)
		return false
	}
	fmt.Println("Obtained JSON: ", string(fileStr))

	res:=jsonEqual([]byte(fileStr), []byte(block_2_json))
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
	log.Println("Test 5_2 Passed.")
}
