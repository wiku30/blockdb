package main

// Test 1: return value check and invalid transaction rejection

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"os/exec"
	"sync"
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

func check(err error, message string) {
	if err != nil {
		cmd.Process.Kill()
		log.Fatalf("Test RuntimeError:"+message+": %v", err)
	}
}
func assert(exp int32, val int32, message string) {
	if val != exp {
		cmd.Process.Kill()
		log.Fatalf("Test Failed: Expecting %s=%d, Got %d", message, exp, val)
	}
}
func assertTrue(b bool, message string) {
	if !b {
		cmd.Process.Kill()
		log.Fatalf("Test Failed: expect %s to be true", message)
	}
}

func id(i int) string {
	return fmt.Sprintf("T1U%05d", i)
}

var cmd *exec.Cmd

func main() {
	// start server
	cmd = exec.Command("./start.sh")
	err := cmd.Start()
	check(err, "start.sh")
	//wait for a while
	time.Sleep(time.Second * 2)

	// Set up a connection to the server.
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	check(err, "Connect to server")
	c := pb.NewBlockDatabaseClient(conn)
	defer conn.Close()

	ctx := context.Background()

	r2, err := c.Get(ctx, &pb.GetRequest{UserID: "NONEXIST"})
	check(err, "Verify new account")
	assert(int32(0), r2.Value, "account default balance")
	//Default value for a new account is expected to be 0.
	fmt.Println("Sanity check succeeded.")

	// wait group
	var wg sync.WaitGroup
	fmt.Println("Part 1: Setup some accounts with value 100")

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			uid := id(i)
			r, err := c.Put(ctx, &pb.Request{UserID: uid, Value: int32(100)})
			check(err, "Setup the new account")
			assertTrue(r.Success, "Setup PUT return")

			r2, err := c.Get(ctx, &pb.GetRequest{UserID: uid})
			check(err, "Verify the new account")
			assert(int32(100), r2.Value, "New account balance")

			wg.Done()
		}(i)
	}
	wg.Wait()
	fmt.Println("Part 1 Success.")

	fmt.Println("Part 2: Test basic account operations")
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			// Sleep random, Deposit 20, Withdraw 60*2, check balance
			time.Sleep(time.Millisecond * time.Duration(rand.Intn(10)))

			uid := id(i)
			r, err := c.Deposit(ctx, &pb.Request{UserID: uid, Value: int32(10)})
			check(err, "Deposit")
			assertTrue(r.Success, "Deposit return")

			r, err = c.Withdraw(ctx, &pb.Request{UserID: uid, Value: int32(60)})
			check(err, "Withdraw")
			assertTrue(r.Success, "Withdraw#1 return")

			r, err = c.Withdraw(ctx, &pb.Request{UserID: uid, Value: int32(60)})
			check(err, "Withdraw")
			assertTrue(!r.Success, "Withdraw#2 return==false")

			r2, err := c.Get(ctx, &pb.GetRequest{UserID: uid})
			check(err, "Verify account balance")
			assert(int32(100+10-60), r2.Value, "Balance after failed deposit")

			wg.Done()
		}(i)
	}
	wg.Wait()
	fmt.Println("Part 2 Success.")

	fmt.Println("Part 3: Check simultaneous connection and atomicity")

	var conns = make([]*grpc.ClientConn, 23)
	var clients = make([]pb.BlockDatabaseClient, 23)
	for i := range conns {
		conns[i], err = grpc.Dial(address, grpc.WithInsecure())
		check(err, "Establish simultaneous connection")
		clients[i] = pb.NewBlockDatabaseClient(conns[i])
	}
	defer func() {
		for i := range conns {
			conns[i].Close()
		}
	}()

	clients[3].Put(ctx, &pb.Request{UserID: id(333), Value: int32(0)})

	for i := 0; i < 250; i++ {
		wg.Add(1)
		go func(i int) {
			// Sleep random, transfer out, into acct#333 (no money)
			// 5 times for each account in [0-50]; exactly 2 times should succeed
			time.Sleep(time.Millisecond * time.Duration(rand.Intn(10)))

			uid := id(i % 50)
			dest := id(333)
			c := clients[i%len(clients)]

			_, err := c.Transfer(ctx, &pb.TransferRequest{FromID: uid, ToID: dest, Value: int32(20)})
			check(err, "Transfer")
			wg.Done()
		}(i)
	}
	wg.Wait()
	// Remaining: 50-20-20=10
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			uid := id(i)
			c := clients[i%len(clients)]

			r, err := c.Get(ctx, &pb.GetRequest{UserID: uid})
			check(err, "Get")
			assert(int32(50-20-20), r.Value, "Balance after 5 transfers, 2 success/3 fail.")

			wg.Done()
		}(i)
	}
	wg.Wait()
	fmt.Println("Part 3 Success.")

	fmt.Println("Part 4: Check concurrent withdrawals")
	results := make(chan bool, 100)

	// Balance in 333: 50*20*2=2000
	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go func(i int) {
			uid := id(333)
			c := clients[rand.Intn(999)%len(clients)]

			r, err := c.Withdraw(ctx, &pb.Request{UserID: uid, Value: 33})
			check(err, "Withdraw")
			results <- r.Success
			wg.Done()
		}(i)
	}
	go func() {
		wg.Wait()
		close(results)
	}()

	var sum int32
	for i := range results {
		if i {
			sum += 1
		}
	}
	assert(2000/33, sum, "Total amount successfully withdrawn from a large account.")
	fmt.Println("Part 4 Success.")

	// Kill
	err = cmd.Process.Kill()
	check(err, "Finished, kill server")

	fmt.Println("Test 1 Passed.")
}
