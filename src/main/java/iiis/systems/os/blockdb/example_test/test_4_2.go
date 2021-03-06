package main

// Test 4_2: Efficiency test_2: mostly overdraft, some valid withdrawals between 3 random accounts with 100 concurrent thread.

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os/exec"
	"log"
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

func id(i int) string {
	return fmt.Sprintf("T1U%05d", i)
}

func main() {
	// Set up server
	cmd := exec.Command("./start.sh")
	err := cmd.Start()
	if err != nil {
		log.Fatalf("Failed to start server.")
	}
	//wait for a while?
	time.Sleep(time.Second * 2)

	// Set up a connection to the server.
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		cmd.Process.Kill()
		log.Fatalf("Failed to establish connection to server.")
	}
	//c := pb.NewBlockDatabaseClient(conn)
	defer conn.Close()
	ctx := context.Background()

	// Prepare clients
	var M = 100
	var conns = make([]*grpc.ClientConn, M)
	var clients = make([]pb.BlockDatabaseClient, M)
	for i := range conns {
		conns[i], err = grpc.Dial(address, grpc.WithInsecure())
		clients[i] = pb.NewBlockDatabaseClient(conns[i])
	}
	defer func() {
		for i := range conns {
			conns[i].Close()
		}
	}()

	// Prepare accounts
	clients[3].Put(ctx, &pb.Request{UserID: id(1), Value: int32(20)})
	clients[3].Put(ctx, &pb.Request{UserID: id(2), Value: int32(20)})
	clients[3].Put(ctx, &pb.Request{UserID: id(0), Value: int32(20)})

	// Good to start!
	cnts := make(chan int)
	times := make(chan time.Duration)
	for i := 0; i < 100; i++ {
		go func(seed int) {
			begin := time.Now()
			cnt := 0
			for {
				cnt += 1
				UserID := id(rand.Intn(3) % 3)
				Value := int32(100)
				if rand.Intn(10) == 0 {
					Value = 1
				}
				clients[seed].Withdraw(ctx, &pb.Request{
					UserID: UserID,
					Value:  Value,
				})
				if time.Since(begin) > 1*time.Second {
					break
				}
			}
			cnts <- cnt
			times <- time.Since(begin)
		}(i)
	}

	sum_cnt := 0
	sum_time := time.Second * 0
	for i := 0; i < 100; i++ {
		cnt := <-cnts
		deltat := <-times
		//fmt.Println("Instance running:",cnt,deltat.Seconds())
		sum_cnt += cnt
		sum_time += deltat
	}

	avg_t := sum_time.Seconds() / 100.0
	iops := float64(sum_cnt) / avg_t
	log.Println("Test 4_2 Total IOPS:", iops)

	cmd.Process.Kill()

	if iops < 100 {
		log.Fatalf("Too slow (<100 iops), failed this test.")
	}
	log.Println("Test 4_2 Passed.")
	return
}
