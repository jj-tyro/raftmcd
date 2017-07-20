package main

import (
	"fmt"
	"os"
	"syscall"

	"time"

	"github.com/dustin/gomemcached/client"
)

func main() {
	var cli *memcached.Client
	var err error

	if cli, err = memcached.Connect("tcp", "127.0.0.1:11201"); err != nil {
		fmt.Println(err)
		os.Exit(-1)
	}
	defer cli.Close()

	cli.Set(0, "test_key", 0, 0, []byte(time.Now().String()))

	resp, err := cli.Get(0, "test_key")

	if err == nil {
		fmt.Printf("Get result! retcode=%v value=%v\n", resp.Status, string(resp.Body))
	} else {
		fmt.Printf("Get Failed! err=%v %v\n", err, err == syscall.EPIPE)
	}

	//respMulti *map[string]*gomemcached.MCResponse

	respMulti, err2 := cli.GetBulk(0, []string{"a", "test_key"})

	if err2 == nil {
		fmt.Printf("Get result! retcode=%v value=%v\n", resp.Status, string(resp.Body))
		for k, v := range respMulti {
			fmt.Printf("Get result! key=%s retcode=%v value=%v\n", k, v.Status, string(v.Body))
		}
	} else {
		fmt.Printf("Get Failed! err=%v \n", err2)
	}
}
