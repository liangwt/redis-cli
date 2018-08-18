package main

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"github.com/liangwt/redis-cli"
	"log"
	"os"
)

var host string
var port string

func init() {
	flag.StringVar(&host, "h", "localhost", "hsot")
	flag.StringVar(&port, "p", "6379", "port")
}

func main() {
	flag.Parse()

	client := rclient.NewClient(host, port)
	err := client.Connect()

	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	for {
		fmt.Printf("%s:%s>", host, port)

		bio := bufio.NewReader(os.Stdin)
		input, _, err := bio.ReadLine()
		if err != nil {
			log.Fatal(err)
		}

		fields := bytes.Fields(input)
		_, err = client.DoRequest(fields[0], fields[1:]...)
		if err != nil {
			log.Fatal(err)
		}

		reply, err := client.GetReply()
		if err != nil {
			log.Fatal(err)
		}

		for _, s := range reply.Format() {
			fmt.Println(s)
		}

	}

}
