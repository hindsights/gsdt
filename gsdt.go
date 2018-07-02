package main

import (
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
)

const (
	defaultIp     = ""
	defaultPort   = 13333
	versionString = "1.0.2"
)

func printVersion() {
	fmt.Printf("gsdt version: %s\n", versionString)
}

func PrintServerUsage() {
	fmt.Println("gsdt server [PORT]")
}

func PrintClientUsage() {
	fmt.Println("gsdt client SERVER_IP[:PORT] FILENAME [CONNECTION_COUNT] [BLOCK_SIZE]")
}

func ParseClientParams(args []string) *ClientParams {
	fmt.Printf("run client test program\n")
	if len(args) <= 1 {
		fmt.Printf("no file specified\n")
		PrintClientUsage()
		return nil
	}
	params := NewClientParams()
	params.path = args[1]
	var err error
	if len(args) > 2 {
		params.clientCount, err = strconv.Atoi(args[2])
	}
	if len(args) > 3 {
		params.blockSize, err = strconv.Atoi(args[3])
	}
	if err != nil {
		fmt.Printf("invalid argument: %s\n", args)
		PrintClientUsage()
		return nil
	}
	params.address = args[0]
	if !strings.Contains(params.address, ":") {
		params.address = params.address + ":" + strconv.FormatInt(defaultPort, 10)
	}
	return params
}

func ClientMain() {
	fmt.Printf("run client test program\n")
	params := ParseClientParams(os.Args[2:])
	if params == nil {
		fmt.Printf("invalid argument: %s\n", os.Args)
		PrintClientUsage()
		return
	}
	client := NewClient()
	client.Run(params)
}

func ParseServerParams(args []string) *ServerParams {
	fmt.Printf("run server test program:%d\n", len(args))
	params := NewServerParams()
	params.ip = defaultIp
	params.port = defaultPort
	if len(args) > 0 {
		var err error
		params.port, err = strconv.Atoi(args[0])
		if err != nil || params.port <= 0 {
			fmt.Printf("invalid port number:%v %v\n", args[0], params.port)
			PrintServerUsage()
			return nil
		}
	}
	return params
}

func ServerMain() {
	fmt.Printf("run server test program:%d\n", len(os.Args))
	params := ParseServerParams(os.Args[2:])
	if params == nil {
		return
	}
	server := NewServer()
	server.Run(params)
}

func main() {
	printVersion()
	defaultMaxProcs := runtime.GOMAXPROCS(1)
	fmt.Printf("gstd default max procs=%v\n", defaultMaxProcs)
	if len(os.Args) < 2 {
		fmt.Println("no argument")
		fmt.Println("usage:")
		PrintServerUsage()
		PrintClientUsage()
		return
	}
	var cmd = os.Args[1]
	switch cmd {
	case "client":
		ClientMain()
	case "server":
		ServerMain()
	default:
		fmt.Printf("invalid command: %s\n", cmd)
	}
}
