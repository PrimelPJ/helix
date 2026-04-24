// helixctl is a command-line client for a Helix cluster.
//
// Usage:
//
//	helixctl --addr http://localhost:8001 get <key>
//	helixctl --addr http://localhost:8001 put <key> <value>
//	helixctl --addr http://localhost:8001 del <key>
//	helixctl --addr http://localhost:8001 status
package main

import (
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
)

var addr = flag.String("addr", "http://localhost:8001", "helix node address")

func main() {
	flag.Parse()
	args := flag.Args()
	if len(args) == 0 {
		usage()
		os.Exit(1)
	}
	switch args[0] {
	case "get":
		if len(args) < 2 {
			die("get requires a key")
		}
		doGet(args[1])
	case "put":
		if len(args) < 3 {
			die("put requires a key and value")
		}
		doPut(args[1], args[2])
	case "del":
		if len(args) < 2 {
			die("del requires a key")
		}
		doDel(args[1])
	case "status":
		doStatus()
	default:
		usage()
		os.Exit(1)
	}
}

func doGet(key string) {
	resp, err := http.Get(fmt.Sprintf("%s/kv/%s", *addr, key))
	check(err)
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode == http.StatusNotFound {
		fmt.Fprintf(os.Stderr, "(not found)\n")
		os.Exit(1)
	}
	fmt.Println(string(body))
}

func doPut(key, value string) {
	req, _ := http.NewRequest(http.MethodPut,
		fmt.Sprintf("%s/kv/%s", *addr, key),
		strings.NewReader(value))
	resp, err := http.DefaultClient.Do(req)
	check(err)
	resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		fmt.Fprintf(os.Stderr, "unexpected status %s\n", resp.Status)
		os.Exit(1)
	}
	fmt.Printf("OK  index=%s\n", resp.Header.Get("X-Raft-Index"))
}

func doDel(key string) {
	req, _ := http.NewRequest(http.MethodDelete,
		fmt.Sprintf("%s/kv/%s", *addr, key), nil)
	resp, err := http.DefaultClient.Do(req)
	check(err)
	resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		fmt.Fprintf(os.Stderr, "unexpected status %s\n", resp.Status)
		os.Exit(1)
	}
	fmt.Println("OK")
}

func doStatus() {
	resp, err := http.Get(fmt.Sprintf("%s/status", *addr))
	check(err)
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	fmt.Println(string(body))
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: helixctl [--addr URL] <get|put|del|status> [args]")
}

func check(err error) {
	if err != nil {
		die(err.Error())
	}
}

func die(msg string) {
	fmt.Fprintln(os.Stderr, "error:", msg)
	os.Exit(1)
}
