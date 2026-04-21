// kvctl — command-line client for the kvstore replicated key-value store.
//
// Usage:
//
//	go run ./client put <key> <value> <server-addr>
//	go run ./client get <key> <server-addr>
//	go run ./client primary <server-addr>
//
// Examples:
//
//	go run ./client put foo bar localhost:7000
//	go run ./client get foo localhost:7001
//	go run ./client primary localhost:7002
//
// Stage 1: implement put, get, primary subcommands (no redirect handling).
// Stage 5: handle redirect_addr in PutResponse — retry Put at the returned address.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	pb "kvstore/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const defaultTimeout = 5 * time.Second

func usage() {
	fmt.Fprintf(os.Stderr, `Usage:
  kvctl put <key> <value> <server-addr>
  kvctl get <key> <server-addr>
  kvctl primary <server-addr>
`)
	os.Exit(1)
}

func dial(addr string) (*grpc.ClientConn, error) {
	return grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
}

// cmdPut sends a Put RPC to addr.
//
// Stage 1: Send Put, print result.
// Stage 5: If PutResponse.ok == false and redirect_addr is set, retry once at
//
//	redirect_addr. Print the result normally on success. If the retry also
//	fails (RPC error or ok=false again), print exactly:
//	  ERROR: could not reach primary after redirect
//	and exit with status 1.
func cmdPut(key, value, addr string) {
	// TODO (Stage 1): implement Put RPC call
	// TODO (Stage 5): handle redirect_addr
	fmt.Fprintln(os.Stderr, "TODO: implement put (Stage 1)")
	os.Exit(1)
}

// cmdGet sends a Get RPC to addr and prints the result.
func cmdGet(key, addr string) {
	// TODO (Stage 1): implement Get RPC call
	fmt.Fprintln(os.Stderr, "TODO: implement get (Stage 1)")
	os.Exit(1)
}

// cmdPrimary calls GetPrimary on addr and prints the current primary's address.
func cmdPrimary(addr string) {
	// TODO (Stage 1): implement GetPrimary RPC call
	fmt.Fprintln(os.Stderr, "TODO: implement primary (Stage 1)")
	os.Exit(1)
}

func main() {
	log.SetFlags(0) // no timestamps in client output
	if len(os.Args) < 2 {
		usage()
	}

	switch os.Args[1] {
	case "put":
		if len(os.Args) != 5 {
			fmt.Fprintln(os.Stderr, "put requires: <key> <value> <server-addr>")
			usage()
		}
		cmdPut(os.Args[2], os.Args[3], os.Args[4])
	case "get":
		if len(os.Args) != 4 {
			fmt.Fprintln(os.Stderr, "get requires: <key> <server-addr>")
			usage()
		}
		cmdGet(os.Args[2], os.Args[3])
	case "primary":
		if len(os.Args) != 3 {
			fmt.Fprintln(os.Stderr, "primary requires: <server-addr>")
			usage()
		}
		cmdPrimary(os.Args[2])
	default:
		fmt.Fprintf(os.Stderr, "unknown subcommand: %s\n", os.Args[1])
		usage()
	}

	// Suppress "declared but not used" errors on the helper until students implement.
	_ = context.Background
	_ = dial
	_ = pb.NewKVStoreClient
	_ = defaultTimeout
}
