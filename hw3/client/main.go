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
//	go run ./client put foo bar localhost:17000
//	go run ./client get foo localhost:17001
//	go run ./client primary localhost:17002
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

	pb "kvraft/proto"

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

// doPut sends a single Put RPC to addr and returns the response.
func doPut(key, value, addr string) (*pb.PutResponse, error) {
	conn, err := dial(addr)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", addr, err)
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	return pb.NewKVStoreClient(conn).Put(ctx, &pb.PutRequest{Key: key, Value: value})
}

// cmdPut sends a Put RPC to addr.
//
// Stage 1: Send Put, print result.
// Stage 5: If PutResponse.ok == false and redirect_addr is set, retry once at
//
//	redirect_addr. If that also fails, print an error and exit 1.
func cmdPut(key, value, addr string) {
	resp, err := doPut(key, value, addr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "put error: %v\n", err)
		os.Exit(1)
	}

	if !resp.Ok {
		if resp.RedirectAddr == "" {
			fmt.Fprintln(os.Stderr, "put failed: not primary and no redirect address available")
			os.Exit(1)
		}
		// Stage 5: retry at the primary.
		resp, err = doPut(key, value, resp.RedirectAddr)
		if err != nil {
			fmt.Fprintf(os.Stderr, "put error (after redirect): %v\n", err)
			os.Exit(1)
		}
		if !resp.Ok {
			fmt.Fprintln(os.Stderr, "put failed after redirect")
			os.Exit(1)
		}
	}

	fmt.Println("OK")
}

// cmdGet sends a Get RPC to addr and prints the result.
func cmdGet(key, addr string) {
	conn, err := dial(addr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "dial error: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	resp, err := pb.NewKVStoreClient(conn).Get(ctx, &pb.GetRequest{Key: key})
	if err != nil {
		fmt.Fprintf(os.Stderr, "get error: %v\n", err)
		os.Exit(1)
	}

	if !resp.Found {
		fmt.Println("(not found)")
		return
	}
	fmt.Println(resp.Value)
}

// cmdPrimary calls GetPrimary on addr and prints the current primary's address.
func cmdPrimary(addr string) {
	conn, err := dial(addr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "dial error: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	resp, err := pb.NewKVStoreClient(conn).GetPrimary(ctx, &pb.Empty{})
	if err != nil {
		fmt.Fprintf(os.Stderr, "primary error: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("primary: node %d at %s\n", resp.PrimaryId, resp.PrimaryAddr)
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
}
