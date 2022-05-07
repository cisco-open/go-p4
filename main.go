/*
 * ------------------------------------------------------------------
 * May, 2022, Reda Haddad
 *
 * Copyright (c) 2022 by cisco Systems, Inc.
 * All rights reserved.
 * ------------------------------------------------------------------
 */
package main

import (
	"context"
	"flag"
	"fmt"
	p4_v1 "github.com/p4lang/p4runtime/go/p4/v1"
	grpc "google.golang.org/grpc"
	"log"
        "os"
	"strconv"
	// 	codes "google.golang.org/grpc/codes"
	// 	status1 "google.golang.org/grpc/status"
)

// Globals
var (
	logger    *log.Logger
)

func main() {
	flag.Parse()
	initLogger()
	validateArgs()
	logger.Println("Called as:", os.Args)

	// Setup the connection with the server
	address := fmt.Sprintf("%s:%s", *serverIP, strconv.Itoa(*serverPort))
	logger.Printf("Connecting to %s\n", address)
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	// Create a new NewP4RuntimeClient instance
	client := p4_v1.NewP4RuntimeClient(conn)

	// RPC and setup the stream
	stream, gerr := client.StreamChannel(context.Background())
	if gerr != nil {
		log.Fatal(gerr)
	}

	// For ever read from stream
	for {
		event, stream_err := stream.Recv()
		if stream_err != nil {
			logger.Println("Client Recv Error %v", stream_err)
			break
		}

		// XXX Remove unecessary entries
		switch event.Update.(type) {
		case *p4_v1.StreamMessageResponse_Arbitration:
		case *p4_v1.StreamMessageResponse_Packet:
		case *p4_v1.StreamMessageResponse_Digest:
		case *p4_v1.StreamMessageResponse_IdleTimeoutNotification:
		case *p4_v1.StreamMessageResponse_Other:
		case *p4_v1.StreamMessageResponse_Error:
		default:
			logger.Printf("Received %s\n", event.String())
		}
	}
}
