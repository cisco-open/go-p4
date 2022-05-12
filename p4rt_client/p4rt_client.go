/*
 * ------------------------------------------------------------------
 * May, 2022, Reda Haddad
 *
 * Copyright (c) 2022 by cisco Systems, Inc.
 * All rights reserved.
 * ------------------------------------------------------------------
 */
package p4rt_client

import (
	"context"
        "fmt"
        grpc "google.golang.org/grpc"
        "log"
        p4_v1 "github.com/p4lang/p4runtime/go/p4/v1"
        "strconv"
)

type P4RTClientParams struct {
	Name       string
        ServerIP   string
        ServerPort int
}

type P4RTClient struct {
	params        *P4RTClientParams
        connection    *grpc.ClientConn
        p4rtClient    p4_v1.P4RuntimeClient
        streamId      uint32
}

func (p *P4RTClient) getAddress() string {
	address := fmt.Sprintf("%s:%s", p.params.ServerIP, strconv.Itoa(p.params.ServerPort))
        return address
}

func (p *P4RTClient) String() string {
	return fmt.Sprintf("%s(%s)", p.params.Name, p.getAddress())
}

func (p *P4RTClient) ServerConnect() error {
	log.Printf("%s Connecting to Server\n", p)
	conn, err := grpc.Dial(p.getAddress(), grpc.WithInsecure())
	if err != nil {
		log.Printf("%s ERROR Connecting to Server: %s\n", p, err);
                return err
	}
        p.connection = conn

	// Create a new P4RuntimeClient instance
	p.p4rtClient = p4_v1.NewP4RuntimeClient(conn)

	return nil
}

// Return unique handle for this stream
// 0 is not a valid id
func (p *P4RTClient) CreateStreamChannel() (uint32, error) {
	if (p.connection == nil) {
        	return 0, fmt.Errorf("Client Not connected")
        }

        // RPC and setup the stream
	stream, gerr := p.p4rtClient.StreamChannel(context.Background())
	if gerr != nil {
		log.Printf("%s ERROR StreamChannel: %s\n", p, gerr);
                return 0, gerr
	}
	
        p.streamId++
        // XXX Add Stream to map, indexed by p.streamId
        //     Map should be locked before add/remove
        //     Map should hold a struct holding channels and stream
        // XXX Init mutexes, channels, etc. to Handle Go routine races/locks
        // XXX Before we send to stream, we need to check if TX exited (lock and send?)
        // 

	// For ever read from stream
	go func() {
                for {
		        // XXX Check if we need to stop
                        event, stream_err := stream.Recv()
		        // XXX Check if we need to stop

                        if stream_err != nil {
			        log.Printf("Client Recv Error %v\n", stream_err)
			        break
		        }

		        // XXX Remove unecessary entries
		        switch event.Update.(type) {
		        case *p4_v1.StreamMessageResponse_Arbitration:
                        	// XXX Add to buffered channel
		        case *p4_v1.StreamMessageResponse_Packet:
                        	// XXX Add to buffered channel
		        case *p4_v1.StreamMessageResponse_Digest:
		        case *p4_v1.StreamMessageResponse_IdleTimeoutNotification:
		        case *p4_v1.StreamMessageResponse_Other:
		        case *p4_v1.StreamMessageResponse_Error:
		        default:
			        log.Printf("ERROR Received %s\n", event.String())
		        }
	        }

		// XXX Cleanup the stream, lock and remove from map
        }()

	return p.streamId, nil
}

// XXX remove streamId from map
func (p *P4RTClient) DestroyStreamChannel(streamId uint32) error {

	return nil
}

func (p *P4RTClient) ServerDisconnect() {
	log.Printf("%s Disconnecting from Server\n", p)
        p.connection.Close()
        p.connection = nil
}

//
// Creates and Initializes a P4RT client
//
func NewP4RTClient(params *P4RTClientParams) *P4RTClient {
	client := &P4RTClient{
        	params: params,
        }

	return client
}
