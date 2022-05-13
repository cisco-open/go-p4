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
	p4_v1 "github.com/p4lang/p4runtime/go/p4/v1"
	grpc "google.golang.org/grpc"
	"log"
	"strconv"
	"sync"
)

type P4RTClientParams struct {
	Name       string
	ServerIP   string
	ServerPort int
}

type P4RTClientSession struct {
	streamId uint32
	stream   p4_v1.P4Runtime_StreamChannelClient

	stop_mu sync.Mutex // Protects the following:
	stop    bool
	// end stop_mu Protection

	// XXX Do we need to mutex protect these?
	ElectionId *p4_v1.Uint128
	DeviceId   uint64
}

func (p *P4RTClientSession) ShouldStop() bool {
	p.stop_mu.Lock()
	defer p.stop_mu.Unlock()
	return p.stop
}

func (p *P4RTClientSession) Stop() {
	p.stop_mu.Lock()
	defer p.stop_mu.Unlock()
	p.stop = true
}

type P4RTClient struct {
	params *P4RTClientParams

	client_mu  sync.Mutex // Protects the following:
	connection *grpc.ClientConn
	p4rtClient p4_v1.P4RuntimeClient
	streamId   uint32 // Global increasing number
	streams    map[uint32]*P4RTClientSession
	// end client_mu Protection
}

func (p *P4RTClient) getAddress() string {
	address := fmt.Sprintf("%s:%s", p.params.ServerIP, strconv.Itoa(p.params.ServerPort))
	return address
}

func (p *P4RTClient) String() string {
	return fmt.Sprintf("%s(%s)", p.params.Name, p.getAddress())
}

func (p *P4RTClient) ServerConnect() error {
	p.client_mu.Lock()
	defer p.client_mu.Unlock()

	if p.connection != nil {
		return fmt.Errorf("Client Already connected")
	}

	log.Printf("%s Connecting to Server\n", p)
	conn, err := grpc.Dial(p.getAddress(), grpc.WithInsecure())
	if err != nil {
		log.Printf("ERROR Client(%s) Connecting to Server: %s\n", p, err)
		return err
	}
	p.connection = conn
	log.Printf("%s Connected to Server\n", p)

	// Create a new P4RuntimeClient instance
	p.p4rtClient = p4_v1.NewP4RuntimeClient(conn)

	return nil
}

func (p *P4RTClient) ServerDisconnect() {

	if p.connection != nil {
		log.Printf("%s Disconnecting from Server\n", p)
		p.connection.Close()
		p.connection = nil
	}
}

// Return unique handle for this stream
// 0 is not a valid id
func (p *P4RTClient) StreamChannelCreate() (uint32, error) {
	p.client_mu.Lock()
	if p.connection == nil {
		p.client_mu.Unlock()
		return 0, fmt.Errorf("Client Not connected")
	}
	p.client_mu.Unlock()

	// RPC and setup the stream
	stream, gerr := p.p4rtClient.StreamChannel(context.Background())
	if gerr != nil {
		log.Printf("ERROR Client(%s) StreamChannel: %s\n", p, gerr)
		return 0, gerr
	}

	p.client_mu.Lock()
	if p.streams == nil {
		p.client_mu.Unlock()
		return 0, fmt.Errorf("P4RTClient Not properly Initialized (nil streams)")
	}
	// Get a new id for this stream
	p.streamId++
	streamId := p.streamId
	cSession := &P4RTClientSession{
		streamId: streamId,
		stream:   stream,
	}
	// Add Stream to map, indexed by p.streamId
	p.streams[streamId] = cSession
	p.client_mu.Unlock()

	// Make sure the RX routine is happy
	upChan := make(chan bool)

	// For ever read from stream
	go func(session *P4RTClientSession) {
		log.Printf("RX(%d) Started\n", session.streamId)
		upChan <- true

		for {
			/*Before we block, we test to stop*/
			if session.ShouldStop() {
				break
			}
			event, stream_err := stream.Recv()
			/*When we wake up, we test to stop*/
			if session.ShouldStop() {
				break
			}

			if stream_err != nil {
				log.Printf("Client Recv Error %v\n", stream_err)
				break
			}

			// XXX Remove unecessary entries
			switch event.Update.(type) {
			case *p4_v1.StreamMessageResponse_Arbitration:
				// XXX Add to buffered channel
				log.Printf("Received %s\n", event.String())
			case *p4_v1.StreamMessageResponse_Packet:
				log.Printf("Received %s\n", event.String())
				// XXX Add to buffered channel
			case *p4_v1.StreamMessageResponse_Digest:
			case *p4_v1.StreamMessageResponse_IdleTimeoutNotification:
			case *p4_v1.StreamMessageResponse_Other:
			case *p4_v1.StreamMessageResponse_Error:
			default:
				log.Printf("ERROR StreamId(%d) Received %s\n",
					session.streamId, event.String())
			}
		}

		// Cleanup the stream, lock and remove from map
		p.StreamChannelDestroy(session.streamId)
		log.Printf("RX(%d) Exited\n", session.streamId)

	}(cSession)

	// Wait for the Tx Routine to start
	log.Printf("Waiting for RX(%d)\n", streamId)
	<-upChan
	log.Printf("RX(%d) Successfully Spawned\n", streamId)

	return streamId, nil
}

func (p *P4RTClient) StreamChannelGet(streamId uint32) *P4RTClientSession {
	p.client_mu.Lock()
	defer p.client_mu.Unlock()

	if cSession, found := p.streams[streamId]; found {
		return cSession
	}

	return nil
}

func (p *P4RTClient) StreamChannelDestroy(streamId uint32) {
	// XXX What do we need to cleanup here
	p.client_mu.Lock()
	defer p.client_mu.Unlock()

	if _, found := p.streams[streamId]; found {
		log.Printf("Destroying RX(%d)\n", streamId)
		p.streams[streamId].Stop()
		delete(p.streams, streamId)
	}
}

func (p *P4RTClient) StreamChannelSendArbitration(streamId uint32,
	deviceId uint64, electionId *p4_v1.Uint128) error {

	cSession := p.StreamChannelGet(streamId)
	if cSession == nil {
		return fmt.Errorf("StreamId(%d) Not found", streamId)
	}

	message := &p4_v1.StreamMessageRequest{
		Update: &p4_v1.StreamMessageRequest_Arbitration{
			Arbitration: &p4_v1.MasterArbitrationUpdate{
				DeviceId:   deviceId,
				ElectionId: electionId,
			},
		},
	}

	err := cSession.stream.Send(message)
	if err != nil {
		log.Printf("ERROR Session(%d) '%s': '%s'\n", streamId, message, err)
		return err
	}

	cSession.ElectionId = electionId
	cSession.DeviceId = deviceId

	return nil
}

//
// Creates and Initializes a P4RT client
//
func NewP4RTClient(params *P4RTClientParams) *P4RTClient {
	client := &P4RTClient{
		params: params,
	}

	// Initialize
	client.streams = make(map[uint32]*P4RTClientSession)

	return client
}
