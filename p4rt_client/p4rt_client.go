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
	"encoding/json"
	"fmt"
	p4_v1 "github.com/p4lang/p4runtime/go/p4/v1"
	grpc "google.golang.org/grpc"
	"io/ioutil"
	"log"
	"strconv"
	"sync"
)

type P4RTSessionParameters struct {
	Name        string
	DeviceId    uint64
	ElectionIdH uint64
	ElectionIdL uint64
}

type P4RTClientParameters struct {
	Name       string
	ServerIP   string
	ServerPort int
	P4InfoFile string
	Sessions   []P4RTSessionParameters
}

type P4RTParameters struct {
	Clients []P4RTClientParameters
}

func P4RTParameterLoad(fileName *string) (*P4RTParameters, error) {
	var param P4RTParameters

	jsonFile, err := ioutil.ReadFile(*fileName)
	if err != nil {
		log.Printf("ERROR Could not open file %s", *fileName)
	} else {
		err = json.Unmarshal(jsonFile, &param)
	}
	return &param, err
}

func P4RTParameterToString(params *P4RTParameters) string {
	data, _ := json.Marshal(params)
	return string(data)
}

type P4RTClientSession struct {
	Params     P4RTSessionParameters // Make a copy
	streamId   uint32
	stream     p4_v1.P4Runtime_StreamChannelClient
	cancelFunc context.CancelFunc

	stopMu sync.Mutex // Protects the following:
	stop   bool
	// end stopMu Protection

	lastRespArbrMu   sync.Mutex
	lastRespArbrCond *sync.Cond
	lastRespArbrSeq  uint64
	lastRespArbr     *p4_v1.MasterArbitrationUpdate
}

func (p *P4RTClientSession) String() string {
	return fmt.Sprintf("%s streamId(%d)", p.Params.Name, p.streamId)
}

func (p *P4RTClientSession) ShouldStop() bool {
	p.stopMu.Lock()
	defer p.stopMu.Unlock()
	return p.stop
}

func (p *P4RTClientSession) Stop() {
	p.stopMu.Lock()
	defer p.stopMu.Unlock()
	p.stop = true
}

func NewP4RTClientSession(params *P4RTSessionParameters, streamId uint32,
	stream p4_v1.P4Runtime_StreamChannelClient, cancelFunc context.CancelFunc) *P4RTClientSession {
	cSession := &P4RTClientSession{
		Params:     *params,
		streamId:   streamId,
		stream:     stream,
		cancelFunc: cancelFunc,
	}

	// Initialize
	cSession.lastRespArbrCond = sync.NewCond(&cSession.lastRespArbrMu)

	return cSession
}

// We want the Client to be more or less stateless so that we can do negative testing
// This would require the test driver to explicitly set every attribute in every message
type P4RTClient struct {
	Params P4RTClientParameters //Make a copy

	client_mu  sync.Mutex // Protects the following:
	connection *grpc.ClientConn
	p4rtClient p4_v1.P4RuntimeClient
	streamId   uint32                        // Global increasing number
	streams    map[uint32]*P4RTClientSession // We can have multiple streams per client
	// end client_mu Protection

	Sessions map[string]uint32
}

func (p *P4RTClient) getAddress() string {
	address := fmt.Sprintf("%s:%s", p.Params.ServerIP, strconv.Itoa(p.Params.ServerPort))
	return address
}

func (p *P4RTClient) String() string {
	return fmt.Sprintf("%s(%s)", p.Params.Name, p.getAddress())
}

func (p *P4RTClient) ServerConnect() error {
	p.client_mu.Lock()
	defer p.client_mu.Unlock()

	if p.connection != nil {
		return fmt.Errorf("'%s' Client Already connected", p)
	}

	log.Printf("'%s' Connecting to Server\n", p)
	conn, err := grpc.Dial(p.getAddress(), grpc.WithInsecure())
	if err != nil {
		log.Printf("ERROR '%s' Connecting to Server: %s", p, err)
		return err
	}
	p.connection = conn
	log.Printf("'%s' Connected to Server", p)

	// Create a new P4RuntimeClient instance
	p.p4rtClient = p4_v1.NewP4RuntimeClient(conn)

	return nil
}

func (p *P4RTClient) ServerDisconnect() {

	if p.connection != nil {
		log.Printf("'%s' Disconnecting from Server\n", p)
		p.connection.Close()
		p.connection = nil
	}
}

// Return unique handle for this stream
// 0 is not a valid id
func (p *P4RTClient) StreamChannelCreate(params *P4RTSessionParameters) (uint32, error) {
	p.client_mu.Lock()
	if p.connection == nil {
		p.client_mu.Unlock()
		return 0, fmt.Errorf("'%s' Client Not connected", p)
	}
	p.client_mu.Unlock()

	// RPC and setup the stream
	ctx, cancelFunc := context.WithCancel(context.Background())
	stream, gerr := p.p4rtClient.StreamChannel(ctx)
	if gerr != nil {
		log.Printf("ERROR '%s' StreamChannel: %s", p, gerr)
		return 0, gerr
	}

	p.client_mu.Lock()
	if p.streams == nil {
		p.client_mu.Unlock()
		return 0, fmt.Errorf("ERROR '%s' P4RTClient Not properly Initialized (nil streams)", p)
	}
	// Get a new id for this stream
	p.streamId++
	streamId := p.streamId
	cSession := NewP4RTClientSession(params, streamId, stream, cancelFunc)
	// Add Stream to map, indexed by p.streamId
	p.streams[streamId] = cSession
	p.client_mu.Unlock()

	// Make sure the RX routine is happy
	upChan := make(chan bool)

	// For ever read from stream
	go func(session *P4RTClientSession) {
		log.Printf("'%s' '%s' Started\n", p, session)
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
				log.Printf("'%s' '%s' Client Recv Error %v\n", p, session, stream_err)
				break
			}

			// XXX Remove unecessary entries
			switch event.Update.(type) {
			case *p4_v1.StreamMessageResponse_Arbitration:
				session.lastRespArbrMu.Lock()
				session.lastRespArbrSeq++
				session.lastRespArbr = event.GetArbitration()
				session.lastRespArbrMu.Unlock()
				session.lastRespArbrCond.Signal()
				log.Printf("'%s' '%s' Seq#(%d) Received %s\n", p, session, session.lastRespArbrSeq, event.String())

			case *p4_v1.StreamMessageResponse_Packet:
				log.Printf("'%s' Received %s\n", session, event.String())
				// XXX Add to buffered channel
			case *p4_v1.StreamMessageResponse_Digest:
			case *p4_v1.StreamMessageResponse_IdleTimeoutNotification:
			case *p4_v1.StreamMessageResponse_Other:
			case *p4_v1.StreamMessageResponse_Error:
			default:
				log.Printf("ERROR '%s' '%s' Received %s\n", p, session, event.String())
			}
		}

		// Cleanup the stream, lock and remove from map
		log.Printf("'%s' '%s' Exiting - calling to destroy session\n", p, session)
		p.StreamChannelDestroy(session.streamId)
		log.Printf("'%s' '%s' Exited\n", p, session)

	}(cSession)

	// Wait for the Tx Routine to start
	log.Printf("'%s' Waiting for '%s' Go Routine\n", p, cSession)
	<-upChan
	log.Printf("'%s' Successfully Spawned '%s'\n", p, cSession)

	return streamId, nil
}

func (p *P4RTClient) streamChannelGet(streamId uint32) *P4RTClientSession {
	p.client_mu.Lock()
	defer p.client_mu.Unlock()

	if cSession, found := p.streams[streamId]; found {
		return cSession
	}

	return nil
}

// This function can be called from the driver or from the RX routine
// in case the serve closes the session
// We handle race conditions here (with locks)
func (p *P4RTClient) StreamChannelDestroy(streamId uint32) {
	cSession := p.streamChannelGet(streamId)
	if cSession == nil {
		log.Printf("'%s' Could not find streamId(%d)\n", p, streamId)
		return
	}

	log.Printf("'%s' Destroying '%s'", p, cSession)
	// Make sure the RX Routine is going to stop
	cSession.Stop()
	// Force the RX Recv() to wake up
	// (which would force the RX routing to Destroy and exit)
	cSession.cancelFunc()

	// Remove from map
	p.client_mu.Lock()
	if _, found := p.streams[streamId]; found {
		delete(p.streams, streamId)
	}
	p.client_mu.Unlock()
}

func (p *P4RTClient) StreamChannelSendArbitration(streamId uint32, msg *p4_v1.StreamMessageRequest) error {

	cSession := p.streamChannelGet(streamId)
	if cSession == nil {
		return fmt.Errorf("'%s' StreamId(%d) Not found", p, streamId)
	}

	err := cSession.stream.Send(msg)
	if err != nil {
		log.Printf("ERROR '%s' '%s' '%s': '%s'\n", p, cSession, msg, err)
		return err
	}

	return nil
}

// Block until AT LEAST minSeqNum is observed
func (p *P4RTClient) StreamChannelGetLastArbitrationResp(streamId uint32,
	minSeqNum uint64) (uint64, *p4_v1.MasterArbitrationUpdate, error) {

	var lastSeqNum uint64
	var lastRespArbr *p4_v1.MasterArbitrationUpdate

	cSession := p.streamChannelGet(streamId)
	if cSession == nil {
		return lastSeqNum, lastRespArbr, fmt.Errorf("'%s' StreamId(%d) Not found", p, streamId)
	}

	cSession.lastRespArbrMu.Lock()
	for cSession.lastRespArbrSeq < minSeqNum {
		cSession.lastRespArbrCond.Wait()

	}
	lastSeqNum = cSession.lastRespArbrSeq
	lastRespArbr = cSession.lastRespArbr
	cSession.lastRespArbrMu.Unlock()

	return lastSeqNum, lastRespArbr, nil
}

func (p *P4RTClient) SetForwardingPipelineConfig(msg *p4_v1.SetForwardingPipelineConfigRequest) error {
	p.client_mu.Lock()
	if p.connection == nil {
		p.client_mu.Unlock()
		return fmt.Errorf("'%s' Client Not connected", p)
	}
	p.client_mu.Unlock()

	log.Printf("'%s' SetForwardingPipelineConfig: %s\n", p, msg)
	_, err := p.p4rtClient.SetForwardingPipelineConfig(context.Background(), msg)
	if err != nil {
		log.Printf("ERROR '%s' SetForwardingPipelineConfig: %s\n", p, err)
	}

	return err
}

func (p *P4RTClient) Write(msg *p4_v1.WriteRequest) error {
	p.client_mu.Lock()
	if p.connection == nil {
		p.client_mu.Unlock()
		return fmt.Errorf("'%s' Client Not connected", p)
	}
	p.client_mu.Unlock()

	log.Printf("(%s) Write: %s\n", p, msg)
	_, err := p.p4rtClient.Write(context.Background(), msg)
	if err != nil {
		log.Printf("ERROR '%s' Write: %s\n", p, err)
	}

	return err
}

//
// Creates and Initializes a P4RT client
//
func NewP4RTClient(params *P4RTClientParameters) *P4RTClient {
	client := &P4RTClient{
		Params: *params,
	}

	// Initialize
	client.streams = make(map[uint32]*P4RTClientSession)
	client.Sessions = make(map[string]uint32)

	return client
}

func InitfromJson(jsonFile *string, serverIP *string, serverPort int) (map[string]*P4RTClient, *P4RTParameters) {
	clientsMap := make(map[string]*P4RTClient)

	// Read params JSON file to configure the setup
	params, err := P4RTParameterLoad(jsonFile)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Params: %s", P4RTParameterToString(params))

	for _, client := range params.Clients {
		if _, found := clientsMap[client.Name]; found {
			log.Fatalf("Client '%s' Already exists", client.Name)
		}

		if len(client.ServerIP) == 0 {
			client.ServerIP = *serverIP
		}
		if client.ServerPort == 0 {
			client.ServerPort = serverPort
		}

		p4rtClient := NewP4RTClient(&client)
		clientsMap[client.Name] = p4rtClient

		// Connect
		err = p4rtClient.ServerConnect()
		if err != nil {
			log.Fatal(err)
		}

		// Establish sessions
		for _, session := range client.Sessions {
			var streamId uint32

			streamId, err = p4rtClient.StreamChannelCreate(&session)
			if err != nil {
				log.Fatal(err)
			}
			p4rtClient.Sessions[session.Name] = streamId

			err = p4rtClient.StreamChannelSendArbitration(streamId, &p4_v1.StreamMessageRequest{
				Update: &p4_v1.StreamMessageRequest_Arbitration{
					Arbitration: &p4_v1.MasterArbitrationUpdate{
						DeviceId: session.DeviceId,
						ElectionId: &p4_v1.Uint128{
							High: session.ElectionIdH,
							Low:  session.ElectionIdL,
						},
					},
				},
			})
			if err != nil {
				log.Fatal(err)
			}

		}

	}

	return clientsMap, params
}
