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
	"encoding/hex"
	"encoding/json"
	"fmt"
	p4_v1 "github.com/p4lang/p4runtime/go/p4/v1"
	grpc "google.golang.org/grpc"
	"io/ioutil"
	"log"
	"strconv"
	"sync"
	"wwwin-github.cisco.com/rehaddad/go-p4/utils"
)

const (
	P4RT_MAX_PACKET_QUEUE_SIZE = 100
)

type P4RTStreamParameters struct {
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
	Streams    []P4RTStreamParameters
}

type P4RTParameters struct {
	Clients []P4RTClientParameters
}

func P4RTParameterLoad(fileName *string) (*P4RTParameters, error) {
	var param P4RTParameters

	jsonFile, err := ioutil.ReadFile(*fileName)
	if err != nil {
		utils.LogErrorf("Could not open file %s", *fileName)
	} else {
		err = json.Unmarshal(jsonFile, &param)
	}
	return &param, err
}

func P4RTParameterToString(params *P4RTParameters) string {
	data, _ := json.Marshal(params)
	return string(data)
}

type P4RTClientStream struct {
	Params          P4RTStreamParameters // Make a copy
	streamRxPktCntr uint64
	stream          p4_v1.P4Runtime_StreamChannelClient
	cancelFunc      context.CancelFunc

	stopMu sync.Mutex // Protects the following:
	stop   bool
	// end stopMu Protection

	lastRespArbrMu   sync.Mutex
	lastRespArbrCond *sync.Cond
	lastRespArbrSeq  uint64
	lastRespArbr     *p4_v1.MasterArbitrationUpdate
}

func (p *P4RTClientStream) String() string {
	return fmt.Sprintf("Stream(%s)", p.Params.Name)
}

func (p *P4RTClientStream) ShouldStop() bool {
	p.stopMu.Lock()
	defer p.stopMu.Unlock()
	return p.stop
}

func (p *P4RTClientStream) Stop() {
	p.stopMu.Lock()
	defer p.stopMu.Unlock()
	p.stop = true
}

func NewP4RTClientStream(params *P4RTStreamParameters, stream p4_v1.P4Runtime_StreamChannelClient,
	cancelFunc context.CancelFunc) *P4RTClientStream {

	cStream := &P4RTClientStream{
		Params:     *params,
		stream:     stream,
		cancelFunc: cancelFunc,
	}

	// Initialize
	cStream.lastRespArbrCond = sync.NewCond(&cStream.lastRespArbrMu)

	return cStream
}

type P4RTPacketInfo struct {
	StreamRxPktCntr uint64
	Pkt             *p4_v1.PacketIn
}

type P4RTPacketCounters struct {
	RxPktCntr       uint64
	RxPktCntrDrop   uint64
	RxPktCntrQueued uint64
}

// We want the Client to be more or less stateless so that we can do negative testing
// This would require the test driver to explicitly set every attribute in every message
type P4RTClient struct {
	Params P4RTClientParameters //Make a copy

	client_mu  sync.Mutex // Protects the following:
	connection *grpc.ClientConn
	p4rtClient p4_v1.P4RuntimeClient
	streams    map[string]*P4RTClientStream // We can have multiple streams per client
	// end client_mu Protection

	// XXX Packets need to be per Stream, as technically each Stream
	// can be connected to a different device
	pkt_mu      sync.Mutex // Protects the following:
	pktCounters P4RTPacketCounters
	pktQSize    int
	pktQ        []*P4RTPacketInfo
	// end pkt_mu Protection
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
		utils.LogErrorf("'%s' Connecting to Server: %s", p, err)
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

// XXX Do we need a callback function to avoid polling?
// Should this be Global or per device?
func (p *P4RTClient) queuePacket(pktInfo *P4RTPacketInfo) {
	p.pkt_mu.Lock()
	p.pktCounters.RxPktCntr++
	qLen := len(p.pktQ)
	if qLen >= p.pktQSize {
		p.pktCounters.RxPktCntrDrop++
		p.pkt_mu.Unlock()
		// XXX Add stream name
		log.Printf("'%s' WARNING Queue Full Dropping QSize(%d) Pkt(%s)",
			p, qLen, pktInfo.Pkt)
		return
	}

	p.pktQ = append(p.pktQ, pktInfo)
	p.pktCounters.RxPktCntrQueued++

	p.pkt_mu.Unlock()
}

func (p *P4RTClient) GetPacketCounters() *P4RTPacketCounters {
	p.pkt_mu.Lock()
	defer p.pkt_mu.Unlock()

	pktCounters := p.pktCounters
	return &pktCounters
}

func (p *P4RTClient) GetPacket() *P4RTPacketInfo {
	p.pkt_mu.Lock()
	defer p.pkt_mu.Unlock()

	if len(p.pktQ) == 0 {
		return nil
	}

	pktInfo := p.pktQ[0]
	p.pktQ = p.pktQ[1:]

	return pktInfo
}

func (p *P4RTClient) SetPacketQSize(size int) {
	p.pkt_mu.Lock()
	defer p.pkt_mu.Unlock()

	p.pktQSize = size
}

func (p *P4RTClient) GetPacketQSize() int {
	p.pkt_mu.Lock()
	defer p.pkt_mu.Unlock()

	return p.pktQSize
}

func (p *P4RTClient) StreamChannelCreate(params *P4RTStreamParameters) error {
	p.client_mu.Lock()
	if p.connection == nil {
		p.client_mu.Unlock()
		return fmt.Errorf("'%s' Client Not connected", p)
	}

	if p.streams == nil {
		p.client_mu.Unlock()
		return fmt.Errorf("'%s' P4RTClient Not properly Initialized (nil streams)", p)
	}

	if _, found := p.streams[params.Name]; found {
		p.client_mu.Unlock()
		return fmt.Errorf("'%s' Stream Name (%s) Already Initialized", p, params.Name)
	}

	// RPC and setup the stream
	ctx, cancelFunc := context.WithCancel(context.Background())
	stream, gerr := p.p4rtClient.StreamChannel(ctx)
	if gerr != nil {
		utils.LogErrorf("'%s' StreamChannel: %s", p, gerr)
		p.client_mu.Unlock()
		return gerr
	}

	cStream := NewP4RTClientStream(params, stream, cancelFunc)
	// Add Stream to map, indexed by stream Name
	p.streams[params.Name] = cStream
	p.client_mu.Unlock()

	// Make sure the RX routine is happy
	upChan := make(chan bool)

	// For ever read from stream
	go func(iStream *P4RTClientStream) {
		log.Printf("'%s' '%s' Started\n", p, iStream)
		upChan <- true

		for {
			/*Before we block, we test to stop*/
			if iStream.ShouldStop() {
				break
			}
			event, stream_err := iStream.stream.Recv()
			/*When we wake up, we test to stop*/
			if iStream.ShouldStop() {
				break
			}

			if stream_err != nil {
				// XXX When the server exits, we get an EOF
				// What should we do with that? Currently we just exit the routine
				log.Printf("'%s' '%s' Client Recv Error %v\n", p, iStream, stream_err)
				break
			}

			// XXX Remove unecessary entries
			switch event.Update.(type) {
			case *p4_v1.StreamMessageResponse_Arbitration:
				iStream.lastRespArbrMu.Lock()
				iStream.lastRespArbrSeq++
				iStream.lastRespArbr = event.GetArbitration()
				iStream.lastRespArbrMu.Unlock()
				iStream.lastRespArbrCond.Signal()
				log.Printf("'%s' '%s' Seq#(%d) Received %s\n", p, iStream, iStream.lastRespArbrSeq, event.String())

			case *p4_v1.StreamMessageResponse_Packet:
				log.Printf("'%s' Received %s\n", iStream, event.String())
				iStream.streamRxPktCntr++
				p.queuePacket(&P4RTPacketInfo{
					StreamRxPktCntr: iStream.streamRxPktCntr,
					Pkt:             event.GetPacket(),
				})
			case *p4_v1.StreamMessageResponse_Digest:
			case *p4_v1.StreamMessageResponse_IdleTimeoutNotification:
			case *p4_v1.StreamMessageResponse_Other:
			case *p4_v1.StreamMessageResponse_Error:
			default:
				log.Printf("ERROR '%s' '%s' Received %s\n", p, iStream, event.String())
			}
		}

		// Cleanup the stream, lock and remove from map
		log.Printf("'%s' '%s' Exiting - calling to destroy stream\n", p, iStream)
		p.StreamChannelDestroy(&iStream.Params.Name)
		log.Printf("'%s' '%s' Exited\n", p, iStream)

	}(cStream)

	// Wait for the Tx Routine to start
	log.Printf("'%s' Waiting for '%s' Go Routine\n", p, cStream)
	<-upChan
	log.Printf("'%s' Successfully Spawned '%s'\n", p, cStream)

	return nil
}

func (p *P4RTClient) streamChannelGet(streamName *string) *P4RTClientStream {
	p.client_mu.Lock()
	defer p.client_mu.Unlock()

	if cStream, found := p.streams[*streamName]; found {
		return cStream
	}

	return nil
}

// This function can be called from the driver or from the RX routine
// in case the serve closes the Stream
// We handle race conditions here (with locks)
func (p *P4RTClient) StreamChannelDestroy(streamName *string) error {
	cStream := p.streamChannelGet(streamName)
	if cStream == nil {
		return fmt.Errorf("'%s' Could not find stream(%s)\n", p, streamName)
	}

	log.Printf("'%s' Destroying '%s'", p, cStream)
	// Make sure the RX Routine is going to stop
	cStream.Stop()
	// Force the RX Recv() to wake up
	// (which would force the RX routing to Destroy and exit)
	cStream.cancelFunc()

	// Remove from map
	p.client_mu.Lock()
	if _, found := p.streams[*streamName]; found {
		delete(p.streams, *streamName)
	}
	p.client_mu.Unlock()

	return nil
}

func (p *P4RTClient) StreamChannelSendMsg(streamName *string, msg *p4_v1.StreamMessageRequest) error {
	cStream := p.streamChannelGet(streamName)
	if cStream == nil {
		return fmt.Errorf("'%s' Could not find stream(%s)\n", p, streamName)
	}

	log.Printf("'%s' '%s' StreamChannelSendMsg: %s\n", p, cStream, msg)
	switch msg.Update.(type) {
	case *p4_v1.StreamMessageRequest_Packet:
		pkt := msg.GetPacket()
		if pkt != nil {
			log.Printf("'%s' '%s' StreamChannelSendMsg: Packet: %s\n", p, cStream, hex.EncodeToString(pkt.Payload))
		}
	default:
	}

	err := cStream.stream.Send(msg)
	if err != nil {
		utils.LogErrorf("'%s' '%s' '%s': '%s'\n", p, cStream, msg, err)
		return err
	}

	return nil
}

// Block until AT LEAST minSeqNum is observed
func (p *P4RTClient) StreamChannelGetLastArbitrationResp(streamName *string,
	minSeqNum uint64) (uint64, *p4_v1.MasterArbitrationUpdate, error) {

	var lastSeqNum uint64
	var lastRespArbr *p4_v1.MasterArbitrationUpdate

	cStream := p.streamChannelGet(streamName)
	if cStream == nil {
		return lastSeqNum, lastRespArbr, fmt.Errorf("'%s' Could not find stream(%s)\n", p, streamName)
	}

	cStream.lastRespArbrMu.Lock()
	for cStream.lastRespArbrSeq < minSeqNum {
		cStream.lastRespArbrCond.Wait()

	}
	lastSeqNum = cStream.lastRespArbrSeq
	lastRespArbr = cStream.lastRespArbr
	cStream.lastRespArbrMu.Unlock()

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
		utils.LogErrorf("'%s' SetForwardingPipelineConfig: %s\n", p, err)
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
		utils.LogErrorf("'%s' Write: %s\n", p, err)
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
	client.pktQSize = P4RT_MAX_PACKET_QUEUE_SIZE
	client.streams = make(map[string]*P4RTClientStream)

	return client
}
