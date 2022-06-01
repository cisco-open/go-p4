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
	P4RT_MAX_ARBITRATION_QUEUE_SIZE = 100
	P4RT_MAX_PACKET_QUEUE_SIZE      = 100
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

type P4RTArbInfo struct {
	SeqNum uint64
	Arb    *p4_v1.MasterArbitrationUpdate
}

type P4RTArbCounters struct {
	RxArbCntr       uint64
	RxArbCntrDrop   uint64
	RxArbCntrQueued uint64
}

type P4RTPacketInfo struct {
	SeqNum uint64
	Pkt    *p4_v1.PacketIn
}

type P4RTPacketCounters struct {
	RxPktCntr       uint64
	RxPktCntrDrop   uint64
	RxPktCntrQueued uint64
}

type P4RTClientStream struct {
	Params     P4RTStreamParameters // Make a copy
	stream     p4_v1.P4Runtime_StreamChannelClient
	cancelFunc context.CancelFunc

	stopMu sync.Mutex // Protects the following:
	stop   bool
	// end stopMu Protection

	arb_mu      sync.Mutex // Protects the following:
	arbCond     *sync.Cond
	arbCounters P4RTArbCounters
	arbQSize    int
	arbQ        []*P4RTArbInfo
	// end arb_mu Protection

	pkt_mu      sync.Mutex // Protects the following:
	pktCond     *sync.Cond
	pktCounters P4RTPacketCounters
	pktQSize    int
	pktQ        []*P4RTPacketInfo
	// end pkt_mu Protection
}

func (p *P4RTClientStream) String() string {
	return fmt.Sprintf("Device(%d) Stream(%s)", p.Params.DeviceId, p.Params.Name)
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

// XXX Do we need a callback function to avoid polling?
// Should this be Global or per device?
func (p *P4RTClientStream) QueueArbt(arbInfo *P4RTArbInfo) {
	p.arb_mu.Lock()
	p.arbCounters.RxArbCntr++
	arbInfo.SeqNum = p.arbCounters.RxArbCntr
	qLen := len(p.arbQ)
	if qLen >= p.arbQSize {
		p.arbCounters.RxArbCntrDrop++
		p.arb_mu.Unlock()
		log.Printf("'%s' WARNING Queue Full Dropping QSize(%d) Arb(%s)",
			p, qLen, arbInfo.Arb)
		return
	}

	p.arbQ = append(p.arbQ, arbInfo)
	p.arbCounters.RxArbCntrQueued++

	p.arb_mu.Unlock()

	p.arbCond.Signal()
}

func (p *P4RTClientStream) GetArbCounters() *P4RTArbCounters {
	p.arb_mu.Lock()
	defer p.arb_mu.Unlock()

	arbCounters := p.arbCounters
	return &arbCounters
}

func (p *P4RTClientStream) GetArbitration(minSeqNum uint64) (uint64, *P4RTArbInfo) {
	p.arb_mu.Lock()
	defer p.arb_mu.Unlock()

	for p.arbCounters.RxArbCntr < minSeqNum {
		log.Printf("'%s' Waiting on Arbitration message (%d/%d)\n",
			p, p.arbCounters.RxArbCntr, minSeqNum)
		p.arbCond.Wait()
	}

	if len(p.arbQ) == 0 {
		return p.arbCounters.RxArbCntr, nil
	}

	arbInfo := p.arbQ[0]
	p.arbQ = p.arbQ[1:]

	return p.arbCounters.RxArbCntr, arbInfo
}

func (p *P4RTClientStream) SetArbQSize(size int) {
	p.arb_mu.Lock()
	defer p.arb_mu.Unlock()

	p.arbQSize = size
}

func (p *P4RTClientStream) GetArbQSize() int {
	p.arb_mu.Lock()
	defer p.arb_mu.Unlock()

	return p.arbQSize
}

// XXX Do we need a callback function to avoid polling?
// Should this be Global or per device?
func (p *P4RTClientStream) QueuePacket(pktInfo *P4RTPacketInfo) {
	p.pkt_mu.Lock()
	p.pktCounters.RxPktCntr++
	pktInfo.SeqNum = p.pktCounters.RxPktCntr
	qLen := len(p.pktQ)
	if qLen >= p.pktQSize {
		p.pktCounters.RxPktCntrDrop++
		p.pkt_mu.Unlock()
		log.Printf("'%s' WARNING Queue Full Dropping QSize(%d) Pkt(%s)",
			p, qLen, pktInfo.Pkt)
		return
	}

	p.pktQ = append(p.pktQ, pktInfo)
	p.pktCounters.RxPktCntrQueued++

	p.pkt_mu.Unlock()

	p.pktCond.Signal()
}

func (p *P4RTClientStream) GetPacketCounters() *P4RTPacketCounters {
	p.pkt_mu.Lock()
	defer p.pkt_mu.Unlock()

	pktCounters := p.pktCounters
	return &pktCounters
}

func (p *P4RTClientStream) GetPacket(minSeqNum uint64) (uint64, *P4RTPacketInfo) {
	p.pkt_mu.Lock()
	defer p.pkt_mu.Unlock()

	for p.pktCounters.RxPktCntr < minSeqNum {
		log.Printf("'%s' Waiting on Packet (%d/%d)\n",
			p, p.pktCounters.RxPktCntr, minSeqNum)
		p.pktCond.Wait()
	}

	if len(p.pktQ) == 0 {
		return p.pktCounters.RxPktCntr, nil
	}

	pktInfo := p.pktQ[0]
	p.pktQ = p.pktQ[1:]

	return p.pktCounters.RxPktCntr, pktInfo
}

func (p *P4RTClientStream) SetPacketQSize(size int) {
	p.pkt_mu.Lock()
	defer p.pkt_mu.Unlock()

	p.pktQSize = size
}

func (p *P4RTClientStream) GetPacketQSize() int {
	p.pkt_mu.Lock()
	defer p.pkt_mu.Unlock()

	return p.pktQSize
}

func NewP4RTClientStream(params *P4RTStreamParameters, stream p4_v1.P4Runtime_StreamChannelClient,
	cancelFunc context.CancelFunc) *P4RTClientStream {

	cStream := &P4RTClientStream{
		Params:     *params,
		stream:     stream,
		cancelFunc: cancelFunc,
		arbQSize:   P4RT_MAX_ARBITRATION_QUEUE_SIZE,
		pktQSize:   P4RT_MAX_PACKET_QUEUE_SIZE,
	}

	// Initialize
	cStream.arbCond = sync.NewCond(&cStream.arb_mu)
	cStream.pktCond = sync.NewCond(&cStream.pkt_mu)

	return cStream
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

			log.Printf("'%s' '%s' Received %s\n", p, iStream, event.String())

			switch event.Update.(type) {
			case *p4_v1.StreamMessageResponse_Arbitration:
				iStream.QueueArbt(&P4RTArbInfo{
					Arb: event.GetArbitration(),
				})
			case *p4_v1.StreamMessageResponse_Packet:
				iStream.QueuePacket(&P4RTPacketInfo{
					Pkt: event.GetPacket(),
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
// Returns the max arbitration sequence number observed and
// the "next" Arbitration from the FIFO queue (could be nil)
func (p *P4RTClient) StreamChannelGetArbitrationResp(streamName *string,
	minSeqNum uint64) (uint64, *P4RTArbInfo, error) {

	var seqNum uint64
	var respArbr *P4RTArbInfo

	cStream := p.streamChannelGet(streamName)
	if cStream == nil {
		return seqNum, respArbr, fmt.Errorf("'%s' Could not find stream(%s)\n", p, streamName)
	}

	seqNum, respArbr = cStream.GetArbitration(minSeqNum)

	return seqNum, respArbr, nil
}

func (p *P4RTClient) StreamChannelGetPacket(streamName *string,
	minSeqNum uint64) (uint64, *P4RTPacketInfo, error) {

	var seqNum uint64
	var pktInfo *P4RTPacketInfo

	cStream := p.streamChannelGet(streamName)
	if cStream == nil {
		return seqNum, pktInfo, fmt.Errorf("'%s' Could not find stream(%s)\n", p, streamName)
	}

	seqNum, pktInfo = cStream.GetPacket(minSeqNum)

	return seqNum, pktInfo, nil
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

func (p *P4RTClient) GetForwardingPipelineConfig(msg *p4_v1.GetForwardingPipelineConfigRequest) (*p4_v1.GetForwardingPipelineConfigResponse, error) {
	p.client_mu.Lock()
	if p.connection == nil {
		p.client_mu.Unlock()
		return nil, fmt.Errorf("'%s' Client Not connected", p)
	}
	p.client_mu.Unlock()

	log.Printf("'%s' GetForwardingPipelineConfig: %s\n", p, msg)
	resp, err := p.p4rtClient.GetForwardingPipelineConfig(context.Background(), msg)
	if err != nil {
		utils.LogErrorf("'%s' GetForwardingPipelineConfig: %s\n", p, err)
	} else {
		log.Printf("'%s' GetForwardingPipelineConfigResponse: %s\n", p, resp)
	}

	return resp, err
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
	client.streams = make(map[string]*P4RTClientStream)

	return client
}
