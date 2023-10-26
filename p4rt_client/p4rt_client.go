/*
 * Copyright (c) 2022 Cisco Systems, Inc. and its affiliates
 * All rights reserved.
 *
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package p4rt_client

import (
	"context"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"reflect"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/golang/glog"
	grpc_retry "github.com/grpc-ecosystem/go-grpc-middleware/retry"
	p4_v1 "github.com/p4lang/p4runtime/go/p4/v1"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	status1 "google.golang.org/grpc/status"
)

const (
	P4RT_MAX_ARBITRATION_QUEUE_SIZE = 100
	P4RT_MAX_PACKET_QUEUE_SIZE      = 100
	P4RT_STREAM_TERM_CHAN_SIZE      = 100
)

// flagCred implements credentials.PerRPCCredentials by populating the
// username and password metadata from flags.
type flagCred struct {
	username string
	password string
}

// GetRequestMetadata is needed by credentials.PerRPCCredentials.
func (s flagCred) GetRequestMetadata(ctx context.Context, uri ...string) (map[string]string, error) {
	// We can pass any metadata to the server here
	return map[string]string{
		"username": s.username,
		"password": s.password,
	}, nil
}

// RequireTransportSecurity is needed by credentials.PerRPCCredentials.
func (s flagCred) RequireTransportSecurity() bool {
	return false
}

type P4RTStreamParameters struct {
	Name        string
	DeviceId    uint64
	ElectionIdH uint64
	ElectionIdL uint64
}

func (p *P4RTStreamParameters) String() string {
	return fmt.Sprintf("Name(%s)-DeviceId(%d)-ElectionId(%d:%d)",
		p.Name, p.DeviceId, p.ElectionIdH, p.ElectionIdL)
}

type P4RTClientParameters struct {
	Name       string
	ServerIP   string
	ServerPort int
	Username   string
	Password   string
	P4InfoFile string
	Streams    []P4RTStreamParameters
}

func (p *P4RTClientParameters) String() string {
	return fmt.Sprintf("Name(%s)-%s:%d",
		p.Name, p.ServerIP, p.ServerPort)
}

type P4RTParameters struct {
	Clients []P4RTClientParameters
}

func P4RTParameterLoad(fileName *string) (*P4RTParameters, error) {
	var param P4RTParameters

	jsonFile, err := ioutil.ReadFile(*fileName)
	if err != nil {
		glog.Errorf("Could not open file %s", *fileName)
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

type P4RTStreamTermErr struct {
	ClientParams *P4RTClientParameters
	StreamParams *P4RTStreamParameters
	StreamErr    error
}

func (p *P4RTStreamTermErr) String() string {
	return fmt.Sprintf("ClientParams(%s) StreamParams(%s) Err(%s)",
		p.ClientParams, p.StreamParams, p.StreamErr)
}

type P4RTClientStream struct {
	Params     P4RTStreamParameters // Make a copy (initial config)
	stream     p4_v1.P4Runtime_StreamChannelClient
	cancelFunc context.CancelFunc

	paramsMu   sync.Mutex     // Protects the following:
	deviceId   uint64         // Based on last sent Arbitration message
	electionId *p4_v1.Uint128 // Based on last sent Arbitration message
	// end paramsMu Protection

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

func (p *P4RTClientStream) SetDeviceId(deviceId uint64) {
	p.paramsMu.Lock()
	defer p.paramsMu.Unlock()
	p.deviceId = deviceId
}

func (p *P4RTClientStream) GetDeviceId() uint64 {
	p.paramsMu.Lock()
	defer p.paramsMu.Unlock()
	return p.deviceId
}

func (p *P4RTClientStream) SetElectionId(electionId *p4_v1.Uint128) {
	p.paramsMu.Lock()
	defer p.paramsMu.Unlock()
	p.electionId = electionId
}

func (p *P4RTClientStream) GetElectionId() *p4_v1.Uint128 {
	p.paramsMu.Lock()
	defer p.paramsMu.Unlock()
	return p.electionId
}

func (p *P4RTClientStream) SetParams(deviceId uint64, electionId *p4_v1.Uint128) {
	p.paramsMu.Lock()
	defer p.paramsMu.Unlock()
	p.deviceId = deviceId
	p.electionId = electionId
}

func (p *P4RTClientStream) GetParams() (uint64, *p4_v1.Uint128) {
	p.paramsMu.Lock()
	defer p.paramsMu.Unlock()
	return p.deviceId, p.electionId
}

func (p *P4RTClientStream) ShouldStop() bool {
	p.stopMu.Lock()
	defer p.stopMu.Unlock()
	return p.stop
}

func (p *P4RTClientStream) Stop() {
	p.stopMu.Lock()
	p.stop = true
	p.stopMu.Unlock()

	// Signal waiting GetArbitration routines.
	p.arb_mu.Lock()
	p.arbCond.Signal()
	p.arb_mu.Unlock()

	// Signal waiting GetPacket routines.
	p.pkt_mu.Lock()
	p.pktCond.Broadcast()
	p.pkt_mu.Unlock()

	// Force the RX Recv() to wake up
	// (which would force the RX routing to Destroy and exit)
	if p.cancelFunc != nil {
		p.cancelFunc()
	}
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
		glog.Warningf("'%s' Queue Full Dropping QSize(%d) Arb(%s)",
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

func (p *P4RTClientStream) GetArbitration(minSeqNum uint64) (uint64, *P4RTArbInfo, error) {
	p.arb_mu.Lock()
	defer p.arb_mu.Unlock()

	for p.arbCounters.RxArbCntr < minSeqNum {
		if glog.V(2) {
			glog.Infof("'%s' Waiting on Arbitration message (%d/%d)\n",
				p, p.arbCounters.RxArbCntr, minSeqNum)
		}
		if p.ShouldStop() {
			return 0, nil, io.EOF
		}
		p.arbCond.Wait()
	}

	if len(p.arbQ) == 0 {
		return p.arbCounters.RxArbCntr, nil, nil
	}

	arbInfo := p.arbQ[0]
	p.arbQ = p.arbQ[1:]

	return p.arbCounters.RxArbCntr, arbInfo, nil
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
		glog.Warningf("'%s' Queue Full Dropping QSize(%d) Pkt(%s)",
			p, qLen, pktInfo.Pkt)
		return
	}

	p.pktQ = append(p.pktQ, pktInfo)
	p.pktCounters.RxPktCntrQueued++

	p.pkt_mu.Unlock()

	p.pktCond.Broadcast()
}

func (p *P4RTClientStream) GetPacketCounters() *P4RTPacketCounters {
	p.pkt_mu.Lock()
	defer p.pkt_mu.Unlock()

	pktCounters := p.pktCounters
	return &pktCounters
}

// GetPacket returns the receivedPacketCounter, the first Packet in Queue and
// an error if any.
// The method blocks until minSeqNum number of packets are received.
// It is advisable to call only one of either GetPacket() or GetPacketsWithTimeout()
// as a timeout in GetPacketsWithTimeout() may also unblock GetPacket()
func (p *P4RTClientStream) GetPacket(minSeqNum uint64) (uint64, *P4RTPacketInfo, error) {
	p.pkt_mu.Lock()
	defer p.pkt_mu.Unlock()

	for p.pktCounters.RxPktCntr < minSeqNum {
		if glog.V(2) {
			glog.Infof("'%s' Waiting on Packet (%d/%d)\n",
				p, p.pktCounters.RxPktCntr, minSeqNum)
		}
		if p.ShouldStop() {
			return 0, nil, io.EOF
		}
		p.pktCond.Wait()
	}

	if len(p.pktQ) == 0 {
		return p.pktCounters.RxPktCntr, nil, nil
	}

	pktInfo := p.pktQ[0]
	p.pktQ = p.pktQ[1:]

	return p.pktCounters.RxPktCntr, pktInfo, nil
}

// GetPacketsWithTimeout returns the receivedPacketCounter, the list of Packets
// in Queue and an error if any.
// If timeout happens before minSeqNum of packets are received, the current Packets
// in queue are returned along with a DeadlineExceeded error.
// It is advisable to call only one of either GetPacket() or GetPacketsWithTimeout()
// as a timeout in GetPacketsWithTimeout() may also unblock GetPacket()
func (p *P4RTClientStream) GetPacketsWithTimeout(minSeqNum uint64, timeout time.Duration) (uint64, []*P4RTPacketInfo, error) {

	done := make(chan struct{})
	defer close(done)
	var failed atomic.Bool

	// routine returns directly if 'done' else updates 'failed'
	// and signals pktCond.
	go func() {
		select {
		case <-done:
			return
		case <-time.After(timeout):
			failed.Store(true)
			p.pktCond.Broadcast()
		}
	}()

	p.pkt_mu.Lock()
	defer p.pkt_mu.Unlock()
	for p.pktCounters.RxPktCntr < minSeqNum {
		// if timeout then loop breaks here.
		if failed.Load() {
			break
		}
		if glog.V(2) {
			glog.Infof("'%s' Waiting on Packet (%d/%d)\n",
				p, p.pktCounters.RxPktCntr, minSeqNum)
		}
		if p.ShouldStop() {
			return 0, nil, io.EOF
		}
		p.pktCond.Wait()
	}

	var pkts []*P4RTPacketInfo

	for i := 0; i < len(p.pktQ); i++ {
		pkts = append(pkts, p.pktQ[i])
	}
	p.pktQ = []*P4RTPacketInfo{}

	if failed.Load() {
		return p.pktCounters.RxPktCntr, pkts, os.ErrDeadlineExceeded
	}
	return p.pktCounters.RxPktCntr, pkts, nil
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
	Params        P4RTClientParameters //Make a copy
	StreamTermErr chan *P4RTStreamTermErr

	client_mu     sync.Mutex // Protects the following:
	connection    *grpc.ClientConn
	connectionSet bool
	p4rtClient    p4_v1.P4RuntimeClient
	streams       map[string]*P4RTClientStream // We can have multiple streams per client
	// end client_mu Protection
}

func (p *P4RTClient) getAddress() string {
	address := fmt.Sprintf("%s:%s", p.Params.ServerIP, strconv.Itoa(p.Params.ServerPort))
	return address
}

func (p *P4RTClient) String() string {
	return fmt.Sprintf("%s(%s)", p.Params.Name, p.getAddress())
}

func (p *P4RTClient) P4rtClientSet(client p4_v1.P4RuntimeClient) error {
	p.client_mu.Lock()
	defer p.client_mu.Unlock()

	if p.connection != nil {
		return fmt.Errorf("'%s' Client Already connected", p)
	}
	p.p4rtClient = client
	p.connectionSet = true

	return nil
}

func (p *P4RTClient) ServerConnect() error {
	p.client_mu.Lock()
	defer p.client_mu.Unlock()

	if (p.connection != nil) || p.connectionSet {
		return fmt.Errorf("'%s' Client Already connected", p)
	}

	if glog.V(2) {
		glog.Infof("'%s' Connecting to Server\n", p)
	}
	conn, err := grpc.Dial(p.getAddress(), grpc.WithInsecure())
	if err != nil {
		glog.Errorf("'%s' Connecting to Server: %s", p, err)
		return err
	}
	p.connection = conn
	if glog.V(1) {
		glog.Infof("'%s' Connected to Server", p)
	}

	// Create a new P4RuntimeClient instance
	p.p4rtClient = p4_v1.NewP4RuntimeClient(conn)

	return nil
}

func (p *P4RTClient) ServerConnectWithOptions(insec bool, skipVerify bool, username string, password string) error {
	p.client_mu.Lock()
	defer p.client_mu.Unlock()

	if (p.connection != nil) || p.connectionSet {
		return fmt.Errorf("'%s' Client Already connected", p)
	}

	if glog.V(2) {
		glog.Infof("'%s' Connecting to Server\n", p)
	}

	// Setup dial options
	dialOpts := []grpc.DialOption{grpc.WithBlock()}

	// TLS
	if insec {
		if glog.V(1) {
			glog.Infof("no TLS")
		}
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	} else if skipVerify {
		if glog.V(1) {
			glog.Infof("TLS and Skip Verify")
		}
		tlsc := credentials.NewTLS(&tls.Config{
			InsecureSkipVerify: skipVerify,
		})
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(tlsc))
	} else {
		if glog.V(1) {
			glog.Infof("TLS and not skipping verify")
		}
	}

	// Per RPC credentials
	if password != "" {
		if glog.V(1) {
			glog.Infof("Using Per RPC Credentials")
		}
		dialOpts = append(dialOpts, grpc.WithPerRPCCredentials(flagCred{
			username: username,
			password: password}))
	}

	// Retry
	retryOpt := grpc_retry.WithPerRetryTimeout(5 * time.Second)
	dialOpts = append(dialOpts,
		grpc.WithStreamInterceptor(grpc_retry.StreamClientInterceptor(retryOpt)),
		grpc.WithUnaryInterceptor(grpc_retry.UnaryClientInterceptor(retryOpt)),
	)

	// Dial context
	ctx := context.Background()
	if glog.V(1) {
		glog.Infof("Setting dial context: (if stuck check TLS config)", p.getAddress())
	}
	conn, err := grpc.DialContext(ctx, p.getAddress(), dialOpts...)
	if err != nil {
		glog.Errorf("'%s' Connecting to Server: %s", p, err)
		return err
	}

	p.connection = conn
	if glog.V(1) {
		glog.Infof("'%s' Connected to Server", p)
	}

	// Create a new P4RuntimeClient instance
	p.p4rtClient = p4_v1.NewP4RuntimeClient(conn)

	return nil
}

func (p *P4RTClient) ServerDisconnect() {
	p.client_mu.Lock()
	defer p.client_mu.Unlock()

	if p.connection != nil {
		if glog.V(1) {
			glog.Infof("'%s' Disconnecting from Server\n", p)
		}
		p.connection.Close()
		p.connection = nil
	}

	p.connectionSet = false
}

func (p *P4RTClient) StreamChannelCreate(params *P4RTStreamParameters) error {
	p.client_mu.Lock()
	if (p.connection == nil) && (p.connectionSet == false) {
		p.client_mu.Unlock()
		return fmt.Errorf("'%s' Client Not connected", p)
	}

	if p.streams == nil {
		p.streams = make(map[string]*P4RTClientStream)
	}

	if _, found := p.streams[params.Name]; found {
		p.client_mu.Unlock()
		return fmt.Errorf("'%s' Stream Name (%s) Already Initialized", p, params.Name)
	}

	// RPC and setup the stream
	ctx, cancelFunc := context.WithCancel(context.Background())
	stream, gerr := p.p4rtClient.StreamChannel(ctx)
	if gerr != nil {
		glog.Errorf("'%s' StreamChannel: %s", p, gerr)
		cancelFunc()
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
		err := errors.New("Stopped by User")

		if glog.V(2) {
			glog.Infof("'%s' '%s' Started\n", p, iStream)
		}
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
				err = stream_err
				glog.Warningf("'%s' '%s' Client Recv Error %v\n", p, iStream, stream_err)
				break
			}

			if glog.V(2) {
				glog.Infof("'%s' '%s' Received %s\n", p, iStream, event.String())
			}

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
				glog.Errorf("'%s' '%s' Received %s\n", p, iStream, event.String())
			}
		}

		// Cleanup the stream, lock and remove from map
		if glog.V(1) {
			glog.Infof("'%s' '%s' Exiting - calling to destroy stream\n", p, iStream)
		}
		p.streamChannelDestroyInternal(iStream, err)
		if glog.V(1) {
			glog.Infof("'%s' '%s' Exited\n", p, iStream)
		}

	}(cStream)

	// Wait for the Tx Routine to start
	if glog.V(2) {
		glog.Infof("'%s' Waiting for '%s' Go Routine\n", p, cStream)
	}
	<-upChan
	if glog.V(1) {
		glog.Infof("'%s' Successfully Spawned '%s'\n", p, cStream)
	}
	return nil
}

func (p *P4RTClient) StreamChannelGet(streamName *string) *P4RTClientStream {
	p.client_mu.Lock()
	defer p.client_mu.Unlock()

	if p.streams != nil {
		if cStream, found := p.streams[*streamName]; found {
			return cStream
		}
	}

	return nil
}

func (p *P4RTClient) streamChannelDestroyInternal(cStream *P4RTClientStream, rErr error) error {
	if glog.V(1) {
		glog.Infof("'%s' Cleaning up '%s'", p, cStream)
	}

	// Remove from map
	p.client_mu.Lock()
	if p.streams != nil {
		if _, found := p.streams[cStream.Params.Name]; found {
			delete(p.streams, cStream.Params.Name)
		}
	}
	p.client_mu.Unlock()

	// Notify listener
	streamParams := cStream.Params // Make a copy
	clientParams := p.Params
	// Buffered Channel (will not get stuck unless the receiver is not reading)
	if p.StreamTermErr != nil {
		p.StreamTermErr <- &P4RTStreamTermErr{
			ClientParams: &clientParams,
			StreamParams: &streamParams,
			StreamErr:    rErr,
		}
	}

	cStream.Stop()

	return nil
}

func (p *P4RTClient) StreamChannelDestroy(streamName *string) error {
	cStream := p.StreamChannelGet(streamName)
	if cStream == nil {
		return fmt.Errorf("'%s' Could not find stream(%s)\n", p, *streamName)
	}

	if glog.V(1) {
		glog.Infof("'%s' Destroying '%s'", p, cStream)
	}
	// Make sure the RX Routine is going to stop
	cStream.Stop()

	return nil
}

func (p *P4RTClient) StreamChannelSendMsg(streamName *string, msg *p4_v1.StreamMessageRequest) error {
	cStream := p.StreamChannelGet(streamName)
	if cStream == nil {
		return fmt.Errorf("'%s' Could not find stream(%s)\n", p, *streamName)
	}

	if glog.V(2) {
		glog.Infof("'%s' '%s' StreamChannelSendMsg: %s\n", p, cStream, msg)
	}
	switch msg.Update.(type) {
	case *p4_v1.StreamMessageRequest_Arbitration:
		arb := msg.GetArbitration()
		if arb != nil {
			cStream.SetParams(arb.GetDeviceId(), arb.GetElectionId())
		}
	case *p4_v1.StreamMessageRequest_Packet:
		pkt := msg.GetPacket()
		if pkt != nil {
			if glog.V(2) {
				glog.Infof("'%s' '%s' StreamChannelSendMsg: Packet: %s\n", p, cStream, hex.EncodeToString(pkt.Payload))
			}
		}
	default:
	}

	err := cStream.stream.Send(msg)
	if err != nil {
		glog.Errorf("'%s' '%s' '%s': '%s'\n", p, cStream, msg, err)
		return err
	}

	return nil
}

func (p *P4RTClient) StreamGetParams(streamName *string) (uint64, *p4_v1.Uint128, error) {
	cStream := p.StreamChannelGet(streamName)
	if cStream == nil {
		return 0, nil, fmt.Errorf("'%s' Could not find stream(%s)\n", p, *streamName)
	}

	d, e := cStream.GetParams()

	return d, e, nil
}

// Block until AT LEAST minSeqNum is observed
// Returns the max arbitration sequence number observed and
// the "next" Arbitration from the FIFO queue (could be nil)
func (p *P4RTClient) StreamChannelGetArbitrationResp(streamName *string,
	minSeqNum uint64) (uint64, *P4RTArbInfo, error) {

	var seqNum uint64
	var respArbr *P4RTArbInfo
	var err error

	cStream := p.StreamChannelGet(streamName)
	if cStream == nil {
		return seqNum, respArbr, fmt.Errorf("'%s' Could not find stream(%s)\n", p, *streamName)
	}

	seqNum, respArbr, err = cStream.GetArbitration(minSeqNum)
	if err != nil {
		if glog.V(2) {
			glog.Infof("%q Stream(%s) Error: %s\n", p, *streamName, err)
		}
		return seqNum, nil, err
	}

	return seqNum, respArbr, nil
}

func (p *P4RTClient) StreamChannelGetPacket(streamName *string,
	minSeqNum uint64) (uint64, *P4RTPacketInfo, error) {

	var seqNum uint64
	var pktInfo *P4RTPacketInfo
	var err error

	cStream := p.StreamChannelGet(streamName)
	if cStream == nil {
		return seqNum, pktInfo, fmt.Errorf("'%s' Could not find stream(%s)\n", p, *streamName)
	}

	seqNum, pktInfo, err = cStream.GetPacket(minSeqNum)
	if err != nil {
		if glog.V(2) {
			glog.Infof("%q Stream(%s) Error: %s\n", p, *streamName, err)
		}
		return seqNum, nil, err
	}

	return seqNum, pktInfo, nil
}

func (p *P4RTClient) StreamChannelGetPackets(streamName *string,
	minSeqNum uint64, timeout time.Duration) (uint64, []*P4RTPacketInfo, error) {

	var seqNum uint64
	var pkts []*P4RTPacketInfo
	var err error

	cStream := p.StreamChannelGet(streamName)
	if cStream == nil {
		return seqNum, pkts, fmt.Errorf("'%s' Could not find stream(%s)\n", p, *streamName)
	}
	seqNum, pkts, err = cStream.GetPacketsWithTimeout(minSeqNum, timeout)
	if err != nil {
		if glog.V(2) {
			glog.Infof("%q Stream(%s) Error: %s\n", p, *streamName, err)
		}
	}

	return seqNum, pkts, err
}

func (p *P4RTClient) SetForwardingPipelineConfig(msg *p4_v1.SetForwardingPipelineConfigRequest) error {
	p.client_mu.Lock()
	if (p.connection == nil) && (p.connectionSet == false) {
		p.client_mu.Unlock()
		return fmt.Errorf("'%s' Client Not connected", p)
	}
	p.client_mu.Unlock()

	if glog.V(2) {
		glog.Infof("'%s' SetForwardingPipelineConfig: %s\n", p, msg)
	}
	_, err := p.p4rtClient.SetForwardingPipelineConfig(context.Background(), msg)
	if err != nil {
		glog.Errorf("'%s' SetForwardingPipelineConfig: %s\n", p, err)
	}

	return err
}

func (p *P4RTClient) GetForwardingPipelineConfig(msg *p4_v1.GetForwardingPipelineConfigRequest) (*p4_v1.GetForwardingPipelineConfigResponse, error) {
	p.client_mu.Lock()
	if (p.connection == nil) && (p.connectionSet == false) {
		p.client_mu.Unlock()
		return nil, fmt.Errorf("'%s' Client Not connected", p)
	}
	p.client_mu.Unlock()

	if glog.V(2) {
		glog.Infof("'%s' GetForwardingPipelineConfig: %s\n", p, msg)
	}
	resp, err := p.p4rtClient.GetForwardingPipelineConfig(context.Background(), msg)
	if err != nil {
		glog.Errorf("'%s' GetForwardingPipelineConfig: %s\n", p, err)
	} else {
		if glog.V(2) {
			glog.Infof("'%s' GetForwardingPipelineConfigResponse: %s\n", p, resp)
		}
	}

	return resp, err
}

func (p *P4RTClient) Capabilities(msg *p4_v1.CapabilitiesRequest) (*p4_v1.CapabilitiesResponse, error) {
	p.client_mu.Lock()
	if (p.connection == nil) && (p.connectionSet == false) {
		p.client_mu.Unlock()
		return nil, fmt.Errorf("'%s' Client Not connected", p)
	}
	p.client_mu.Unlock()

	if glog.V(2) {
		glog.Infof("'%s' Capabilities: %s\n", p, msg)
	}
	resp, err := p.p4rtClient.Capabilities(context.Background(), msg)
	if err != nil {
		glog.Warningf("'%s' Capabilities: %s\n", p, err)
	} else {
		if glog.V(2) {
			glog.Infof("'%s' Capabilities: %s\n", p, resp)
		}
	}

	return resp, err
}

func (p *P4RTClient) Write(msg *p4_v1.WriteRequest) error {
	p.client_mu.Lock()
	if (p.connection == nil) && (p.connectionSet == false) {
		p.client_mu.Unlock()
		return fmt.Errorf("'%s' Client Not connected", p)
	}
	p.client_mu.Unlock()

	if glog.V(2) {
		glog.Infof("(%s) Write: %s\n", p, msg)
	}
	_, err := p.p4rtClient.Write(context.Background(), msg)
	if err != nil {
		glog.Warningf("'%s' Write: %s\n", p, err)
	}

	return err
}

func (p *P4RTClient) Read(msg *p4_v1.ReadRequest) (p4_v1.P4Runtime_ReadClient, error) {
	p.client_mu.Lock()
	if (p.connection == nil) && (p.connectionSet == false) {
		p.client_mu.Unlock()
		return nil, fmt.Errorf("'%s' Client Not connected", p)
	}
	p.client_mu.Unlock()

	if glog.V(2) {
		glog.Infof("(%s) Read: %s\n", p, msg)
	}
	stream, err := p.p4rtClient.Read(context.Background(), msg)
	if err != nil {
		glog.Errorf("'%s' Read: %s\n", p, err)
	}

	return stream, err
}

// Creates and Initializes a P4RT client
func NewP4RTClient(params *P4RTClientParameters) *P4RTClient {
	client := &P4RTClient{
		Params:        *params,
		StreamTermErr: make(chan *P4RTStreamTermErr, P4RT_STREAM_TERM_CHAN_SIZE),
	}

	return client
}

// Helper function to parse the Write Errors
func P4RTWriteErrParse(err error) (int, int, []*p4_v1.Error) {
	countOK := 0
	countNotOK := 0
	var errDetails []*p4_v1.Error

	statsDetails := status1.Convert(err).Details()
	for _, statsDetail := range statsDetails {
		if se, ok := statsDetail.(*p4_v1.Error); ok {
			if glog.V(2) {
				glog.Infof("p4Server.Write Detail Error: %d Msg: %s", se.GetCanonicalCode(), se.GetMessage())
			}
			errDetails = append(errDetails, se)
			if se.GetCanonicalCode() == int32(codes.OK) {
				countOK++
			} else {
				countNotOK++
			}
		} else {
			glog.Fatalf("Error, not expecting Type %s", reflect.TypeOf(statsDetail))
		}
	}

	if glog.V(2) {
		glog.Infof("Write Response CountOK(%d) countNotOK(%d)", countOK, countNotOK)
	}
	return countOK, countNotOK, errDetails
}
