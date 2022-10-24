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
package main

import (
	"flag"
	"github.com/cisco-open/go-p4/p4rt_client"
	"github.com/cisco-open/go-p4/utils"
	"github.com/golang/glog"
	p4_v1 "github.com/p4lang/p4runtime/go/p4/v1"
	codes "google.golang.org/grpc/codes"
	"net"
	"os"
	"time"
)

// This is just an example usage
func main() {
	flag.Parse()
	validateArgs()

	glog.Infof("Called as: %s", os.Args)

	clientMap := p4rt_client.NewP4RTClientMap()
	params, err := clientMap.InitfromJson(jsonFile, serverIP, *serverPort)
	if err != nil {
		glog.Fatal(err)
	}

	// Grab first Client Name (from JSON)
	client0Name := params.Clients[0].Name
	// Grab first stream Name
	client0Stream0Name := params.Clients[0].Streams[0].Name
	// Grab second stream Name
	client0Stream1Name := params.Clients[0].Streams[1].Name

	// Grab first client
	client0, cErr0 := clientMap.ClientGet(&client0Name)
	if cErr0 != nil {
		glog.Fatal(cErr0)
	}

	// Potential race condition here between stream 1 and stream 2
	// It is a bit random which one comes up first. If stream 2 comes up first
	// then stream 1 does not become primary at all.

	// Check primary state
	glog.Infof("'%s' Checking Primary state", client0)
	lastSeqNum0, arbMsg0, arbErr0 := client0.StreamChannelGetArbitrationResp(&client0Stream0Name, 1)
	if arbErr0 != nil {
		glog.Fatal(arbErr0)
	}
	if arbMsg0 == nil {
		glog.Fatalf("'%s' nil Arbitration", client0Stream0Name)
	}
	isPrimary0 := arbMsg0.Arb.Status.Code == int32(codes.OK)
	glog.Infof("'%s' '%s' Got Primary(%v) SeqNum(%d) %s", client0Name, client0Stream0Name, isPrimary0, lastSeqNum0, arbMsg0.Arb.String())

	// Let's see what Client0 stream1 has received as last arbitration
	// Stream1 should have preempted
	lastSeqNum1, arbMsg1, arbErr1 := client0.StreamChannelGetArbitrationResp(&client0Stream1Name, 1)
	if arbErr1 != nil {
		glog.Fatal(arbErr1)
	}
	if arbMsg1 == nil {
		glog.Fatalf("'%s' nil Arbitration", client0Stream1Name)
	}
	isPrimary1 := arbMsg1.Arb.Status.Code == int32(codes.OK)
	glog.Infof("'%s' '%s' Got Primary(%v) SeqNum(%d) %s", client0Name, client0Stream1Name, isPrimary1, lastSeqNum1, arbMsg1.Arb.String())

	// Load P4Info file
	p4Info, p4InfoErr := utils.P4InfoLoad(&params.Clients[0].P4InfoFile)
	if p4InfoErr != nil {
		glog.Fatal(p4InfoErr)
	}

	// Get Capbilities (for now, we just log it)
	_, err = client0.Capabilities(&p4_v1.CapabilitiesRequest{})
	if err != nil {
		glog.Warningf("Capabilities err: %s", err)
	}

	// Set Forwarding pipeline
	// Not associated with any streams, but we have to use the primary's
	// Note, arbMsg1 have the primary's Election Id
	err = client0.SetForwardingPipelineConfig(&p4_v1.SetForwardingPipelineConfigRequest{
		DeviceId:   arbMsg1.Arb.DeviceId,
		ElectionId: arbMsg1.Arb.ElectionId,
		Action:     p4_v1.SetForwardingPipelineConfigRequest_VERIFY_AND_COMMIT,
		Config: &p4_v1.ForwardingPipelineConfig{
			P4Info: &p4Info,
			Cookie: &p4_v1.ForwardingPipelineConfig_Cookie{
				Cookie: 159,
			},
		},
	})
	if err != nil {
		glog.Fatal(err)
	}

	// Get Forwarding pipeline (for now, we just log it)
	_, err = client0.GetForwardingPipelineConfig(&p4_v1.GetForwardingPipelineConfigRequest{
		DeviceId:     arbMsg1.Arb.DeviceId,
		ResponseType: p4_v1.GetForwardingPipelineConfigRequest_ALL,
	})
	if err != nil {
		glog.Fatal(err)
	}

	// Write is not associated with any streams, but we have to use the primary's
	err = client0.Write(&p4_v1.WriteRequest{
		DeviceId:   arbMsg1.Arb.DeviceId,
		ElectionId: arbMsg1.Arb.ElectionId,
		Updates: []*p4_v1.Update{
			{
				Type: p4_v1.Update_INSERT,
				Entity: &p4_v1.Entity{
					Entity: &p4_v1.Entity_TableEntry{
						TableEntry: &p4_v1.TableEntry{
							TableId: 123,
							Match: []*p4_v1.FieldMatch{
								{
									FieldId: 1,
									FieldMatchType: &p4_v1.FieldMatch_Optional_{
										Optional: &p4_v1.FieldMatch_Optional{
											Value: []byte{byte(1)},
										},
									},
								},
								{
									FieldId: 2,
									FieldMatchType: &p4_v1.FieldMatch_Optional_{
										Optional: &p4_v1.FieldMatch_Optional{
											Value: []byte{byte(1)},
										},
									},
								},
							},
							Action: &p4_v1.TableAction{
								Type: &p4_v1.TableAction_Action{
									Action: &p4_v1.Action{
										ActionId: 1,
									},
								},
							},
						},
					},
				},
			},
			{
				Type: p4_v1.Update_INSERT,
				Entity: &p4_v1.Entity{
					Entity: &p4_v1.Entity_TableEntry{
						TableEntry: &p4_v1.TableEntry{
							TableId: 1,
							Match: []*p4_v1.FieldMatch{
								{
									FieldId: 1,
									FieldMatchType: &p4_v1.FieldMatch_Optional_{
										Optional: &p4_v1.FieldMatch_Optional{
											Value: []byte{byte(2)},
										},
									},
								},
								{
									FieldId: 2,
									FieldMatchType: &p4_v1.FieldMatch_Optional_{
										Optional: &p4_v1.FieldMatch_Optional{
											Value: []byte{byte(2)},
										},
									},
								},
							},
							Action: &p4_v1.TableAction{
								Type: &p4_v1.TableAction_Action{
									Action: &p4_v1.Action{
										ActionId: 1,
									},
								},
							},
						},
					},
				},
			},
		},
		Atomicity: p4_v1.WriteRequest_CONTINUE_ON_ERROR,
	})
	if err != nil {
		countOK, countNotOK, errDetails := p4rt_client.P4RTWriteErrParse(err)
		if glog.V(2) {
			glog.Infof("Write Partial Errors %d/%d: %s", countOK, countNotOK, errDetails)
		}
	} else {
		glog.Info("Write Success")
	}

	// Read ALL and log
	rStream, rErr := client0.Read(&p4_v1.ReadRequest{
		DeviceId: arbMsg1.Arb.DeviceId,
		Entities: []*p4_v1.Entity{
			&p4_v1.Entity{
				Entity: &p4_v1.Entity_TableEntry{},
			},
		},
	})
	if rErr != nil {
		glog.Fatal(rErr)
	}
	for {
		readResp, respErr := rStream.Recv()
		if respErr != nil {
			glog.Warningf("Read Response Err: %s", respErr)
			break
		} else {
			if glog.V(2) {
				glog.Infof("Read Response: %s", readResp)
			}
		}
	}

	// Send L3 packet to ingress (on Primary channel)
	err = client0.StreamChannelSendMsg(
		&client0Stream1Name, &p4_v1.StreamMessageRequest{
			Update: &p4_v1.StreamMessageRequest_Packet{
				Packet: &p4_v1.PacketOut{
					Payload: utils.PacketICMPEchoRequestGet(false,
						net.HardwareAddr{0xFF, 0xAA, 0xFA, 0xAA, 0xFF, 0xAA},
						net.HardwareAddr{0xBD, 0xBD, 0xBD, 0xBD, 0xBD, 0xBD},
						net.IP{10, 0, 0, 1},
						net.IP{10, 0, 0, 2},
						64),
					Metadata: []*p4_v1.PacketMetadata{
						&p4_v1.PacketMetadata{
							MetadataId: 2, // "submit_to_ingress"
							Value:      []byte{0x1},
						},
					},
				},
			},
		})
	if err != nil {
		glog.Fatal(err)
	}

	// Get the last sequence number received so far
	lastSeqNum0, arbMsg0, arbErr0 = client0.StreamChannelGetArbitrationResp(&client0Stream0Name, 0)
	if arbErr0 != nil {
		glog.Fatal(arbErr0)
	}
	if arbMsg0 != nil {
		isPrimary0 = arbMsg0.Arb.Status.Code == int32(codes.OK)
		glog.Infof("'%s' '%s' Got Primary(%v) SeqNum(%d) %s", client0Name, client0Stream0Name, isPrimary0, lastSeqNum0, arbMsg0.Arb.String())
	}
	if glog.V(2) {
		glog.Infof("'%s' '%s' Got Last SeqNum(%d)", client0Name, client0Stream0Name, lastSeqNum0)
	}

	// Try removing the current Primary
	client0.StreamChannelDestroy(&client0Stream1Name)

	// Read what Stream 1 got AFTER the primary exits and Deplete the queue
	lastSeqNum0 = lastSeqNum0 + 1
	for {
		lastSeqNum0, arbMsg0, arbErr0 = client0.StreamChannelGetArbitrationResp(&client0Stream0Name, lastSeqNum0)
		if arbErr0 != nil {
			glog.Fatal(arbErr0)
		}
		if arbMsg0 != nil {
			isPrimary0 = arbMsg0.Arb.Status.Code == int32(codes.OK)
			glog.Infof("'%s' '%s' Got Primary(%v) SeqNum(%d) %s", client0Name, client0Stream0Name, isPrimary0, lastSeqNum0, arbMsg0.Arb.String())
		} else {
			if glog.V(2) {
				glog.Infof("'%s' '%s' nil Arb Msg - Got Last SeqNum(%d)", client0Name, client0Stream0Name, lastSeqNum0)
			}
			break
		}
	}

	// We can also get a reference to the stream and use it directly
	client0Stream0 := client0.StreamChannelGet(&client0Stream0Name)
	if client0Stream0 == nil {
		glog.Fatalf("'%s' nil stream", client0Stream0Name)
	}

	arbCntrs := client0Stream0.GetArbCounters()
	glog.Infof("stream '%s' Arbitration Counters RxArbCntr(%d) RxArbCntrDrop(%d) RxArbCntrQueued(%d)",
		client0Stream0Name, arbCntrs.RxArbCntr, arbCntrs.RxArbCntrDrop, arbCntrs.RxArbCntrQueued)

	qSize := 200
	// Set the Arbitration queue size (in case default is not enough)
	client0Stream0.SetArbQSize(qSize)
	qSizeRead := client0Stream0.GetArbQSize()
	if qSize != qSizeRead {
		glog.Fatalf("Stream '%s' expecting Arbitration qSize(%d) Got (%d)",
			client0Stream0Name, qSize, qSizeRead)
	}

	pktCntrs := client0Stream0.GetPacketCounters()
	glog.Infof("stream '%s' Packet Counters RxPktCntr(%d) RxPktCntrDrop(%d) RxPktCntrQueued(%d)",
		client0Stream0Name, pktCntrs.RxPktCntr, pktCntrs.RxPktCntrDrop, pktCntrs.RxPktCntrQueued)

	// Set the packet queue size (in case default is not enough)
	client0Stream0.SetPacketQSize(qSize)
	qSizeRead = client0Stream0.GetPacketQSize()
	if qSize != qSizeRead {
		glog.Fatalf("Stream '%s' expecting Packet qSize(%d) Got (%d)",
			client0Stream0Name, qSize, qSizeRead)
	}

	counter := 0
ForEver:
	for {
		select {
		case <-time.After(1 * time.Microsecond):
			if counter > 100000 {
				break ForEver
			}
			counter++

			// Send L2 packet to egress
			err = client0.StreamChannelSendMsg(
				&client0Stream0Name, &p4_v1.StreamMessageRequest{
					Update: &p4_v1.StreamMessageRequest_Packet{
						Packet: &p4_v1.PacketOut{
							Payload: utils.PacketICMPEchoRequestGet(true,
								net.HardwareAddr{0xFF, 0xAA, 0xFA, 0xAA, 0xFF, 0xAA},
								net.HardwareAddr{0xBD, 0xBD, 0xBD, 0xBD, 0xBD, 0xBD},
								net.IP{10, 0, 0, 1},
								net.IP{10, 0, 0, 2},
								64),
							Metadata: []*p4_v1.PacketMetadata{
								&p4_v1.PacketMetadata{
									MetadataId: 1,            // "egress_port"
									Value:      []byte("24"), // Port-id As configured
								},
							},
						},
					},
				})
			if err != nil {
				glog.Fatal(err)
			}

			if glog.V(2) {
				glog.Infof("Going back to sleep...")
			}
		}
	}

	client0.StreamChannelDestroy(&client0Stream0Name)

	client0.ServerDisconnect()

	glog.Flush()

	// XXX Second device stream still up (should be cleaned up on exit)
}
