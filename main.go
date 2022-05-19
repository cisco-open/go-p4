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
	"flag"
	p4_v1 "github.com/p4lang/p4runtime/go/p4/v1"
	codes "google.golang.org/grpc/codes"
	"log"
	"net"
	"os"
	"time"
	"wwwin-github.cisco.com/rehaddad/go-p4/p4info/wbb"
	"wwwin-github.cisco.com/rehaddad/go-p4/p4rt_client"
	"wwwin-github.cisco.com/rehaddad/go-p4/utils"
	// 	codes "google.golang.org/grpc/codes"
	// 	status1 "google.golang.org/grpc/status"
)

// XXX Should this be test case 1? Or is this an entrance to regression 1?
//     How are tests written? This is not a script...
func main() {
	flag.Parse()
	utils.UtilsInitLogger(*outputDir)
	validateArgs()
	log.Println("Called as:", os.Args)

	clientsMap, params := p4rt_client.InitfromJson(jsonFile, serverIP, *serverPort)

	// Grab first Client (from JSON)
	p4rtClient := clientsMap[params.Clients[0].Name]
	// Grab first session ID
	sessionId1Name := params.Clients[0].Sessions[0].Name
	sessionId1 := p4rtClient.Sessions[sessionId1Name]

	// XXX Noticed a race condition here between session 1 and session 2
	// It is a bit random which one comes up first. If session 2 comes up first
	// then session 1 does not become master at all.
	// In this case, the sequence number is messed up (expecting 2, but we got 1)

	// Check mastership
	lastSeqNum, arbMsg, arbErr := p4rtClient.StreamChannelGetLastArbitrationResp(sessionId1, 1)
	if arbErr != nil {
		log.Fatal(arbErr)
	}
	isMaster := arbMsg.Status.Code == int32(codes.OK)
	log.Printf("'%s' '%s' Got Master(%v) %d %s", params.Clients[0].Name, sessionId1Name, isMaster, lastSeqNum, arbMsg.String())

	// Let's see what Client1 session2 has received as last arbitration
	// Master2 should have preempted
	sessionId2Name := params.Clients[0].Sessions[1].Name
	sessionId2 := p4rtClient.Sessions[sessionId2Name]
	lastSeqNum2, arbMsg2, arbErr2 := p4rtClient.StreamChannelGetLastArbitrationResp(sessionId2, 1)
	if arbErr2 != nil {
		log.Fatal(arbErr2)
	}
	isMaster2 := arbMsg2.Status.Code == int32(codes.OK)
	log.Printf("'%s' '%s' Got Master(%v) %d %s", params.Clients[0].Name, sessionId2Name, isMaster2, lastSeqNum2, arbMsg2.String())

	// Load P4Info file
	p4Info, p4InfoErr := utils.P4InfoLoad(&params.Clients[0].P4InfoFile)
	if p4InfoErr != nil {
		log.Fatal(p4InfoErr)
	}

	// Set Forwarding pipeline
	// Not associated with any sessions, but we have to use the master's
	// Note, both arbMsg and arbMsg2 have the master's Election Id
	err := p4rtClient.SetForwardingPipelineConfig(&p4_v1.SetForwardingPipelineConfigRequest{
		DeviceId:   arbMsg2.DeviceId,
		ElectionId: arbMsg2.ElectionId,
		Action:     p4_v1.SetForwardingPipelineConfigRequest_VERIFY_AND_COMMIT,
		Config: &p4_v1.ForwardingPipelineConfig{
			P4Info: &p4Info,
			Cookie: &p4_v1.ForwardingPipelineConfig_Cookie{
				Cookie: 1,
			},
		},
	})
	if err != nil {
		log.Fatal(err)
	}

	// Write is not associated with any sessions, but we have to use the master's
	err = p4rtClient.Write(&p4_v1.WriteRequest{
		DeviceId:   arbMsg2.DeviceId,
		ElectionId: arbMsg2.ElectionId,
		Updates: wbb.AclWbbIngressTableEntryGet([]*wbb.AclWbbIngressTableEntryInfo{
			&wbb.AclWbbIngressTableEntryInfo{
				Type:          p4_v1.Update_INSERT,
				EtherType:     0x6007,
				EtherTypeMask: 0xFFFF,
			},
			&wbb.AclWbbIngressTableEntryInfo{
				Type:    p4_v1.Update_INSERT,
				IsIpv4:  0x1,
				Ttl:     0x1,
				TtlMask: 0xFF,
			},
		}),
		Atomicity: p4_v1.WriteRequest_CONTINUE_ON_ERROR,
	})
	if err != nil {
		log.Fatal(err)
	}

	// Send L3 packet to ingress
	err = p4rtClient.StreamChannelSendMsg(
		sessionId1, &p4_v1.StreamMessageRequest{
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
		log.Fatal(err)
	}

	// Try removing the current master
	p4rtClient.StreamChannelDestroy(sessionId2)

	// Read what Session 1 got - block for Seq# 3
	lastSeqNum, arbMsg, arbErr = p4rtClient.StreamChannelGetLastArbitrationResp(sessionId1, 2)
	if arbErr != nil {
		log.Fatal(arbErr)
	}
	isMaster = arbMsg.Status.Code == int32(codes.OK)
	log.Printf("'%s' '%s' Got Master(%v) %d %s", params.Clients[0].Name, sessionId1Name, isMaster, lastSeqNum, arbMsg.String())

	// XXX Add packet handling

	// Test Driver
	//ForEver:
	for {
		// XXX do things
		select {
		case <-time.After(1 * time.Second):
			//break ForEver
			// Send L2 packet to egress
			err = p4rtClient.StreamChannelSendMsg(
				sessionId1, &p4_v1.StreamMessageRequest{
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
				log.Fatal(err)
			}

			log.Printf("Going back to sleep...")
		}
	}

	p4rtClient.StreamChannelDestroy(sessionId1)

	p4rtClient.ServerDisconnect()

	// XXX Second device session still up
}
