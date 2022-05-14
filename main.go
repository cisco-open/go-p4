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

	// XXX Read some JSON file to configure the setup

	// For now, Setup the connection with the server
	p4rtClient := p4rt_client.NewP4RTClient(&p4rt_client.P4RTClientParams{
		Name:       "Client1",
		ServerIP:   *serverIP,
		ServerPort: *serverPort,
	})

	err := p4rtClient.ServerConnect()
	if err != nil {
		log.Fatal(err)
	}

	var streamId uint32
	streamId, err = p4rtClient.StreamChannelCreate()
	if err != nil {
		log.Fatal(err)
	}

	// XXX The set/get arbitration here may be wrapped in a single function
	// Note that, the server can send async arb resp messages
	// So, we can loop on StreamChannelGetLastArbitrationResp() unti
	// we get the expected result

	err = p4rtClient.StreamChannelSendArbitration(streamId, &p4_v1.StreamMessageRequest{
		Update: &p4_v1.StreamMessageRequest_Arbitration{
			Arbitration: &p4_v1.MasterArbitrationUpdate{
				DeviceId: 3,
				ElectionId: &p4_v1.Uint128{
					High: 0,
					Low:  1,
				},
			},
		},
	})
	if err != nil {
		log.Fatal(err)
	}

	lastSeqNum, arbMsg, arbErr := p4rtClient.StreamChannelGetLastArbitrationResp(streamId, 1)
	if arbErr != nil {
		log.Fatal(arbErr)
	}
	isMaster := arbMsg.Status.Code == int32(codes.OK)

	log.Printf("Got Master(%v) %d %s", isMaster, lastSeqNum, arbMsg.String())

	p4Info, p4InfoErr := utils.P4InfoLoad(p4InfoFile)
	if p4InfoErr != nil {
		log.Fatal(p4InfoErr)
	}

	err = p4rtClient.SetForwardingPipelineConfig(&p4_v1.SetForwardingPipelineConfigRequest{
		DeviceId: 3,
		ElectionId: &p4_v1.Uint128{
			High: 0,
			Low:  1,
		},
		Action: p4_v1.SetForwardingPipelineConfigRequest_VERIFY_AND_COMMIT,
		Config: &p4_v1.ForwardingPipelineConfig{
			P4Info: &p4Info,
			Cookie: &p4_v1.ForwardingPipelineConfig_Cookie{
				Cookie: 15,
			},
		},
	})

	// Second session same client

	var streamId2 uint32
	streamId2, err = p4rtClient.StreamChannelCreate()
	if err != nil {
		log.Fatal(err)
	}

	// XXX The set/get arbitration here may be wrapped in a single function
	// Note that, the server can send async arb resp messages
	// So, we can loop on StreamChannelGetLastArbitrationResp() unti
	// we get the expected result

	err = p4rtClient.StreamChannelSendArbitration(streamId2, &p4_v1.StreamMessageRequest{
		Update: &p4_v1.StreamMessageRequest_Arbitration{
			Arbitration: &p4_v1.MasterArbitrationUpdate{
				DeviceId: 3,
				ElectionId: &p4_v1.Uint128{
					High: 0,
					Low:  100,
				},
			},
		},
	})
	if err != nil {
		log.Fatal(err)
	}

	lastSeqNum2, arbMsg2, arbErr2 := p4rtClient.StreamChannelGetLastArbitrationResp(streamId2, 1)
	if arbErr2 != nil {
		log.Fatal(arbErr2)
	}
	isMaster2 := arbMsg2.Status.Code == int32(codes.OK)

	log.Printf("Got Master(%v) %d %s", isMaster2, lastSeqNum2, arbMsg2.String())

	// Master2 should have preempted
	lastSeqNum, arbMsg, arbErr = p4rtClient.StreamChannelGetLastArbitrationResp(streamId, 2)
	if arbErr != nil {
		log.Fatal(arbErr)
	}
	isMaster = arbMsg.Status.Code == int32(codes.OK)
	log.Printf("Got Master(%v) %d %s", isMaster, lastSeqNum, arbMsg.String())

	entity := wbb.AclWbbIngressTableEntryGet(&wbb.AclWbbIngressTableEntryInfo{
		IsIpv4:          1,
		IsIpv6:          1,
		EtherType:       0x6007,
		EtherTypeMask:   0xFFFF,
		Ttl:             0x1,
		TtlMask:         0xFF,
		OuterVlanId:     4000,
		OuterVlanIdMask: 0xFFF,
	})
	log.Printf("%s", entity)

	// Try removing the master
	p4rtClient.StreamChannelDestroy(streamId2)

	// Test Driver
ForEver:
	for {
		// XXX do things
		select {
		case <-time.After(5 * time.Second):
			break ForEver
		}
	}

	p4rtClient.StreamChannelDestroy(streamId)

	p4rtClient.ServerDisconnect()
}
