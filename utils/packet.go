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
package utils

import (
	"github.com/golang/glog"
	"github.com/google/gopacket"
	"github.com/google/gopacket/layers"
)

// import net
// MAC e.g. net.HardwareAddr{0xFF, 0xAA, 0xFA, 0xAA, 0xFF, 0xAA}
// IP e.g. net.IP{10, 0, 0, 1}
func PacketICMPEchoRequestGet(l2 bool, srcMac []byte, dstMac []byte,
	srcIP []byte, dstIP []byte, payLoadLen uint) []byte {
	var i uint

	buf := gopacket.NewSerializeBuffer()
	opts := gopacket.SerializeOptions{
		FixLengths:       true,
		ComputeChecksums: true,
	}
	pktIP := &layers.IPv4{
		Version:  4,
		TTL:      64,
		SrcIP:    srcIP,
		DstIP:    dstIP,
		Protocol: layers.IPProtocolICMPv4,
		//etc
	}
	pktICMP := &layers.ICMPv4{
		TypeCode: layers.ICMPv4TypeEchoRequest,
		Id:       0x159,
	}
	payload := []byte{}
	for i = 0; i < payLoadLen; i++ {
		payload = append(payload, byte(i))
	}

	var err error
	if l2 {
		pktEth := &layers.Ethernet{
			SrcMAC:       srcMac,
			DstMAC:       dstMac,
			EthernetType: layers.EthernetTypeIPv4,
		}
		err = gopacket.SerializeLayers(buf, opts,
			pktEth, pktIP, pktICMP, gopacket.Payload(payload),
		)
	} else {
		err = gopacket.SerializeLayers(buf, opts,
			pktIP, pktICMP, gopacket.Payload(payload),
		)
	}
	if err != nil {
		glog.Fatal(err)
	}

	return buf.Bytes()
}
