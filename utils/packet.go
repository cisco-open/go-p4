/*
 * ------------------------------------------------------------------
 * May, 2022, Reda Haddad
 *
 * Copyright (c) 2022 by cisco Systems, Inc.
 * All rights reserved.
 * ------------------------------------------------------------------
 */
package utils

import (
	"github.com/google/gopacket"
	"github.com/google/gopacket/layers"
	"log"
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
		log.Fatal(err)
	}

	return buf.Bytes()
}
