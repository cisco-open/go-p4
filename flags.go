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
	"log"
	"net"
)

// Command line args
var (
	outputDir  = flag.String("output_dir", "./output", "Output Directory")
	serverIP   = flag.String("server_ip", "192.168.0.1", "P4RT Server IP")
	serverPort = flag.Int("server_port", 57400, "P4RT Server Port")
	p4InfoFile = flag.String("p4_info_file", "./p4info/wbb/wbb.p4info.pb.txt", "P4 Info File Path")
	jsonFile   = flag.String("json_file", "./json/test.json", "JSON Params File Path")
)

func validateArgs() {
	// Validate the IP
	ip := net.ParseIP(*serverIP)
	if ip == nil {
		log.Fatalf("Invalid Server IP: %s", *serverIP)
	}
}