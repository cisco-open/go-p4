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
