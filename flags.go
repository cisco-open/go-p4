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
	"github.com/golang/glog"
	"net"
)

// Command line args
var (
	serverIP   = flag.String("server_ip", "127.0.0.1", "P4RT Server IP")
	serverPort = flag.Int("server_port", 57400, "P4RT Server Port")
	username   = flag.String("username", "", "Server username")
	password   = flag.String("password", "", "Server password")
	jsonFile   = flag.String("json_file", "./json/example.json", "JSON Params File Path")
)

func validateArgs() {
	// Validate the IP
	ip := net.ParseIP(*serverIP)
	if ip == nil {
		glog.Fatalf("Invalid Server IP: %s", *serverIP)
	}
}
