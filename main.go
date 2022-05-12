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
	"os"
	"time"
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
	defer p4rtClient.ServerDisconnect()

	// Test Driver
	for {
		// XXX do things
		select {
		case <-time.After(1 * time.Second):
			// timeout and try again
			log.Printf("Timeout - waiting on instructions")
		}
	}
}
