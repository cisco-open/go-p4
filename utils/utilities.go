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
	"github.com/golang/protobuf/proto"
	p4_v1_config "github.com/p4lang/p4runtime/go/p4/config/v1"
	"io/ioutil"
	"log"
)

func P4InfoLoad(fileName *string) (p4_v1_config.P4Info, error) {
	var p4Info p4_v1_config.P4Info

	p4infoFile, err := ioutil.ReadFile(*fileName)
	if err != nil {
		log.Printf("ERROR Could not open file %s", fileName)
	} else {
		err = proto.UnmarshalText(string(p4infoFile), &p4Info)
	}

	return p4Info, err
}
