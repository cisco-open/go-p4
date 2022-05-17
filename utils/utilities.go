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
	"encoding/json"
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

type SessionParameters struct {
	Description string
	DeviceId    uint64
	ElectionIdH uint64
	ElectionIdL uint64
	Cookie      uint64
}

type ClientParameters struct {
	Description string
	ServerIP    string
	ServerPort  uint
	P4InfoFile  string
	Sessions    []SessionParameters
}

type GlobalParameters struct {
	Test string
}

type Parameters struct {
	Clients []ClientParameters
	Globals GlobalParameters
}

func ParameterLoad(fileName *string) (*Parameters, error) {
	var param Parameters

	jsonFile, err := ioutil.ReadFile(*fileName)
	if err != nil {
		log.Printf("ERROR Could not open file %s", jsonFile)
	} else {
		err = json.Unmarshal(jsonFile, &param)
	}
	return &param, err
}

func ParameterToString(params *Parameters) string {
	data, _ := json.Marshal(params)
	return string(data)
}
