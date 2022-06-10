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
	"github.com/golang/protobuf/proto"
	p4_v1_config "github.com/p4lang/p4runtime/go/p4/config/v1"
	"io/ioutil"
	"log"
)

func P4InfoLoad(fileName *string) (p4_v1_config.P4Info, error) {
	var p4Info p4_v1_config.P4Info

	p4infoFile, err := ioutil.ReadFile(*fileName)
	if err != nil {
		log.Printf("ERROR Could not open file %s", *fileName)
	} else {
		err = proto.UnmarshalText(string(p4infoFile), &p4Info)
	}

	return p4Info, err
}
