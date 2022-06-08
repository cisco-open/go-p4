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
package p4rt_client

import (
	"fmt"
	p4_v1 "github.com/p4lang/p4runtime/go/p4/v1"
	"log"
	"sync"
	"wwwin-github.cisco.com/rehaddad/go-p4/utils"
)

type P4RTClientMap struct {
	client_mu  sync.Mutex // Protects the following:
	clientsMap map[string]*P4RTClient
	// end client_mu Protection
}

func (p *P4RTClientMap) ClientAdd(params *P4RTClientParameters) (*P4RTClient, error) {
	if len(params.Name) == 0 {
		return nil, fmt.Errorf("Bad Client Name")
	}

	p4rtClient := NewP4RTClient(params)

	p.client_mu.Lock()
	defer p.client_mu.Unlock()

	if _, found := p.clientsMap[params.Name]; found {
		return nil, fmt.Errorf("Client '%s' Already exists", params.Name)
	}

	p.clientsMap[params.Name] = p4rtClient

	return p4rtClient, nil
}

func (p *P4RTClientMap) ClientGet(clientName *string) (*P4RTClient, error) {
	p.client_mu.Lock()
	if p4rtClient, found := p.clientsMap[*clientName]; found {
		p.client_mu.Unlock()
		return p4rtClient, nil
	}
	p.client_mu.Unlock()

	return nil, fmt.Errorf("Client '%s' Does Not exist", clientName)
}

// Helper function to bootstrap things from a JSON file
func (p *P4RTClientMap) InitfromJson(jsonFile *string, serverIP *string, serverPort int) (*P4RTParameters, error) {
	// Read params JSON file to configure the setup
	params, err := P4RTParameterLoad(jsonFile)
	if err != nil {
		utils.LogErrorf("%s", err)
		return nil, err
	}
	log.Printf("Params: %s", P4RTParameterToString(params))

	for index, clientParam := range params.Clients {
		if len(clientParam.ServerIP) == 0 {
			clientParam.ServerIP = *serverIP
		}
		if clientParam.ServerPort == 0 {
			clientParam.ServerPort = serverPort
		}

		newClient, nErr := p.ClientAdd(&clientParam)
		if nErr != nil {
			utils.LogErrorf("Could not add Client at Index(%d) %s", index, nErr)
			return nil, nErr
		}

		// Connect
		err = newClient.ServerConnect()
		if err != nil {
			utils.LogErrorf("Could not Connect Client at Index(%d) %s", index, err)
			return nil, err
		}

		// Establish sessions
		for sIndex, sessionParams := range clientParam.Streams {
			err = newClient.StreamChannelCreate(&sessionParams)
			if err != nil {
				utils.LogErrorf("Could not Stream Create at Index(%d) %s", sIndex, err)
				return nil, err
			}

			err = newClient.StreamChannelSendMsg(&sessionParams.Name, &p4_v1.StreamMessageRequest{
				Update: &p4_v1.StreamMessageRequest_Arbitration{
					Arbitration: &p4_v1.MasterArbitrationUpdate{
						DeviceId: sessionParams.DeviceId,
						ElectionId: &p4_v1.Uint128{
							High: sessionParams.ElectionIdH,
							Low:  sessionParams.ElectionIdL,
						},
					},
				},
			})
			if err != nil {
				utils.LogErrorf("Could not Stream SendMsg at Index(%d) %s", sIndex, err)
				return nil, err
			}
		}

	}

	return params, nil
}

func NewP4RTClientMap() *P4RTClientMap {
	clientMap := &P4RTClientMap{}
	clientMap.clientsMap = make(map[string]*P4RTClient)

	return clientMap
}
