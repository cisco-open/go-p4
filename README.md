# go-p4

For bash env, setup go to build:
```
export GOROOT=/sw/packages/xr/go/1.17.1
export GOCACHE=/nobackup/$USER/go_cache/go-build
export PATH=$GOROOT/bin:$PATH
```
# Code Development
Formatting:
```
gofmt -w *.go
```

Building:
```
go build
```

When adding a new package in go.mod
```
go get wwwin-github.cisco.com/rehaddad/go-p4
```

# TODO
- Use JSON to parameterize the test
    - e.g. spawn N clients with different server IP and port
- The test is designed to have
    - Driver: This is the class that drives the test cases
    - Clients: These are the classes that talk to the P4RT Server
    - Investigate ability to synchronize client "send"
- Create client class to manage connections and GO routines
    - Keep state about the client that can be queried from driver
    - The client uses buffered channels to communicate with the driver:
        - Write/Read
        - Stream, this can further demux packets from other messages
    - The Driver can then watch for channel callbacks from various clients
        a wait() wrapper can listen on desired channel and timeout
    - Messages in channels should be as per p4runtime.proto
    - Driver can then interact with clients through (requires some design):
        - level 1 i.e. send raw messages (think of these as channel wrappers)
            These can be 1-to-1 channels to rpc?
        - level 2 i.e. some preset functions that drive level 1 to some state
            E.g. establish mastership

- scale session management with different/same device-id/LC/node in Paralell; 18LC * 4NP * 4[Primary+Backup] = 288/Box
- API to call the rpc/functions with flexibility to modify all the fields
- client lib to handle different RPC messages
- packet encode/decode module to help packet I/O [GDP, Ping, Traceroute, LLDP]
- check lib to handle the rpc response/packet I/O result
- p4info lib to handle p4info dynmically, need to set/unset, read/write the related table/fields
- need to support packet stats
- logging infra to record the message/packets
    
