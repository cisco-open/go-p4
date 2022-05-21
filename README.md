# go-p4

## Description
- The test is designed to have
    - Driver: This is the part that drives the test cases
    - Client: This is the object that talks to the P4RT Server, sets up streams, etc.

## Building the binary

For bash env, setup go to build:
```
export GOROOT=/sw/packages/xr/go/1.17.1
export GOCACHE=/nobackup/$USER/go_cache/go-build
export PATH=$GOROOT/bin:$PATH

go build
```

## Help
```
./go-p4 -h
```

# Code Development
Formatting:
```
gofmt -w *.go
```

When adding a new package in go.mod
```
go get wwwin-github.cisco.com/rehaddad/go-p4
```

# TODO Development
- Fix Packet buffers and arb resp to be per stream
- Callback function can be registered per stream or global P4RTClientMap?
- Need to support Stats e.g. Set/Write/Packets/Etc.
    
# TODO Tests
- Scale stream management with different/same device-id/LC/node in Paralell; 18LC * 4NP * 4[Primary+Backup] = 288/Box
