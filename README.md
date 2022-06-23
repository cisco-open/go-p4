# go-p4

Note, when running with '-log_dir foo', make sure 'foo' directory exists.

## Description
At a high level, this package is intended to help drive P4Runtime use cases (clients and tests included).
It contains wrappers to handle connections, streams, reading from streams, managing go routines, counters, etc.
It is assumed that a higher level package (client/test) can use the go-p4 package to help drive P4Runtime P4Info specific use cases.


