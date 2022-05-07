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
        "log"
        "os"
        "path/filepath"
)

func initLogger() {
	err := os.MkdirAll(*outputDir, 0744)
	if err != nil {
		log.Fatal(err)
	}

	logFile := filepath.Join(*outputDir, "log.txt")
	file, err1 := os.Create(logFile)
	if err1 != nil {
		log.Fatal(err1)
	}
	logger = log.New(file, "", log.LstdFlags|log.Lshortfile)
}
