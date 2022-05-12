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
        "log"
        "os"
        "path/filepath"
)

// Set the standard logger
func UtilsInitLogger(outputDir string) {
	err := os.MkdirAll(outputDir, 0744)
	if err != nil {
		log.Fatal(err)
	}

	logFile := filepath.Join(outputDir, "log.txt")
	file, err1 := os.Create(logFile)
	if err1 != nil {
		log.Fatal(err1)
	}

	// Redirect to file
	log.SetOutput(file)
        //log.SetPrefix("")
        log.SetFlags(log.LstdFlags|log.Lmicroseconds|log.Lshortfile)
}
