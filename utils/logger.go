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
	log.SetFlags(log.LstdFlags | log.Lmicroseconds | log.Lshortfile)
}

func LogError(err string) {
	log.Print("ERROR " + err)
}

func LogErrorf(format string, args ...interface{}) {
	log.Printf("ERROR "+format, args...)
}
