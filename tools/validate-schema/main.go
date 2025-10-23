package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"log"
	"os"

	"github.com/xeipuuv/gojsonschema"
	"gopkg.in/yaml.v3"
)

func main() {
	// Define command-line flags
	filePath := flag.String("file", "", "Path to a file")
	format := flag.String("format", "json", "The data format to use (json or yaml)")
	schemaPath := flag.String("schema", "", "Path to the schema")
	flag.Parse()

	// Validate required flags
	if *filePath == "" || *schemaPath == "" {
		flag.Usage()
		log.Fatal("Missing required flags: --file and/or --schema")
	}

	// Read the data file
	data := map[string]any{}
	dataBytes, err := os.ReadFile(*filePath)
	if err != nil {
		log.Fatalf("reading data file: %v", err)
	}
	fixedDataBytes := bytes.ReplaceAll(dataBytes, []byte("True"), []byte("true"))
	fixedDataBytes = bytes.ReplaceAll(fixedDataBytes, []byte("False"), []byte("false"))

	// Unmarshal the data into a map
	switch *format {
	case "json":
		if err := json.Unmarshal(fixedDataBytes, &data); err != nil {
			log.Fatalf("unmarshaling json data: %v", err)
		}
	case "yaml":
		if err := yaml.Unmarshal(fixedDataBytes, &data); err != nil {
			log.Fatalf("unmarshaling yaml data: %v", err)
		}
	default:
		log.Fatalf("unknown data format: %s", *format)
	}

	// Load schema
	schemaBytes, err := os.ReadFile(*schemaPath)
	if err != nil {
		log.Fatalf("reading schema file: %v", err)
	}
	schemaLoader := gojsonschema.NewStringLoader(string(schemaBytes))
	schema, err := gojsonschema.NewSchema(schemaLoader)
	if err != nil {
		log.Fatalf("loading schema: %v", err)
	}

	// Convert data to JSON for validation
	dataJSON, err := json.Marshal(data)
	if err != nil {
		log.Fatalf("marshaling data for validation: %v", err)
	}

	documentLoader := gojsonschema.NewStringLoader(string(dataJSON))
	result, err := schema.Validate(documentLoader)
	if err != nil {
		log.Fatalf("validating data: %v", err)
	}

	if !result.Valid() {
		log.Println("Data validation failed:")
		for _, desc := range result.Errors() {
			log.Printf("- %s\n", desc)
		}
		log.Fatal("Data validation failed")
	}

	log.Println("Data validation successful")
}
