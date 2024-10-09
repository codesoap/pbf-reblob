package main

import (
	"os"
	"testing"
)

func BenchmarkStreamBlobs(b *testing.B) {
	cfg := config{
		maxBlobSize: 4 * 1024 * 1024,
		// wget https://download.geofabrik.de/europe/germany/bremen-latest.osm.pbf
		//inFile:  "/home/richard/Large_Files/sachsen-latest.osm.pbf",
		//outFile: "/home/richard/Large_Files/sachsen-latest.fat16.osm.pbf",
		inFile:      "/tmp/bremen-latest.osm.pbf",
		outFile:     "/tmp/bremen-latest.fattmp.osm.pbf",
		compression: "zstd",
	}
	os.Remove(cfg.outFile)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		reblob(cfg)
	}
}
