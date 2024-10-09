package main

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"

	"github.com/codesoap/pbf-reblob/pbfio"
)

type config struct {
	maxBlobSize     int
	verbose         bool
	inFile, outFile string
	compression     string
}

func readFlags(cfg *config) {
	flag.Usage = func() {
		fmt.Fprintln(os.Stderr,
			"Usage:\n  pbf-reblob [-v] [-s <size>] [-c <compression>] <IN_FILE> <OUT_FILE>")
		fmt.Fprintln(os.Stderr, "Options:")
		flag.PrintDefaults()
	}
	flag.BoolVar(&cfg.verbose, "v", false, "verbose")
	flag.StringVar(&cfg.compression, "c", "zlib", "output compression; either 'raw', 'zlib' or 'zstd'")
	sizep := flag.String("s", "16M", "uncompressed blob size limit; suffixes 'k' and 'M' allowed")
	flag.Parse()
	size := *sizep

	if flag.NArg() != 2 {
		flag.Usage()
		os.Exit(1)
	}
	cfg.inFile = flag.Arg(0)
	cfg.outFile = flag.Arg(1)
	if _, err := os.Stat(cfg.outFile); !errors.Is(err, os.ErrNotExist) {
		fmt.Fprintf(os.Stderr, "The file '%s' already exists.\n", cfg.outFile)
		os.Exit(1)
	}

	if cfg.compression != "raw" &&
		cfg.compression != "zlib" &&
		cfg.compression != "zstd" {
		flag.Usage()
		os.Exit(1)
	}
	setMaxBlobSize(cfg, size)
}

func setMaxBlobSize(cfg *config, size string) {
	if size == "" {
		fmt.Fprintln(os.Stderr, "Error: Empty size given.")
		os.Exit(1)
	}
	mult := 1
	lastChar := size[len(size)-1]
	if lastChar == 'k' || lastChar == 'K' {
		mult = 1024
		size = size[:len(size)-1]
	} else if lastChar == 'm' || lastChar == 'M' {
		mult = 1024 * 1024
		size = size[:len(size)-1]
	}
	var err error
	if cfg.maxBlobSize, err = strconv.Atoi(size); err != nil {
		format := "Error: Could not understand given size '%s': %v\n"
		fmt.Fprintf(os.Stderr, format, size, err)
		os.Exit(1)
	}
	cfg.maxBlobSize *= mult
	if cfg.maxBlobSize < 1024 {
		format := "Error: Size %d is too small. Use at least 1024.\n"
		fmt.Fprintf(os.Stderr, format, cfg.maxBlobSize)
		os.Exit(1)
	} else if cfg.maxBlobSize > 32*1024*1024 {
		format := "Error: Size %d is too large. Use at most 32M.\n"
		fmt.Fprintf(os.Stderr, format, cfg.maxBlobSize)
		os.Exit(1)
	}
}

func main() {
	var cfg config
	readFlags(&cfg)
	reblob(cfg)
}

func reblob(cfg config) {
	// FIXME: os.Exit ignores defers
	blobsIn := make(chan pbfio.DecodedBlob)
	go pbfio.StreamBlobs(cfg.inFile, blobsIn)
	osmHeader, ok := <-blobsIn
	if !ok {
		fmt.Fprintln(os.Stderr, "Error: Could not read OSMHeader blob.")
		os.Exit(1)
	}
	err := validateOSMHeader(osmHeader)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: Invalid OSMHeader: %v\n", err)
		os.Exit(1)
	}

	blobsOut := make(chan pbfio.DecodedBlob)
	errs := make(chan error)
	go pbfio.WriteBlobs(cfg.outFile, cfg.compression, blobsOut, errs)
	success := false
	defer func() {
		if !success {
			os.Remove(cfg.outFile)
		}
	}()
	blobsOut <- osmHeader

	var outBlob *pbfio.DecodedBlob
	for {
		stop := false
		select {
		case blob, ok := <-blobsIn:
			if ok {
				if outBlob, err = processBlob(blob, outBlob, blobsOut, cfg); err != nil {
					fmt.Fprintf(os.Stderr, "Error: Could not process blob: %v\n", err)
					os.Exit(1)
				}
			} else {
				stop = true
			}
		case err, ok := <-errs:
			if ok {
				fmt.Fprintf(os.Stderr, "Error: Could not write blob: %v\n", err)
				os.Exit(1)
			}
		}
		if stop {
			break
		}
	}
	if cfg.verbose {
		log.Printf("Info: Writing blob with raw size %2.3f MiB",
			float64(outBlob.PrimitiveBlock.SizeVT())/1024/1024)
	}
	blobsOut <- *outBlob
	close(blobsOut)
	if err, ok := <-errs; ok {
		fmt.Fprintf(os.Stderr, "Could not write blob: %v\n", err)
		os.Exit(1)
	}
	success = true
}

func validateOSMHeader(osmHeader pbfio.DecodedBlob) error {
	if osmHeader.Err != nil {
		return osmHeader.Err
	} else if *osmHeader.BlobHeader.Type != "OSMHeader" {
		return fmt.Errorf("expected blob of type 'OSMHeader' but got '%s'",
			*osmHeader.BlobHeader.Type)
	}
	for _, reqFeature := range osmHeader.HeaderBlock.RequiredFeatures {
		if reqFeature != "OsmSchema-V0.6" &&
			reqFeature != "DenseNodes" &&
			reqFeature != "HistoricalInformation" {
			return fmt.Errorf("unsupported feature '%s' is required", reqFeature)
		}
	}
	return nil
}

func processBlob(blob pbfio.DecodedBlob, outBlob *pbfio.DecodedBlob, blobsOut chan pbfio.DecodedBlob, cfg config) (*pbfio.DecodedBlob, error) {
	if *blob.BlobHeader.Type != "OSMData" {
		return nil, fmt.Errorf("unexpected blob type '%s'.\n", *blob.BlobHeader.Type)
	} else if blob.Err != nil {
		return nil, fmt.Errorf("could not read blob: %v\n", blob.Err)
	} else if outBlob == nil {
		newStrings = nil
		outBlob = &blob
		outBlob.PrimitiveBlock.ClearGroupSizeCache()
		if outBlob.PrimitiveBlock.MySize() >= cfg.maxBlobSize {
			fmt.Fprintln(os.Stderr,
				"Warning: A blob from the input file is already too large. Still using it.")
		}
	} else {
		// Avoid cloning outBlock for performance:
		outBlock := outBlob.PrimitiveBlock
		origStringtableLen := len(outBlock.Stringtable.S)
		origGroupLen := len(outBlock.Primitivegroup)

		testBlock := blob.PrimitiveBlock.CloneVT()
		ok := merge(outBlock, testBlock)
		if !ok || outBlock.MySize() >= cfg.maxBlobSize {
			// Restore outBlob to state before merge:
			outBlock.Stringtable.S = outBlock.Stringtable.S[:origStringtableLen]
			outBlock.Primitivegroup = outBlock.Primitivegroup[:origGroupLen]
			testBlock.ReturnToVTPool()

			if cfg.verbose {
				log.Printf("Info: Writing blob with raw size %2.3f MiB",
					float64(outBlob.PrimitiveBlock.MySize())/1024/1024)
			}
			blobsOut <- *outBlob
			outBlob = &blob
			newStrings = nil
			outBlob.PrimitiveBlock.ClearGroupSizeCache()
			if outBlob.PrimitiveBlock.MySize() >= cfg.maxBlobSize {
				fmt.Fprintln(os.Stderr,
					"Warning: A blob from the input file is already too large. Still using it.")
			}
		} else {
			blob.PrimitiveBlock.ReturnToVTPool()
		}
	}
	return outBlob, nil
}
