package pbfio

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"runtime"
	"sync"

	"github.com/codesoap/pbf-reblob/pbfproto"

	"github.com/codesoap/lineworker"
)

// See https://wiki.openstreetmap.org/wiki/PBF_Format#File_format
const maxBlobHeaderSize = 64 * 1024

var blobHeaderMem []byte
var rawBlobPool = sync.Pool{New: func() any { return make([]byte, 0, 10*1024) }}

type undecodedBlob struct {
	decompressor *decompressor
	blobHeader   *pbfproto.BlobHeader
	blob         []byte
}

type DecodedBlob struct {
	Err        error // Any error that might have occured when reading the blob.
	BlobHeader *pbfproto.BlobHeader

	// Either HeaderBlock or PrimitiveGroup will be nil.
	HeaderBlock    *pbfproto.HeaderBlock
	PrimitiveBlock *pbfproto.PrimitiveBlock
}

// StreamBlobs will parse individual blobs from inFile and return them
// on the ret channel. If any error occurs, ret.Err will bet set and
// reading will abort. StreamBlobs will close ret.
func StreamBlobs(inFile string, ret chan DecodedBlob) {
	defer close(ret)
	decompressor := newDecompressor()
	defer decompressor.close()

	file, err := os.Open(inFile)
	if err != nil {
		ret <- DecodedBlob{
			Err: fmt.Errorf("could not open in file '%s': %v", inFile, err),
		}
		return
	}
	defer file.Close()
	dataDecoder := lineworker.NewWorkerPool(runtime.NumCPU(), decodeBlob)

	errs := make(chan error)
	go feedBlobsWithHeaders(file, decompressor, dataDecoder, errs)

	results := make(chan DecodedBlob)
	go channelResults(dataDecoder, results)

	for loop := true; loop; {
		select {
		case err, ok := <-errs:
			if loop = !ok; !loop {
				ret <- DecodedBlob{Err: err}
				dataDecoder.Stop()
				dataDecoder.DiscardWork()
			} else {
				errs = nil
			}
		case res, ok := <-results:
			if ok {
				ret <- res
				if loop = res.Err == nil; !loop {
					dataDecoder.Stop()
					dataDecoder.DiscardWork()
				}
			} else {
				results = nil
			}
		}
		if errs == nil && results == nil {
			break
		}
	}
}

func decodeBlob(in *undecodedBlob) (DecodedBlob, error) {
	defer rawBlobPool.Put(in.blob)
	// Note that out.err is not set here. Instead errors are returned.
	// The returned error will be filled into out.err in channelResults.
	out := DecodedBlob{}
	blob := &pbfproto.Blob{}
	if err := blob.UnmarshalVT(in.blob); err != nil {
		return out, err
	}
	data, err := in.decompressor.toRawData(blob)
	if err != nil {
		return out, err
	}
	defer in.decompressor.returnToBlobPool(data)
	switch *in.blobHeader.Type {
	case "OSMHeader":
		out.BlobHeader = in.blobHeader
		out.HeaderBlock = &pbfproto.HeaderBlock{}
		return out, out.HeaderBlock.UnmarshalVT(data)
	case "OSMData":
		out.BlobHeader = in.blobHeader
		//println("requesting block")
		out.PrimitiveBlock = pbfproto.PrimitiveBlockFromVTPool()
		return out, out.PrimitiveBlock.UnmarshalVT(data)
	default:
		return out, fmt.Errorf("unknown blob type '%s'", *in.blobHeader.Type)
	}
}

func feedBlobsWithHeaders(file *os.File, decompressor *decompressor, decoder *lineworker.WorkerPool[*undecodedBlob, DecodedBlob], errs chan error) {
	defer close(errs)
	defer decoder.Stop()
	for {
		blobHeaderSize, err := getBlobHeaderSize(file)
		if err == io.EOF {
			return
		} else if err != nil {
			errs <- fmt.Errorf("could not read blob header size: %v", err)
			return
		}
		blobHeaderMem, err = readAllIntoBuf(io.LimitReader(file, int64(blobHeaderSize)), blobHeaderMem)
		if err != nil {
			errs <- fmt.Errorf("could not read BlobHeader: %v", err)
			return
		}
		ub := &undecodedBlob{decompressor: decompressor}
		ub.blobHeader = &pbfproto.BlobHeader{}
		if err = ub.blobHeader.UnmarshalVT(blobHeaderMem); err != nil {
			errs <- fmt.Errorf("could not unmarshal BlobHeader: %v", err)
			return
		}
		if ub.blobHeader.Type == nil {
			errs <- fmt.Errorf("fileblock is missing type")
			return
		} else {
			ub.blob = rawBlobPool.Get().([]byte)
			ub.blob, err = readAllIntoBuf(io.LimitReader(file, int64(*ub.blobHeader.Datasize)), ub.blob)
			if err != nil {
				errs <- fmt.Errorf("could not read blob from file: %v", err)
				return
			}
			if !decoder.Process(ub) {
				// The decoder is not accepting work anymore; there must be
				// a problem elsewhere. Stop reading blobs.
				return
			}
		}
	}
}

func channelResults(decoder *lineworker.WorkerPool[*undecodedBlob, DecodedBlob], results chan DecodedBlob) {
	defer close(results)
	for {
		res, err := decoder.Next()
		if err == lineworker.EOS {
			break
		}
		res.Err = err
		results <- res
		if res.Err != nil {
			decoder.Stop()
			decoder.DiscardWork()
			break
		}
	}
}

func getBlobHeaderSize(file *os.File) (uint32, error) {
	buf := make([]byte, 4)
	if _, err := io.ReadFull(file, buf); err != nil {
		return 0, err
	}
	size := binary.BigEndian.Uint32(buf)
	if size >= maxBlobHeaderSize {
		return 0, fmt.Errorf("blobHeader size %d >= 64KiB", size)
	}
	return size, nil
}

func readAllIntoBuf(r io.Reader, b []byte) ([]byte, error) {
	// Code is mostly copied from io.ReadAll.
	b = b[:0]
	for {
		n, err := r.Read(b[len(b):cap(b)])
		b = b[:len(b)+n]
		if err != nil {
			if err == io.EOF {
				err = nil
			}
			return b, err
		}

		if len(b) == cap(b) {
			// Add more capacity (let append pick how much).
			b = append(b, 0)[:len(b)]
		}
	}
}
