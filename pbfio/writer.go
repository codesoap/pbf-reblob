package pbfio

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"runtime"
	"sync"

	"github.com/codesoap/lineworker"
	"github.com/codesoap/pbf-reblob/pbfproto"
	"github.com/klauspost/compress/zlib"
	"github.com/klauspost/compress/zstd"
)

var zlibWriterPool []*zlib.Writer
var zlibWriterPoolLock sync.Mutex
var zstdEncoder *zstd.Encoder
var zstdEncoderLock sync.Mutex

// WriteBlobs writes received blobs to outFile after serializing them.
// Any errors are written to the errs channel; this channel will be
// closed before the function returns.
//
// This function is not safe for concurrent use.
func WriteBlobs(outFile string, compression string, blobs chan DecodedBlob, errs chan error) {
	defer close(errs)
	blobbers := lineworker.NewWorkerPool(runtime.NumCPU(),
		func(blob DecodedBlob) (*undecodedBlob, error) {
			return serializeBlob(compression, blob)
		})

	go feedBlobsToSerializer(blobs, blobbers)
	file, err := os.Create(outFile)
	if err != nil {
		errs <- err
		return
	}
	defer file.Close()

	for {
		blob, err := blobbers.Next()
		if err == lineworker.EOS {
			break
		} else if err != nil {
			blobbers.Stop()
			blobbers.DiscardWork()
			errs <- err
			break
		}
		if err = blob.write(file); err != nil {
			blobbers.Stop()
			blobbers.DiscardWork()
			errs <- err
			break
		}
	}

	zlibWriterPoolLock.Lock()
	for _, writer := range zlibWriterPool {
		writer.Close()
	}
	zlibWriterPoolLock.Unlock()
}

func feedBlobsToSerializer(blobs chan DecodedBlob, blobbers *lineworker.WorkerPool[DecodedBlob, *undecodedBlob]) {
	for {
		blob, ok := <-blobs
		if !ok {
			break
		}
		blobbers.Process(blob)
	}
	blobbers.Stop()
}

func serializeBlob(compression string, blob DecodedBlob) (*undecodedBlob, error) {
	var err error
	var data []byte
	if blob.HeaderBlock != nil {
		data, err = blob.HeaderBlock.MarshalVT()
	} else {
		if blob.PrimitiveBlock == nil {
			return nil, fmt.Errorf("cannot write unknown blob type")
		}
		data = rawBlobPool.Get().([]byte)
		size := blob.PrimitiveBlock.SizeVT()
		if cap(data) < size {
			data = make([]byte, size)
		}
		data = data[:size]
		_, err = blob.PrimitiveBlock.MarshalToSizedBufferVT(data)
		blob.PrimitiveBlock.ReturnToVTPool()
	}
	if err != nil {
		return nil, fmt.Errorf("could not encode blob data: %v", err)
	}
	rawBlob, err := toRawBlob(compression, data)
	rawBlobPool.Put(data)
	if err != nil {
		return nil, err
	}
	rawBlobSize := int32(len(rawBlob))
	blob.BlobHeader.Datasize = &rawBlobSize
	return &undecodedBlob{blobHeader: blob.BlobHeader, blob: rawBlob}, err
}

func (b *undecodedBlob) write(file *os.File) error {
	defer rawBlobPool.Put(b.blob)
	rawHeader, err := b.blobHeader.MarshalVT()
	if err != nil {
		return err
	}
	headerSizeBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(headerSizeBuf, uint32(len(rawHeader)))
	if _, err := file.Write(headerSizeBuf); err != nil {
		return err
	}
	if _, err = file.Write(rawHeader); err != nil {
		return err
	}
	_, err = file.Write(b.blob)
	return err
}

func toRawBlob(compression string, data []byte) ([]byte, error) {
	switch compression {
	case "raw":
		blob := &pbfproto.Blob{Data: &pbfproto.Blob_Raw{Raw: data}}
		return blob.MarshalVT()
	case "zlib":
		return toRawZlibBlob(data)
	case "zstd":
		return toRawZstdBlob(data)
	}
	return nil, fmt.Errorf("invalid compression '%s'", compression)
}

func toRawZlibBlob(data []byte) ([]byte, error) {
	b := rawBlobPool.Get().([]byte)[:0]
	buf := bytes.NewBuffer(b)
	var zlibWriter *zlib.Writer
	zlibWriterPoolLock.Lock()
	if len(zlibWriterPool) > 0 {
		zlibWriter = zlibWriterPool[len(zlibWriterPool)-1]
		zlibWriter.Reset(buf)
		zlibWriterPool = zlibWriterPool[:len(zlibWriterPool)-1]
	} else {
		zlibWriter, _ = zlib.NewWriterLevel(buf, zlib.BestCompression)
	}
	zlibWriterPoolLock.Unlock()
	defer func() {
		zlibWriterPoolLock.Lock()
		zlibWriterPool = append(zlibWriterPool, zlibWriter)
		zlibWriterPoolLock.Unlock()
	}()
	if _, err := zlibWriter.Write(data); err != nil {
		return nil, err
	}
	if err := zlibWriter.Flush(); err != nil {
		return nil, err
	}
	rawSize := int32(len(data))
	blob := &pbfproto.Blob{
		RawSize: &rawSize,
		Data:    &pbfproto.Blob_ZlibData{ZlibData: buf.Bytes()},
	}
	return blob.MarshalVT()
}

func toRawZstdBlob(data []byte) ([]byte, error) {
	var err error
	zstdEncoderLock.Lock()
	if zstdEncoder == nil {
		zstdEncoder, err = zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedBestCompression))
	}
	zstdEncoderLock.Unlock()
	if err != nil {
		return nil, err
	}
	out := rawBlobPool.Get().([]byte)[:0]
	out = zstdEncoder.EncodeAll(data, out)
	rawSize := int32(len(out))
	blob := &pbfproto.Blob{
		RawSize: &rawSize,
		Data:    &pbfproto.Blob_ZstdData{ZstdData: out},
	}
	return blob.MarshalVT()
}
