package main

import (
	"fmt"
	"io"
	"os"

	"github.com/ipfs/go-cid"
	carIndex "github.com/ipld/go-car/v2/index"
	"github.com/multiformats/go-multicodec"

	"github.com/rpcpool/yellowstone-faithful/compactindexsized"
	"github.com/rpcpool/yellowstone-faithful/indexes"
	"github.com/rpcpool/yellowstone-faithful/indexmeta"
)

type ReaderAtCloser interface {
	io.ReaderAt
	io.Closer
}

var Kind_CidToOffsetAndSize = []byte("cid-to-offset-and-size")

type CidToOffsetAndSize_Reader struct {
	file  io.Closer
	meta  *indexes.Metadata
	index *compactindexsized.DB
}

func Open_CidToOffsetAndSize(file string) (*CidToOffsetAndSize_Reader, error) {
	reader, err := os.Open(file)
	if err != nil {
		return nil, fmt.Errorf("failed to open index file: %w", err)
	}
	return OpenWithReader_CidToOffsetAndSize(reader)
}

func OpenWithReader_CidToOffsetAndSize(reader ReaderAtCloser) (*CidToOffsetAndSize_Reader, error) {
	index, err := compactindexsized.Open(reader)
	if err != nil {
		return nil, fmt.Errorf("failed to open index: %w", err)
	}
	meta, err := getDefaultMetadata(index)
	if err != nil {
		return nil, err
	}
	if !indexes.IsValidNetwork(meta.Network) {
		return nil, fmt.Errorf("invalid network")
	}
	if meta.RootCid == cid.Undef {
		return nil, fmt.Errorf("root cid is undefined")
	}
	if err := meta.AssertIndexKind(Kind_CidToOffsetAndSize); err != nil {
		return nil, err
	}
	return &CidToOffsetAndSize_Reader{
		file:  reader,
		meta:  meta,
		index: index,
	}, nil
}

func (r *CidToOffsetAndSize_Reader) Get(cid_ cid.Cid) (*indexes.OffsetAndSize, error) {
	if cid_ == cid.Undef {
		return nil, fmt.Errorf("cid is undefined")
	}
	key := cid_.Bytes()
	value, err := r.index.Lookup(key)
	if err != nil {
		return nil, err
	}
	oas := &indexes.OffsetAndSize{}
	if err := oas.FromBytes(value); err != nil {
		return nil, err
	}
	return oas, nil
}

func (r *CidToOffsetAndSize_Reader) Close() error {
	return r.file.Close()
}

// Meta returns the metadata for the index.
func (r *CidToOffsetAndSize_Reader) Meta() *indexes.Metadata {
	return r.meta
}

func (r *CidToOffsetAndSize_Reader) Prefetch(b bool) {
	r.index.Prefetch(b)
}

// getDefaultMetadata gets and validates the metadata from the index.
// Will return an error if some of the metadata is missing.
func getDefaultMetadata(index *compactindexsized.DB) (*indexes.Metadata, error) {
	out := &indexes.Metadata{}
	meta := index.Header.Metadata

	indexKind, ok := meta.Get(indexmeta.MetadataKey_Kind)
	if ok {
		out.IndexKind = indexKind
	} else {
		return nil, fmt.Errorf("metadata.kind is empty (index kind)")
	}

	epochBytes, ok := meta.Get(indexmeta.MetadataKey_Epoch)
	if ok {
		out.Epoch = indexes.BtoUint64(epochBytes)
	} else {
		return nil, fmt.Errorf("metadata.epoch is empty")
	}

	rootCidBytes, ok := meta.Get(indexmeta.MetadataKey_RootCid)
	if ok {
		var err error
		out.RootCid, err = cid.Cast(rootCidBytes)
		if err != nil {
			return nil, err
		}
	} else {
		return nil, fmt.Errorf("metadata.rootCid is empty")
	}

	networkBytes, ok := meta.Get(indexmeta.MetadataKey_Network)
	if ok {
		out.Network = indexes.Network(networkBytes)
	} else {
		return nil, fmt.Errorf("metadata.network is empty")
	}

	return out, nil
}

func NewYellowstoneIndex(indexUrl string) (carIndex.Index, error) {
	indexReader := NewHTTPReaderAt(indexUrl)
	r, err := OpenWithReader_CidToOffsetAndSize(indexReader)
	if err != nil {
		return nil, err
	}
	w := &CarIndexWrapper{inner: r}
	return w, nil
}

type CarIndexWrapper struct {
	inner *CidToOffsetAndSize_Reader
}

func (i *CarIndexWrapper) Codec() multicodec.Code {
	//TODO implement me
	panic("implement me")
}

func (i *CarIndexWrapper) Marshal(w io.Writer) (uint64, error) {
	//TODO implement me
	panic("implement me")
}

func (i *CarIndexWrapper) Unmarshal(r io.Reader) error {
	//TODO implement me
	panic("implement me")
}

func (i *CarIndexWrapper) Load(records []carIndex.Record) error {
	//TODO implement me
	panic("implement me")
}

func (i *CarIndexWrapper) GetAll(c cid.Cid, f func(uint64) bool) error {
	offsetAndSize, err := i.inner.Get(c)
	if err != nil {
		return err
	}

	f(offsetAndSize.Offset)
	return nil
}

var _ carIndex.Index = (*CarIndexWrapper)(nil)
