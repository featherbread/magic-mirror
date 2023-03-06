package image

import (
	"encoding/json"

	"github.com/opencontainers/go-digest"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
)

type ManifestKind interface {
	Descriptor() v1.Descriptor
	Validate() error
}

type Index interface {
	ManifestKind
	ToParsed() ParsedIndex
}

var (
	_ Index = RawIndex{}
	_ Index = ParsedIndex{}
)

type RawIndex struct {
	ParsedIndex
	Raw json.RawMessage
}

func (ri RawIndex) Descriptor() v1.Descriptor {
	return v1.Descriptor{
		MediaType: ri.ParsedIndex.MediaType,
		Digest:    digest.Canonical.FromBytes(ri.Raw),
		Size:      int64(len(ri.Raw)),
	}
}

func (ri RawIndex) ToParsed() ParsedIndex {
	return ri.ParsedIndex
}

func (ri RawIndex) MarshalJSON() ([]byte, error) {
	return ri.Raw, nil
}

func (ri *RawIndex) UnmarshalJSON(text []byte) error {
	*ri = RawIndex{Raw: text}
	return json.Unmarshal(text, &ri.ParsedIndex)
}

type ParsedIndex v1.Index

func (i ParsedIndex) ToParsed() ParsedIndex { return i }

func (i ParsedIndex) Descriptor() v1.Descriptor {
	content, err := json.Marshal(i)
	if err != nil {
		panic(err)
	}
	return v1.Descriptor{
		MediaType: i.MediaType,
		Digest:    digest.Canonical.FromBytes(content),
		Size:      int64(len(content)),
	}
}

func (i ParsedIndex) Validate() error {
	for _, manifest := range i.Manifests {
		if err := manifest.Digest.Validate(); err != nil {
			return err
		}
	}
	return nil
}

type Manifest interface {
	ManifestKind
	ToParsed() ParsedManifest
}

var (
	_ Manifest = RawManifest{}
	_ Manifest = ParsedManifest{}
)

type RawManifest struct {
	ParsedManifest
	Raw json.RawMessage
}

func (rm RawManifest) Descriptor() v1.Descriptor {
	return v1.Descriptor{
		MediaType: rm.ParsedManifest.MediaType,
		Digest:    digest.Canonical.FromBytes(rm.Raw),
		Size:      int64(len(rm.Raw)),
	}
}

func (rm RawManifest) ToParsed() ParsedManifest {
	return rm.ParsedManifest
}

func (rm RawManifest) MarshalJSON() ([]byte, error) {
	return rm.Raw, nil
}

func (rm *RawManifest) UnmarshalJSON(text []byte) error {
	*rm = RawManifest{Raw: text}
	return json.Unmarshal(text, &rm.ParsedManifest)
}

type ParsedManifest v1.Manifest

func (m ParsedManifest) ToParsed() ParsedManifest { return m }

func (m ParsedManifest) Descriptor() v1.Descriptor {
	content, err := json.Marshal(m)
	if err != nil {
		panic(err)
	}
	return v1.Descriptor{
		MediaType: m.MediaType,
		Digest:    digest.Canonical.FromBytes(content),
		Size:      int64(len(content)),
	}
}

func (m ParsedManifest) Validate() error {
	if err := m.Config.Digest.Validate(); err != nil {
		return err
	}
	for _, layer := range m.Layers {
		if err := layer.Digest.Validate(); err != nil {
			return err
		}
	}
	return nil
}
