package image

import (
	"encoding/json"

	"github.com/opencontainers/go-digest"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
)

type ManifestKind interface {
	Descriptor() v1.Descriptor
	Encoded() json.RawMessage
	GetMediaType() MediaType
	Validate() error
}

type Index interface {
	ManifestKind
	Parsed() ParsedIndex
}

var (
	_ Index = RawIndex{}
	_ Index = ParsedIndex{}
)

type RawIndex struct {
	ParsedIndex
	Raw json.RawMessage
}

func (ri RawIndex) Parsed() ParsedIndex { return ri.ParsedIndex }

func (ri RawIndex) Encoded() json.RawMessage { return ri.Raw }

func (ri RawIndex) Descriptor() v1.Descriptor {
	return v1.Descriptor{
		MediaType: ri.ParsedIndex.MediaType,
		Digest:    digest.Canonical.FromBytes(ri.Raw),
		Size:      int64(len(ri.Raw)),
	}
}

func (ri RawIndex) MarshalJSON() ([]byte, error) {
	return ri.Raw, nil
}

func (ri *RawIndex) UnmarshalJSON(text []byte) error {
	*ri = RawIndex{Raw: text}
	return json.Unmarshal(text, &ri.ParsedIndex)
}

type ParsedIndex v1.Index

func (i ParsedIndex) Parsed() ParsedIndex { return i }

func (i ParsedIndex) GetMediaType() MediaType { return MediaType(i.MediaType) }

func (i ParsedIndex) Encoded() json.RawMessage {
	content, err := json.Marshal(i)
	if err != nil {
		panic(err)
	}
	return content
}

func (i ParsedIndex) Descriptor() v1.Descriptor {
	body := i.Encoded()
	return v1.Descriptor{
		MediaType: i.MediaType,
		Digest:    digest.Canonical.FromBytes(body),
		Size:      int64(len(body)),
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
	Parsed() ParsedManifest
}

var (
	_ Manifest = RawManifest{}
	_ Manifest = ParsedManifest{}
)

type RawManifest struct {
	ParsedManifest
	Raw json.RawMessage
}

func (rm RawManifest) Parsed() ParsedManifest { return rm.ParsedManifest }

func (rm RawManifest) Encoded() json.RawMessage { return rm.Raw }

func (rm RawManifest) Descriptor() v1.Descriptor {
	return v1.Descriptor{
		MediaType: rm.ParsedManifest.MediaType,
		Digest:    digest.Canonical.FromBytes(rm.Raw),
		Size:      int64(len(rm.Raw)),
	}
}

func (rm RawManifest) MarshalJSON() ([]byte, error) {
	return rm.Raw, nil
}

func (rm *RawManifest) UnmarshalJSON(text []byte) error {
	*rm = RawManifest{Raw: text}
	return json.Unmarshal(text, &rm.ParsedManifest)
}

type ParsedManifest v1.Manifest

func (m ParsedManifest) Parsed() ParsedManifest { return m }

func (m ParsedManifest) GetMediaType() MediaType { return MediaType(m.MediaType) }

func (m ParsedManifest) Encoded() json.RawMessage {
	content, err := json.Marshal(m)
	if err != nil {
		panic(err)
	}
	return content
}

func (m ParsedManifest) Descriptor() v1.Descriptor {
	body := m.Encoded()
	return v1.Descriptor{
		MediaType: m.MediaType,
		Digest:    digest.Canonical.FromBytes(body),
		Size:      int64(len(body)),
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
