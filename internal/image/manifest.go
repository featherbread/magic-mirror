package image

import v1 "github.com/opencontainers/image-spec/specs-go/v1"

type Index v1.Index

func (i Index) Validate() error {
	for _, manifest := range i.Manifests {
		if err := manifest.Digest.Validate(); err != nil {
			return err
		}
	}
	return nil
}

type Manifest v1.Manifest

func (m Manifest) Validate() error {
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
