package image

import v1 "github.com/opencontainers/image-spec/specs-go/v1"

type MediaType string

const (
	OCIIndexMediaType    = MediaType(v1.MediaTypeImageIndex)
	DockerIndexMediaType = MediaType("application/vnd.docker.distribution.manifest.list.v2+json")

	OCIManifestMediaType    = MediaType(v1.MediaTypeImageManifest)
	DockerManifestMediaType = MediaType("application/vnd.docker.distribution.manifest.v2+json")
)

func (mt MediaType) IsIndex() bool {
	return mt == OCIIndexMediaType || mt == DockerIndexMediaType
}

func (mt MediaType) IsManifest() bool {
	return mt == OCIManifestMediaType || mt == DockerManifestMediaType
}

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
