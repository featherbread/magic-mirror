package image

import v1 "github.com/opencontainers/image-spec/specs-go/v1"

type MediaType string

const (
	OCIIndexMediaType    = MediaType(v1.MediaTypeImageIndex)
	DockerIndexMediaType = MediaType("application/vnd.docker.distribution.manifest.list.v2+json")

	OCIManifestMediaType    = MediaType(v1.MediaTypeImageManifest)
	DockerManifestMediaType = MediaType("application/vnd.docker.distribution.manifest.v2+json")
)

var AllManifestMediaTypes = []string{
	string(OCIIndexMediaType),
	string(DockerIndexMediaType),
	string(OCIManifestMediaType),
	string(DockerManifestMediaType),
}

func (mt MediaType) IsIndex() bool {
	return mt == OCIIndexMediaType || mt == DockerIndexMediaType
}

func (mt MediaType) IsManifest() bool {
	return mt == OCIManifestMediaType || mt == DockerManifestMediaType
}

func (mt MediaType) IsOCI() bool {
	return mt == OCIIndexMediaType || mt == OCIManifestMediaType
}

func (mt MediaType) IsDocker() bool {
	return mt == DockerIndexMediaType || mt == DockerManifestMediaType
}
