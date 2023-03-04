package main

func main() {}

type MirrorSpec struct {
	FromReference string
	ToReference   string
	Platforms     []string
}

type Repository struct {
	Registry   string
	Repository string
}

type Digest string

type BlobMirrorRequest struct {
	Digest      Digest
	Destination Repository
}

type BlobMirrorStatus struct {
	BlobMirrorRequest
	Done chan struct{}
}

type BlobController struct {
	BlobSources map[Digest][]Repository

	CheckQueue []BlobMirrorRequest
	MountQueue []BlobMirrorRequest

	CopyStatuses map[BlobMirrorRequest]BlobMirrorStatus
	CopyQueue    []BlobMirrorRequest
}

type ManifestMirrorRequest struct {
	BlobStatuses []*BlobMirrorStatus
}
