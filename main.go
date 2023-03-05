package main

import "context"

func main() {}

type Digest string

type Repository struct {
	Registry string
	Path     string
}

type Image struct {
	Repository
	Tag string
}

type MirrorRequest struct {
	From      Image
	To        Image
	Platforms []string
}

func Mirror(req MirrorRequest) {
	var bmc BlobMirrorController

	// Get the source manifest.
	// Find all the blobs.

	var digests []Digest
	var statuses []*BlobMirrorStatus
	for _, d := range digests {
		statuses = append(statuses, bmc.RequestTransfer(d, req.From.Repository, req.To.Repository))
	}

	for _, status := range statuses {
		<-status.Ctx.Done()
	}

	// Write the destination manifest.
}

type BlobMirrorRequest struct {
	Digest Digest
	To     Repository
}

type BlobMirrorStatus struct {
	Ctx  context.Context
	done context.CancelCauseFunc
}

type BlobMirrorController struct {
	sources  map[Digest][]Repository
	statuses map[BlobMirrorRequest]*BlobMirrorStatus
}

func (bmc *BlobMirrorController) RequestTransfer(d Digest, from, to Repository) *BlobMirrorStatus {
	bmc.RegisterSource(d, from)
	return bmc.RegisterMirrorRequest(BlobMirrorRequest{d, to})
}

func (bmc *BlobMirrorController) RegisterSource(d Digest, r Repository) {
	bmc.sources[d] = append(bmc.sources[d], r)
}

func (bmc *BlobMirrorController) RegisterMirrorRequest(req BlobMirrorRequest) *BlobMirrorStatus {
	sources := bmc.sources[req.Digest]
	for range sources {
		// Identify sources within the same registry.
		// If there is one, attempt a mount operation.
		// Otherwise, select a source and transfer via the network.
	}
	return nil
}
