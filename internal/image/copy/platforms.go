package copy

import (
	"encoding/json"
	"log"

	"github.com/opencontainers/go-digest"

	"go.alexhamlin.co/magic-mirror/internal/image"
	"go.alexhamlin.co/magic-mirror/internal/work"
)

type platformCopier struct {
	*work.Queue[platformCopyRequest, image.Image]

	manifests *manifestCache
	blobs     *blobCopier
}

type platformCopyRequest struct {
	From image.Image
	To   image.Image
}

func newPlatformCopier(manifests *manifestCache, blobs *blobCopier) *platformCopier {
	c := &platformCopier{
		manifests: manifests,
		blobs:     blobs,
	}
	c.Queue = work.NewQueue(0, c.handleRequest)
	return c
}

func (c *platformCopier) Copy(from image.Image, to image.Image) (image.Image, error) {
	return c.Queue.GetOrSubmit(platformCopyRequest{From: from, To: to}).Wait()
}

func (c *platformCopier) CopyAll(to image.Repository, from ...image.Image) ([]image.Image, error) {
	reqs := make([]platformCopyRequest, len(from))
	for i, from := range from {
		reqs[i] = platformCopyRequest{
			From: from,
			To: image.Image{
				Repository: to,
				Digest:     from.Digest,
			},
		}
	}
	return c.Queue.GetOrSubmitAll(reqs...).WaitAll()
}

func (c *platformCopier) handleRequest(req platformCopyRequest) (img image.Image, err error) {
	manifest, err := c.manifests.Get(req.From)
	if err != nil {
		return
	}

	var parsedManifest image.Manifest
	if err = json.Unmarshal([]byte(manifest.Body), &parsedManifest); err != nil {
		return
	}
	if err = parsedManifest.Validate(); err != nil {
		return
	}

	blobDigests := make([]digest.Digest, len(parsedManifest.Layers)+1)
	for i, layer := range parsedManifest.Layers {
		blobDigests[i] = layer.Digest
	}
	blobDigests[len(blobDigests)-1] = parsedManifest.Config.Digest
	if err = c.blobs.CopyAll(req.From.Repository, req.To.Repository, blobDigests...); err != nil {
		return
	}

	err = uploadManifest(req.To, manifest)
	if err != nil {
		return
	}

	log.Printf("[platform]\tcopied %s to %s", req.From, req.To)
	img = req.To
	return
}
