package copy

import (
	"encoding/json"
	"log"

	"github.com/opencontainers/go-digest"

	"go.alexhamlin.co/magic-mirror/internal/image"
	"go.alexhamlin.co/magic-mirror/internal/work"
)

type platformCopier struct {
	*work.Queue[platformCopyRequest, work.NoValue]

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
	c.Queue = work.NewQueue(0, work.NoValueHandler(c.handleRequest))
	return c
}

func (c *platformCopier) Copy(from image.Image, to image.Image) error {
	_, err := c.Queue.GetOrSubmit(platformCopyRequest{From: from, To: to}).Wait()
	return err
}

func (c *platformCopier) CopyAll(to image.Repository, from ...image.Image) error {
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
	_, err := c.Queue.GetOrSubmitAll(reqs...).WaitAll()
	return err
}

func (c *platformCopier) handleRequest(req platformCopyRequest) error {
	manifest, err := c.manifests.Get(req.From)
	if err != nil {
		return err
	}

	var parsedManifest image.Manifest
	if err := json.Unmarshal([]byte(manifest.Body), &parsedManifest); err != nil {
		return err
	}
	if err := parsedManifest.Validate(); err != nil {
		return err
	}

	blobDigests := make([]digest.Digest, len(parsedManifest.Layers)+1)
	for i, layer := range parsedManifest.Layers {
		blobDigests[i] = layer.Digest
	}
	blobDigests[len(blobDigests)-1] = parsedManifest.Config.Digest
	if err := c.blobs.CopyAll(req.From.Repository, req.To.Repository, blobDigests...); err != nil {
		return err
	}

	err = uploadManifest(req.To, manifest)
	if err != nil {
		return err
	}

	log.Printf("[platform]\tcopied %s to %s", req.From, req.To)
	return nil
}
