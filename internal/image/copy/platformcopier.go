package copy

import (
	"encoding/json"
	"log"

	"go.alexhamlin.co/magic-mirror/internal/image"
	"go.alexhamlin.co/magic-mirror/internal/work"
)

type platformCopier struct {
	*work.Queue[platformCopyRequest, work.NoValue]

	manifests *manifestDownloader
	blobs     *BlobCopier
}

type platformCopyRequest struct {
	From image.Image
	To   image.Repository
}

func newPlatformCopier(workers int, manifests *manifestDownloader, blobs *BlobCopier) *platformCopier {
	c := &platformCopier{
		manifests: manifests,
		blobs:     blobs,
	}
	c.Queue = work.NewQueue(workers, work.NoValueHandler(c.handleRequest))
	return c
}

func (c *platformCopier) Copy(from image.Image, to image.Repository) error {
	_, err := c.Queue.GetOrSubmit(platformCopyRequest{From: from, To: to}).Wait()
	return err
}

func (c *platformCopier) CopyAll(to image.Repository, from ...image.Image) error {
	reqs := make([]platformCopyRequest, len(from))
	for i, img := range from {
		reqs[i] = platformCopyRequest{From: img, To: to}
	}
	_, err := c.Queue.GetOrSubmitAll(reqs...).WaitAll()
	return err
}

func (c *platformCopier) handleRequest(req platformCopyRequest) error {
	manifestResponse, err := c.manifests.Get(req.From)
	if err != nil {
		return err
	}

	var manifest struct {
		Config struct {
			Digest image.Digest `json:"digest"`
		} `json:"config"`
		Layers []struct {
			Digest image.Digest `json:"digest"`
		} `json:"layers"`
	}
	if err := json.Unmarshal([]byte(manifestResponse.Body), &manifest); err != nil {
		return err
	}

	blobDigests := make([]image.Digest, len(manifest.Layers)+1)
	for i, layer := range manifest.Layers {
		blobDigests[i] = layer.Digest
	}
	blobDigests[len(blobDigests)-1] = manifest.Config.Digest

	err = c.blobs.CopyAll(req.From.Repository, req.To, blobDigests...)
	if err != nil {
		return err
	}

	destImg := image.Image{Repository: req.To, Tag: req.From.Tag, Digest: req.From.Digest}
	err = uploadManifest(destImg, manifestResponse.ContentType, manifestResponse.Body)
	if err != nil {
		return err
	}

	log.Printf("[platform]\tcopied %s to %s", req.From, destImg.Repository)
	return nil
}
