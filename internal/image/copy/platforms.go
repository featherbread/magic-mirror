package copy

import (
	"fmt"
	"log"

	"github.com/opencontainers/go-digest"

	"go.alexhamlin.co/magic-mirror/internal/image"
	"go.alexhamlin.co/magic-mirror/internal/work"
)

type platformCopier struct {
	*work.Queue[platformCopyRequest, image.Manifest]

	compareMode CompareMode
	manifests   *manifestCache
	blobs       *blobCopier
}

type platformCopyRequest struct {
	From image.Image
	To   image.Image
}

func newPlatformCopier(compareMode CompareMode, manifests *manifestCache, blobs *blobCopier) *platformCopier {
	c := &platformCopier{
		compareMode: compareMode,
		manifests:   manifests,
		blobs:       blobs,
	}
	c.Queue = work.NewQueue(0, c.handleRequest)
	return c
}

func (c *platformCopier) Copy(from image.Image, to image.Image) (image.Manifest, error) {
	return c.Queue.GetOrSubmit(platformCopyRequest{From: from, To: to}).Wait()
}

func (c *platformCopier) CopyAll(to image.Repository, from ...image.Image) ([]image.Manifest, error) {
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

func (c *platformCopier) handleRequest(req platformCopyRequest) (m image.Manifest, err error) {
	sourceManifest, err := c.manifests.Get(req.From)
	if err != nil {
		return
	}
	if !sourceManifest.GetMediaType().IsManifest() {
		err = fmt.Errorf("%s is a manifest list, but should be a manifest", req.From)
		return
	}

	manifest := sourceManifest.(image.Manifest)
	if err = manifest.Validate(); err != nil {
		return
	}

	layers := manifest.Parsed().Layers
	blobDigests := make([]digest.Digest, len(layers)+1)
	for i, layer := range layers {
		blobDigests[i] = layer.Digest
	}
	blobDigests[len(blobDigests)-1] = manifest.Parsed().Config.Digest
	if err = c.blobs.CopyAll(req.From.Repository, req.To.Repository, blobDigests...); err != nil {
		return
	}

	if c.compareMode == CompareModeEqual {
		err := uploadManifest(req.To, sourceManifest)
		if err == nil {
			log.Printf("[platform]\tcopied %s to %s", req.From, req.To)
		}
		return manifest, err
	}

	newManifest := manifest.Parsed()
	newManifest.MediaType = string(image.OCIManifestMediaType)
	if newManifest.Annotations == nil {
		newManifest.Annotations = make(map[string]string)
	}
	newManifest.Annotations[annotationSourceDigest] = manifest.Descriptor().Digest.String()

	newImg := image.Image{
		Repository: req.To.Repository,
		Tag:        req.To.Tag,
		Digest:     newManifest.Descriptor().Digest,
	}
	err = uploadManifest(newImg, newManifest)
	if err == nil {
		log.Printf("[platform]\tcopied %s to %s", req.From, req.To)
	}
	return newManifest, err
}
