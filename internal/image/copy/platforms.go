package copy

import (
	"encoding/json"
	"log"

	"github.com/opencontainers/go-digest"

	"go.alexhamlin.co/magic-mirror/internal/image"
	"go.alexhamlin.co/magic-mirror/internal/work"
)

type platformCopier struct {
	*work.Queue[platformCopyRequest, manifest]

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

func (c *platformCopier) Copy(from image.Image, to image.Image) (manifest, error) {
	return c.Queue.GetOrSubmit(platformCopyRequest{From: from, To: to}).Wait()
}

func (c *platformCopier) CopyAll(to image.Repository, from ...image.Image) ([]manifest, error) {
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

func (c *platformCopier) handleRequest(req platformCopyRequest) (m manifest, err error) {
	sourceManifest, err := c.manifests.Get(req.From)
	if err != nil {
		return
	}

	var parsedManifest image.Manifest
	if err = json.Unmarshal([]byte(sourceManifest.Body), &parsedManifest); err != nil {
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

	if c.compareMode == CompareModeEqual {
		err := uploadManifest(req.To, sourceManifest)
		if err == nil {
			log.Printf("[platform]\tcopied %s to %s", req.From, req.To)
		}
		return sourceManifest, err
	}

	parsedManifest.MediaType = string(image.OCIManifestMediaType)
	if parsedManifest.Annotations == nil {
		parsedManifest.Annotations = make(map[string]string)
	}
	parsedManifest.Annotations[annotationSourceDigest] = digest.Canonical.FromBytes(sourceManifest.Body).String()

	newManifestBody, err := json.Marshal(parsedManifest)
	if err != nil {
		return
	}
	newManifest := manifest{
		ContentType: string(image.OCIManifestMediaType),
		Body:        newManifestBody,
	}
	newDigest := digest.Canonical.FromBytes(newManifestBody)
	newImg := image.Image{
		Repository: req.To.Repository,
		Tag:        req.To.Tag,
		Digest:     newDigest,
	}
	err = uploadManifest(newImg, newManifest)
	if err == nil {
		log.Printf("[platform]\tcopied %s to %s", req.From, req.To)
	}
	return newManifest, err
}
