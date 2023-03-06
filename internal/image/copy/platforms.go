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
	Src image.Image
	Dst image.Image
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

func (c *platformCopier) Copy(src image.Image, dst image.Image) (image.Manifest, error) {
	return c.Queue.GetOrSubmit(platformCopyRequest{Src: src, Dst: dst}).Wait()
}

func (c *platformCopier) CopyAll(dst image.Repository, srcs ...image.Image) ([]image.Manifest, error) {
	reqs := make([]platformCopyRequest, len(srcs))
	for i, src := range srcs {
		reqs[i] = platformCopyRequest{
			Src: src,
			Dst: image.Image{
				Repository: dst,
				Digest:     src.Digest,
			},
		}
	}
	return c.Queue.GetOrSubmitAll(reqs...).WaitAll()
}

func (c *platformCopier) handleRequest(req platformCopyRequest) (m image.Manifest, err error) {
	srcManifest, err := c.manifests.Get(req.Src)
	if err != nil {
		return
	}
	if !srcManifest.GetMediaType().IsManifest() {
		err = fmt.Errorf("%s is a manifest list, but should be a manifest", req.Src)
		return
	}

	manifest := srcManifest.(image.Manifest)
	if err = manifest.Validate(); err != nil {
		return
	}

	layers := manifest.Parsed().Layers
	blobDigests := make([]digest.Digest, len(layers)+1)
	for i, layer := range layers {
		blobDigests[i] = layer.Digest
	}
	blobDigests[len(blobDigests)-1] = manifest.Parsed().Config.Digest
	if err = c.blobs.CopyAll(req.Src.Repository, req.Dst.Repository, blobDigests...); err != nil {
		return
	}

	if c.compareMode == CompareModeEqual {
		err := uploadManifest(req.Dst, srcManifest)
		if err == nil {
			log.Printf("[platform]\tcopied %s to %s", req.Src, req.Dst)
		}
		return manifest, err
	}

	newManifest := manifest.Parsed()
	newManifest.MediaType = string(image.OCIManifestMediaType)
	if newManifest.Annotations == nil {
		newManifest.Annotations = make(map[string]string)
	}
	newManifest.Annotations[annotationSourceDigest] = srcManifest.Descriptor().Digest.String()

	newImg := image.Image{
		Repository: req.Dst.Repository,
		Tag:        req.Dst.Tag,
	}
	err = uploadManifest(newImg, newManifest)
	if err == nil {
		log.Printf("[platform]\tcopied %s to %s", req.Src, req.Dst)
	}
	return newManifest, err
}
