package copy

import (
	"fmt"

	"github.com/opencontainers/go-digest"

	"github.com/featherbread/magic-mirror/internal/image"
	"github.com/featherbread/magic-mirror/internal/log"
	"github.com/featherbread/magic-mirror/internal/parka"
)

type platformCopier struct {
	*parka.Map[platformCopyKey, image.Manifest]

	manifests *manifestCache
	blobs     *blobCopier
}

type platformCopyKey struct {
	Src image.Image
	Dst image.Image
}

func newPlatformCopier(manifests *manifestCache, blobs *blobCopier) *platformCopier {
	c := &platformCopier{
		manifests: manifests,
		blobs:     blobs,
	}
	c.Map = parka.NewMap(c.copyPlatform)
	return c
}

func (c *platformCopier) Copy(src image.Image, dst image.Image) (image.Manifest, error) {
	return c.Map.Get(platformCopyKey{Src: src, Dst: dst})
}

func (c *platformCopier) CopyAll(dst image.Repository, srcs ...image.Image) ([]image.Manifest, error) {
	reqs := make([]platformCopyKey, len(srcs))
	for i, src := range srcs {
		reqs[i] = platformCopyKey{
			Src: src,
			Dst: image.Image{
				Repository: dst,
				Digest:     src.Digest,
			},
		}
	}
	return c.Map.Collect(reqs...)
}

func (c *platformCopier) copyPlatform(_ *parka.Handle, req platformCopyKey) (m image.Manifest, err error) {
	// We share this manifest cache with the top-level copier. The top level
	// requests both indexes and platform manifests, without knowing in advance
	// what it'll get. This level always gets platform manifests, which are
	// required to discover source blobs, so we put our requests up front to
	// fill the blob queue faster.
	c.manifests.InformFront(req.Src)
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

	dstImg := image.Image{
		Repository: req.Dst.Repository,
		Tag:        req.Dst.Tag,
		Digest:     manifest.Descriptor().Digest,
	}
	err = uploadManifest(dstImg, manifest)
	if err == nil {
		log.Verbosef("[platform]\tmirrored %s to %s", req.Src, dstImg)
	}
	return manifest, err
}
