package copy

import (
	"log"

	"go.alexhamlin.co/magic-mirror/internal/image"
)

// blobTracer downloads all of the manifest information for the images provided
// to it, and registers the image's repository as a valid source for every blob
// referenced in each manifest.
//
// A blobTracer is typically used with the destination images targeted by a copy
// operation. In the event that a multi-platform image index at the destination
// is already up to date with its source, there is normally no reason to
// download its referenced platform-level manifests when we could instead move
// on to check new sources. However, the blob information in these manifests can
// still be valuable to optimize future copies to the same registry, so the
// blobTracer downloads these manifests in the background using a separate
// manifest cache. (TODO: Move this documentation elsewhere.)
type blobTracer struct {
	manifests *manifestCache
	blobs     *blobCopier
}

func newBlobTracer(manifests *manifestCache, blobs *blobCopier) *blobTracer {
	d := &blobTracer{
		manifests: manifests,
		blobs:     blobs,
	}
	return d
}

// QueueForTracing submits the provided image for tracing, without waiting for
// the trace to complete.
func (d *blobTracer) Trace(repo image.Repository, manifest image.ManifestKind) {
	manifestType := manifest.GetMediaType()
	if manifestType.IsIndex() {
		d.queueManifestsFromIndex(repo, manifest.(image.Index))
		return
	}
	if !manifestType.IsManifest() {
		return
	}

	parsed := manifest.(image.Manifest).Parsed()
	d.blobs.RegisterSource(parsed.Config.Digest, repo)
	for _, layer := range parsed.Layers {
		d.blobs.RegisterSource(layer.Digest, repo)
	}
	log.Printf("[dest]\tdiscovered existing blobs for %s@%s", repo, manifest.Descriptor().Digest)
}

func (d *blobTracer) queueManifestsFromIndex(repo image.Repository, index image.Index) {
	descriptors := index.Parsed().Manifests
	for _, desc := range descriptors {
		desc := desc
		go func() {
			manifest, err := d.manifests.Get(image.Image{Repository: repo, Digest: desc.Digest})
			if err == nil {
				d.Trace(repo, manifest)
			}
		}()
	}
}
