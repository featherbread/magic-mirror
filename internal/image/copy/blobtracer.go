package copy

import (
	"log"

	"go.alexhamlin.co/magic-mirror/internal/image"
	"go.alexhamlin.co/magic-mirror/internal/work"
)

type blobTracer struct {
	*work.Queue[image.Image, work.NoValue]

	manifests *manifestCache
	blobs     *blobCopier
}

func newBlobTracer(manifests *manifestCache, blobs *blobCopier) *blobTracer {
	d := &blobTracer{
		manifests: manifests,
		blobs:     blobs,
	}
	d.Queue = work.NewQueue(0, work.NoValueHandler(d.handleRequest))
	return d
}

func (d *blobTracer) QueueForTracing(img image.Image) {
	d.Queue.GetOrSubmit(img)
}

func (d *blobTracer) QueueAllForTracing(imgs ...image.Image) {
	d.Queue.GetOrSubmitAll(imgs...)
}

func (d *blobTracer) handleRequest(img image.Image) error {
	manifest, err := d.manifests.Get(img)
	if err != nil {
		return err
	}
	if err := manifest.Validate(); err != nil {
		return err
	}

	manifestType := manifest.GetMediaType()
	switch {
	case manifestType.IsIndex():
		d.queueManifestsFromIndex(img.Repository, manifest.(image.Index))
	case manifestType.IsManifest():
		d.traceManifest(img.Repository, manifest.(image.Manifest))
		log.Printf("[dest]\tdiscovered existing blobs for %s", img)
	}
	return nil
}

func (d *blobTracer) queueManifestsFromIndex(repo image.Repository, index image.Index) {
	descriptors := index.Parsed().Manifests
	imgs := make([]image.Image, len(descriptors))
	for i, desc := range descriptors {
		imgs[i] = image.Image{Repository: repo, Digest: desc.Digest}
	}
	d.QueueAllForTracing(imgs...)
}

func (d *blobTracer) traceManifest(repo image.Repository, manifest image.Manifest) {
	parsed := manifest.Parsed()
	d.blobs.RegisterSource(parsed.Config.Digest, repo)
	for _, layer := range parsed.Layers {
		d.blobs.RegisterSource(layer.Digest, repo)
	}
}
