package copy

import (
	"log"

	"go.alexhamlin.co/magic-mirror/internal/image"
	"go.alexhamlin.co/magic-mirror/internal/work"
)

type destinationTracer struct {
	*work.Queue[image.Image, work.NoValue]

	manifests *manifestCache
	blobs     *blobCopier
}

func newDestinationTracer(manifests *manifestCache, blobs *blobCopier) *destinationTracer {
	d := &destinationTracer{
		manifests: manifests,
		blobs:     blobs,
	}
	d.Queue = work.NewQueue(0, work.NoValueHandler(d.handleRequest))
	return d
}

func (d *destinationTracer) QueueTrace(img image.Image) {
	d.Queue.GetOrSubmit(img)
}

func (d *destinationTracer) QueueTraces(imgs ...image.Image) {
	d.Queue.GetOrSubmitAll(imgs...)
}

func (d *destinationTracer) handleRequest(img image.Image) error {
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
		d.queueManifestsForIndex(img.Repository, manifest.(image.Index))
	case manifestType.IsManifest():
		d.traceManifest(img.Repository, manifest.(image.Manifest))
		log.Printf("[dest]\tdiscovered existing blobs for %s", img)
	}
	return nil
}

func (d *destinationTracer) queueManifestsForIndex(repo image.Repository, index image.Index) {
	descriptors := index.Parsed().Manifests
	imgs := make([]image.Image, len(descriptors))
	for i, desc := range descriptors {
		imgs[i] = image.Image{Repository: repo, Digest: desc.Digest}
	}
	d.QueueTraces(imgs...)
}

func (d *destinationTracer) traceManifest(repo image.Repository, manifest image.Manifest) {
	d.blobs.RegisterSource(manifest.Parsed().Config.Digest, repo)
	for _, layer := range manifest.Parsed().Layers {
		d.blobs.RegisterSource(layer.Digest, repo)
	}
}
