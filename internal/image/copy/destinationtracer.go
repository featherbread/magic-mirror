package copy

import (
	"encoding/json"
	"log"

	"github.com/opencontainers/go-digest"

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

	var parsedManifest struct {
		Config struct {
			Digest digest.Digest `json:"digest"`
		} `json:"config"`
		Layers []struct {
			Digest digest.Digest `json:"digest"`
		} `json:"layers"`
		Manifests []struct {
			Digest digest.Digest `json:"digest"`
		} `json:"manifests"`
	}
	if err := json.Unmarshal([]byte(manifest.Body), &parsedManifest); err != nil {
		return err
	}

	if len(parsedManifest.Manifests) > 0 {
		imgs := make([]image.Image, len(parsedManifest.Manifests))
		for i, m := range parsedManifest.Manifests {
			if err := m.Digest.Validate(); err != nil {
				return err
			}
			imgs[i] = image.Image{Repository: img.Repository, Digest: m.Digest}
		}
		d.QueueTraces(imgs...)
	}

	for _, l := range parsedManifest.Layers {
		if err := l.Digest.Validate(); err != nil {
			return err
		}
		d.blobs.RegisterSource(l.Digest, img.Repository)
	}
	if parsedManifest.Config.Digest != "" {
		if err := parsedManifest.Config.Digest.Validate(); err != nil {
			return err
		}
		d.blobs.RegisterSource(parsedManifest.Config.Digest, img.Repository)
	}

	log.Printf("[dest]\tdiscovered existing blobs for %s", img)
	return nil
}
