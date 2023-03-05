package copy

import (
	"encoding/json"
	"log"

	"go.alexhamlin.co/magic-mirror/internal/image"
	"go.alexhamlin.co/magic-mirror/internal/work"
)

type Copier struct {
	*work.Queue[Request, work.NoValue]

	blobs     *blobCopier
	manifests *manifestCache
	platforms *platformCopier
}

type Request struct {
	From image.Image
	To   image.Image
}

func NewCopier(workers int) *Copier {
	blobs := newBlobCopier(workers)
	manifests := newManifestCache(workers)
	platforms := newPlatformCopier(0, manifests, blobs)

	c := &Copier{
		blobs:     blobs,
		manifests: manifests,
		platforms: platforms,
	}
	c.Queue = work.NewQueue(0, work.NoValueHandler(c.handleRequest))
	return c
}

func (c *Copier) Copy(from, to image.Image) error {
	_, err := c.Queue.GetOrSubmit(Request{From: from, To: to}).Wait()
	return err
}

func (c *Copier) CopyAll(reqs ...Request) error {
	_, err := c.Queue.GetOrSubmitAll(reqs...).WaitAll()
	return err
}

func (c *Copier) CloseSubmit() {
	c.Queue.CloseSubmit()
	c.platforms.CloseSubmit()
	c.manifests.CloseSubmit()
	c.blobs.CloseSubmit()
}

func (c *Copier) handleRequest(req Request) error {
	log.Printf("[image]\tstarting copy from %s to %s", req.From, req.To)

	manifest, err := c.manifests.Get(req.From)
	if err != nil {
		return err
	}

	var parsedManifest struct {
		Manifests []struct {
			Digest image.Digest `json:"digest"`
		} `json:"manifests"`
		Layers []struct {
			Digest image.Digest `json:"digest"`
		} `json:"layers"`
	}
	if err := json.Unmarshal([]byte(manifest.Body), &parsedManifest); err != nil {
		return err
	}

	if len(parsedManifest.Manifests) == 0 {
		err = c.platforms.Copy(req.From, req.To.Repository)
	} else {
		imgs := make([]image.Image, len(parsedManifest.Manifests))
		for i, m := range parsedManifest.Manifests {
			imgs[i] = image.Image{Repository: req.From.Repository, Digest: m.Digest}
		}
		err = c.platforms.CopyAll(req.To.Repository, imgs...)
	}
	if err != nil {
		return err
	}

	if len(parsedManifest.Manifests) > 0 {
		if err := uploadManifest(req.To, manifest); err != nil {
			return err
		}
	}
	log.Printf("[image]\tfully copied %s to %s", req.From, req.To)
	return nil
}
