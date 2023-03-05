package manifest

import (
	"encoding/json"
	"log"

	"go.alexhamlin.co/magic-mirror/internal/image"
	"go.alexhamlin.co/magic-mirror/internal/work"
)

type ImageCopier struct {
	*work.Queue[ImageRequest, work.NoValue]

	manifestDownloader *Downloader
	platformCopier     *PlatformCopier
}

type ImageRequest struct {
	From image.Image
	To   image.Image
}

func NewImageCopier(workers int, manifestDownloader *Downloader, platformCopier *PlatformCopier) *ImageCopier {
	c := &ImageCopier{
		manifestDownloader: manifestDownloader,
		platformCopier:     platformCopier,
	}
	c.Queue = work.NewQueue(workers, work.NoValueHandler(c.handleRequest))
	return c
}

func (c *ImageCopier) Copy(from, to image.Image) error {
	_, err := c.Queue.GetOrSubmit(ImageRequest{From: from, To: to}).Wait()
	return err
}

func (c *ImageCopier) CopyAll(reqs ...ImageRequest) error {
	_, err := c.Queue.GetOrSubmitAll(reqs...).WaitAll()
	return err
}

func (c *ImageCopier) handleRequest(req ImageRequest) error {
	log.Printf("[image]\tstarting copy from %s to %s", req.From, req.To)

	manifestResponse, err := c.manifestDownloader.Get(req.From)
	if err != nil {
		return err
	}

	var manifest struct {
		Manifests []struct {
			Digest image.Digest `json:"digest"`
		} `json:"manifests"`
		Layers []struct {
			Digest image.Digest `json:"digest"`
		} `json:"layers"`
	}
	if err := json.Unmarshal([]byte(manifestResponse.Body), &manifest); err != nil {
		return err
	}

	if len(manifest.Manifests) == 0 {
		err = c.platformCopier.Copy(req.From, req.To.Repository)
	} else {
		imgs := make([]image.Image, len(manifest.Manifests))
		for i, m := range manifest.Manifests {
			imgs[i] = image.Image{Repository: req.From.Repository, Digest: string(m.Digest)}
		}
		err = c.platformCopier.CopyAll(req.To.Repository, imgs...)
	}
	if err != nil {
		return err
	}

	if len(manifest.Manifests) > 0 {
		if err := uploadManifest(req.To, manifestResponse.ContentType, manifestResponse.Body); err != nil {
			return err
		}
	}
	log.Printf("[image]\tfully copied %s to %s", req.From, req.To)
	return nil
}
