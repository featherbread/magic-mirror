package manifest

import (
	"encoding/json"
	"log"

	"go.alexhamlin.co/magic-mirror/internal/image"
	"go.alexhamlin.co/magic-mirror/internal/work"
)

type ImageCopier struct {
	engine             *work.Queue[ImageRequest, work.NoValue]
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
	c.engine = work.NewQueue(workers, work.NoValueHandler(c.handleRequest))
	return c
}

func (c *ImageCopier) SubmitAll(reqs ...ImageRequest) []ImageCopyTask {
	tasks := make([]ImageCopyTask, len(reqs))
	for i, task := range c.engine.GetOrSubmitAll(reqs...) {
		tasks[i] = ImageCopyTask{task}
	}
	return tasks
}

type ImageCopyTask struct {
	*work.Task[work.NoValue]
}

func (t ImageCopyTask) Wait() error {
	_, err := t.Task.Wait()
	return err
}

func (c *ImageCopier) Close() {
	c.engine.CloseSubmit()
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
