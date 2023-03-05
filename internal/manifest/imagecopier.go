package manifest

import (
	"encoding/json"
	"log"

	"go.alexhamlin.co/magic-mirror/internal/engine"
	"go.alexhamlin.co/magic-mirror/internal/image"
)

type ImageCopier struct {
	engine         *engine.Engine[ImageRequest, engine.NoValue]
	platformCopier *PlatformCopier
}

type ImageRequest struct {
	From image.Image
	To   image.Image
}

func NewImageCopier(workers int, platformCopier *PlatformCopier) *ImageCopier {
	c := &ImageCopier{platformCopier: platformCopier}
	c.engine = engine.NewEngine(workers, engine.NoValueFunc(c.handleRequest))
	return c
}

func (c *ImageCopier) RequestCopy(from, to image.Image) ImageCopyTask {
	return ImageCopyTask{c.engine.GetOrSubmit(ImageRequest{From: from, To: to})}
}

type ImageCopyTask struct {
	*engine.Task[engine.NoValue]
}

func (t ImageCopyTask) Wait() error {
	_, err := t.Task.Wait()
	return err
}

func (c *ImageCopier) Close() {
	c.engine.Close()
}

func (c *ImageCopier) handleRequest(req ImageRequest) error {
	log.Printf("[image] downloading root manifest for %s", req.From)
	contentType, body, err := downloadManifest(req.From.Repository, req.From.Tag)
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
	if err := json.Unmarshal([]byte(body), &manifest); err != nil {
		return err
	}

	var tasks []PlatformCopyTask
	if len(manifest.Manifests) > 0 {
		for _, m := range manifest.Manifests {
			tasks = append(tasks, c.platformCopier.RequestCopy(string(m.Digest), req.From.Repository, req.To.Repository))
		}
	} else {
		tasks = []PlatformCopyTask{c.platformCopier.RequestCopy(req.From.Tag, req.From.Repository, req.To.Repository)}
	}
	var taskErr error
	for _, task := range tasks {
		if err := task.Wait(); err != nil && taskErr == nil {
			taskErr = err
		}
	}
	if taskErr != nil {
		return taskErr
	}

	if len(manifest.Manifests) > 0 {
		if err := uploadManifest(req.To.Repository, req.From.Tag, contentType, body); err != nil {
			return err
		}
	}
	log.Printf("[image] fully copied %s to %s", req.From, req.To)
	return nil
}
