package manifest

import (
	"encoding/json"
	"log"

	"go.alexhamlin.co/magic-mirror/internal/engine"
	"go.alexhamlin.co/magic-mirror/internal/image"
)

type ImageCopier struct {
	engine             *engine.Engine[ImageRequest, engine.NoValue]
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
	c.engine = engine.NewEngine(workers, engine.NoValueHandler(c.handleRequest))
	return c
}

func (c *ImageCopier) RequestCopy(from, to image.Image) ImageCopyTask {
	return ImageCopyTask{c.engine.GetOrSubmit(ImageRequest{From: from, To: to})}
}

func (c *ImageCopier) RequestCopyAll(reqs ...ImageRequest) []ImageCopyTask {
	tasks := make([]ImageCopyTask, len(reqs))
	for i, task := range c.engine.GetOrSubmitAll(reqs...) {
		tasks[i] = ImageCopyTask{task}
	}
	return tasks
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
	log.Printf("[image]\tstarting copy from %s to %s", req.From, req.To)

	manifestResponse, err := c.manifestDownloader.RequestDownload(req.From).Wait()
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

	var tasks []PlatformCopyTask
	if len(manifest.Manifests) > 0 {
		for _, m := range manifest.Manifests {
			img := image.Image{Repository: req.From.Repository, Digest: string(m.Digest)}
			tasks = append(tasks, c.platformCopier.RequestCopy(img, req.To.Repository))
		}
	} else {
		tasks = []PlatformCopyTask{c.platformCopier.RequestCopy(req.From, req.To.Repository)}
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
		if err := uploadManifest(req.To, manifestResponse.ContentType, manifestResponse.Body); err != nil {
			return err
		}
	}
	log.Printf("[image]\tfully copied %s to %s", req.From, req.To)
	return nil
}
