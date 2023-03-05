package manifest

import (
	"encoding/json"

	"go.alexhamlin.co/magic-mirror/internal/blob"
	"go.alexhamlin.co/magic-mirror/internal/engine"
	"go.alexhamlin.co/magic-mirror/internal/image"
)

type PlatformCopier struct {
	engine             *engine.Engine[PlatformRequest, engine.NoValue]
	manifestDownloader *Downloader
	blobCopier         *blob.Copier
}

type PlatformRequest struct {
	Reference string
	From      image.Repository
	To        image.Repository
}

func NewPlatformCopier(workers int, manifestDownloader *Downloader, blobCopier *blob.Copier) *PlatformCopier {
	c := &PlatformCopier{
		manifestDownloader: manifestDownloader,
		blobCopier:         blobCopier,
	}
	c.engine = engine.NewEngine(workers, engine.NoValueFunc(c.handleRequest))
	return c
}

func (c *PlatformCopier) RequestCopy(reference string, from, to image.Repository) PlatformCopyTask {
	return PlatformCopyTask{c.engine.GetOrSubmit(PlatformRequest{
		Reference: reference,
		From:      from,
		To:        to,
	})}
}

type PlatformCopyTask struct {
	*engine.Task[engine.NoValue]
}

func (t PlatformCopyTask) Wait() error {
	_, err := t.Task.Wait()
	return err
}

func (c *PlatformCopier) Close() {
	c.engine.Close()
}

func (c *PlatformCopier) handleRequest(req PlatformRequest) error {
	manifestResponse, err := c.manifestDownloader.RequestDownload(req.From, req.Reference).Wait()
	if err != nil {
		return err
	}

	var manifest struct {
		Config struct {
			Digest image.Digest `json:"digest"`
		} `json:"config"`
		Layers []struct {
			Digest image.Digest `json:"digest"`
		} `json:"layers"`
	}
	if err := json.Unmarshal([]byte(manifestResponse.Body), &manifest); err != nil {
		return err
	}

	tasks := make([]blob.CopyTask, len(manifest.Layers)+1)
	for i, layer := range manifest.Layers {
		tasks[i] = c.blobCopier.RequestCopy(layer.Digest, req.From, req.To)
	}
	tasks[len(tasks)-1] = c.blobCopier.RequestCopy(manifest.Config.Digest, req.From, req.To)

	var taskErr error
	for _, task := range tasks {
		if err := task.Wait(); err != nil && taskErr == nil {
			taskErr = err
		}
	}
	if taskErr != nil {
		return taskErr
	}

	return uploadManifest(req.To, req.Reference, manifestResponse.ContentType, manifestResponse.Body)
}
