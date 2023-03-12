package copy

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"go.alexhamlin.co/magic-mirror/internal/image"
	"go.alexhamlin.co/magic-mirror/internal/log"
	"go.alexhamlin.co/magic-mirror/internal/work"
)

// CopyAll performs a bulk copy between OCI image registries based on the
// provided copy specs, using the provided concurrency for each component of the
// overall operation.
func CopyAll(concurrency int, specs ...Spec) error {
	keys, err := coalesceRequests(specs)
	if err != nil {
		return err
	}
	copier := newCopier(concurrency)
	defer copier.CloseSubmit()
	return copier.CopyAll(keys...)
}

type copier struct {
	queue *work.Queue[Spec, work.NoValue]

	blobs        *blobCopier
	srcManifests *manifestCache
	platforms    *platformCopier
	dstManifests *manifestCache
	dstIndexer   *blobIndexer

	statsTimer *time.Timer
}

func newCopier(workers int) *copier {
	blobs := newBlobCopier(workers)
	srcManifests := newManifestCache(workers)
	platforms := newPlatformCopier(srcManifests, blobs)
	dstManifests := newManifestCache(workers)
	dstIndexer := newBlobIndexer(workers, blobs)

	c := &copier{
		blobs:        blobs,
		srcManifests: srcManifests,
		platforms:    platforms,
		dstManifests: dstManifests,
		dstIndexer:   dstIndexer,
	}
	c.queue = work.NewQueue(0, work.NoValueHandler(c.handleRequest))
	c.statsTimer = time.AfterFunc(statsInterval, c.printStats)
	return c
}

func (c *copier) CopyAll(specs ...Spec) error {
	_, err := c.queue.GetOrSubmitAll(specs...).Wait()
	c.printStats()
	return err
}

func (c *copier) CloseSubmit() {
	// TODO: This is only safe after all Copier tasks are finished.
	// TODO: There is no way to cleanly stop destination blob indexing.
	// TODO: Should really stop the stats timer too.
	c.queue.CloseSubmit()
	c.platforms.CloseSubmit()
	c.srcManifests.CloseSubmit()
	c.dstManifests.CloseSubmit()
	c.blobs.CloseSubmit()
}

const statsInterval = 5 * time.Second

func (c *copier) printStats() {
	var (
		blobsDone, blobsTotal         = c.blobs.Stats()
		platformsDone, platformsTotal = c.platforms.Stats()
		imagesDone, imagesTotal       = c.queue.Stats()
	)
	log.Printf(
		"[stats] blobs: %d of %d copied; platforms: %d of %d copied; images: %d of %d in sync",
		blobsDone, blobsTotal,
		platformsDone, platformsTotal,
		imagesDone, imagesTotal,
	)
	c.statsTimer.Reset(statsInterval)
}

func (c *copier) handleRequest(_ context.Context, spec Spec) error {
	log.Verbosef("[image]\tstarting copy from %s to %s", spec.Src, spec.Dst)

	srcTask := c.srcManifests.GetOrSubmit(spec.Src)
	dstTask := c.dstManifests.GetOrSubmit(spec.Dst)

	srcManifest, err := srcTask.Wait()
	if err != nil {
		return err
	}

	dstManifest, err := dstTask.Wait()
	if err == nil {
		c.dstIndexer.Submit(spec.Dst.Repository, dstManifest)
		if bytes.Equal(srcManifest.Encoded(), dstManifest.Encoded()) {
			log.Verbosef("[image]\tno change from %s to %s", spec.Src, spec.Dst)
			return nil
		}
	}

	srcMediaType := srcManifest.GetMediaType()
	switch {
	case srcMediaType.IsIndex():
		err = c.copyIndex(spec, srcManifest.(image.Index))
	case srcMediaType.IsManifest():
		_, err = c.platforms.Copy(spec.Src, spec.Dst)
	default:
		err = fmt.Errorf("unknown manifest type for %s: %s", spec.Src, srcMediaType)
	}
	if err != nil {
		return err
	}

	log.Verbosef("[image]\tfully mirrored %s to %s", spec.Src, spec.Dst)
	return nil
}

func (c *copier) copyIndex(spec Spec, srcIndex image.Index) error {
	src := spec.Src
	dst := spec.Dst

	if err := srcIndex.Validate(); err != nil {
		return err
	}

	srcDescriptors := srcIndex.Parsed().Manifests
	imgs := make([]image.Image, len(srcDescriptors))
	for i, m := range srcDescriptors {
		imgs[i] = image.Image{Repository: src.Repository, Digest: m.Digest}
	}
	dstManifests, err := c.platforms.CopyAll(dst.Repository, imgs...)
	if err != nil {
		return err
	}

	var (
		uploadIndex    = srcIndex
		dstIndex       image.ParsedIndex
		dstIndexCopied bool
	)
	ensureNewDstIndex := func() {
		if !dstIndexCopied {
			dstIndex = image.DeepCopy(srcIndex).(image.Index).Parsed()
			dstIndexCopied = true
		}
	}
	for i, dstManifest := range dstManifests {
		desc := dstManifest.Descriptor()
		if desc.Digest != srcDescriptors[i].Digest {
			ensureNewDstIndex()
			dstIndex.Manifests[i] = desc
			dstIndex.Manifests[i].Annotations = srcDescriptors[i].Annotations
			dstIndex.Manifests[i].Platform = srcDescriptors[i].Platform
		}
	}
	if dstIndexCopied {
		uploadIndex = dstIndex
	}
	return uploadManifest(dst, uploadIndex)
}
