package copy

import (
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
func CopyAll(concurrency int, compareMode CompareMode, specs ...Spec) error {
	keys, err := keyifyRequests(specs)
	if err != nil {
		return err
	}
	copier := newCopier(concurrency, compareMode)
	defer copier.CloseSubmit()
	return copier.CopyAll(keys...)
}

type copier struct {
	queue *work.Queue[specKey, work.NoValue]

	comparer     comparer
	blobs        *blobCopier
	srcManifests *manifestCache
	platforms    *platformCopier
	dstManifests *manifestCache
	dstIndexer   *blobIndexer

	statsTimer *time.Timer
}

func newCopier(workers int, compareMode CompareMode) *copier {
	comparer := comparers[compareMode]
	blobs := newBlobCopier(workers)
	srcManifests := newManifestCache(workers)
	platforms := newPlatformCopier(comparer, srcManifests, blobs)
	dstManifests := newManifestCache(workers)
	dstIndexer := newBlobIndexer(workers, blobs)

	c := &copier{
		comparer:     comparer,
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

func (c *copier) CopyAll(keys ...specKey) error {
	_, err := c.queue.GetOrSubmitAll(keys...).Wait()
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
		"[stats] blobs: %d of %d copied; platforms: %d of %d mirrored; images: %d of %d in sync",
		blobsDone, blobsTotal,
		platformsDone, platformsTotal,
		imagesDone, imagesTotal,
	)
	c.statsTimer.Reset(statsInterval)
}

func (c *copier) handleRequest(_ context.Context, req specKey) error {
	log.Verbosef("[image]\tstarting copy from %s to %s", req.Src, req.Dst)

	srcTask := c.srcManifests.GetOrSubmit(req.Src)
	dstTask := c.dstManifests.GetOrSubmit(req.Dst)

	srcManifest, err := srcTask.Wait()
	if err != nil {
		return err
	}

	dstManifest, err := dstTask.Wait()
	if err == nil {
		c.dstIndexer.Submit(req.Dst.Repository, dstManifest)
		if c.comparer.IsMirrored(srcManifest, dstManifest) {
			log.Verbosef("[image]\tno change from %s to %s", req.Src, req.Dst)
			return nil
		}
	}

	srcMediaType := srcManifest.GetMediaType()
	switch {
	case srcMediaType.IsIndex():
		err = c.copyIndex(srcManifest.(image.Index), req.Src, req.Dst)
	case srcMediaType.IsManifest():
		_, err = c.platforms.Copy(req.Src, req.Dst)
	default:
		err = fmt.Errorf("unknown manifest type for %s: %s", req.Src, srcMediaType)
	}
	if err != nil {
		return err
	}

	log.Verbosef("[image]\tfully mirrored %s to %s", req.Src, req.Dst)
	return nil
}

func (c *copier) copyIndex(srcIndex image.Index, src, dst image.Image) error {
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

	markedIndex := c.comparer.MarkSource(uploadIndex, srcIndex.Descriptor().Digest)
	return uploadManifest(dst, markedIndex)
}
