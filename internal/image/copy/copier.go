package copy

import (
	"context"
	"errors"
	"fmt"
	"time"

	mapset "github.com/deckarep/golang-set/v2"

	"go.alexhamlin.co/magic-mirror/internal/image"
	"go.alexhamlin.co/magic-mirror/internal/log"
	"go.alexhamlin.co/magic-mirror/internal/work"
)

type Request struct {
	Src image.Image
	Dst image.Image
}

func ValidateRequests(reqs ...Request) error {
	// TODO: Validate that destinations do not contain digests in annotation
	// comparison mode.

	// TODO: Validate that source and destination digests do not mismatch.

	_, err := coalesceRequests(reqs)
	return err
}

func coalesceRequests(reqs []Request) ([]Request, error) {
	var errs []error

	srcs := mapset.NewThreadUnsafeSet[image.Image]()
	for _, req := range reqs {
		srcs.Add(req.Src)
	}
	for _, req := range reqs {
		if srcs.Contains(req.Dst) {
			errs = append(errs, fmt.Errorf("%s is both a source and a destination", req.Dst))
		}
	}

	coalesced := make([]Request, 0, len(reqs))
	requestsByDst := make(map[image.Image]Request)
	for _, current := range reqs {
		previous, ok := requestsByDst[current.Dst]
		if !ok {
			coalesced = append(coalesced, current)
			requestsByDst[current.Dst] = current
			continue
		}
		if previous != current {
			errs = append(errs, fmt.Errorf("%s requests inconsistent copies from %s and %s", current.Dst, current.Src, previous.Src))
		}
	}

	return coalesced, errors.Join(errs...)
}

type Copier struct {
	*work.Queue[Request, work.NoValue]

	comparer     comparer
	blobs        *blobCopier
	srcManifests *manifestCache
	platforms    *platformCopier
	dstManifests *manifestCache
	dstIndexer   *blobIndexer

	statsTimer *time.Timer
}

func NewCopier(workers int, compareMode CompareMode) *Copier {
	comparer := comparers[compareMode]
	blobs := newBlobCopier(workers)
	srcManifests := newManifestCache(workers)
	platforms := newPlatformCopier(comparer, srcManifests, blobs)
	dstManifests := newManifestCache(workers)
	dstIndexer := newBlobIndexer(workers, blobs)

	c := &Copier{
		comparer:     comparer,
		blobs:        blobs,
		srcManifests: srcManifests,
		platforms:    platforms,
		dstManifests: dstManifests,
		dstIndexer:   dstIndexer,
	}
	c.Queue = work.NewQueue(0, work.NoValueHandler(c.handleRequest))
	c.statsTimer = time.AfterFunc(statsInterval, c.printStats)
	return c
}

func (c *Copier) Copy(src, dst image.Image) error {
	_, err := c.Queue.GetOrSubmit(Request{Src: src, Dst: dst}).Wait()
	c.printStats()
	return err
}

func (c *Copier) CopyAll(reqs ...Request) error {
	reqs, err := coalesceRequests(reqs)
	if err != nil {
		return err
	}
	_, err = c.Queue.GetOrSubmitAll(reqs...).Wait()
	c.printStats()
	return err
}

func (c *Copier) CloseSubmit() {
	// TODO: This is only safe after all Copier tasks are finished.
	// TODO: There is no way to cleanly stop destination blob indexing.
	// TODO: Should really stop the stats timer too.
	c.Queue.CloseSubmit()
	c.platforms.CloseSubmit()
	c.srcManifests.CloseSubmit()
	c.dstManifests.CloseSubmit()
	c.blobs.CloseSubmit()
}

const statsInterval = 5 * time.Second

func (c *Copier) printStats() {
	var (
		blobsDone, blobsTotal         = c.blobs.Stats()
		platformsDone, platformsTotal = c.platforms.Stats()
		imagesDone, imagesTotal       = c.Queue.Stats()
	)
	log.Printf(
		"[stats]\tblobs: %d of %d copied; platforms: %d of %d mirrored; images: %d of %d in sync",
		blobsDone, blobsTotal,
		platformsDone, platformsTotal,
		imagesDone, imagesTotal,
	)
	c.statsTimer.Reset(statsInterval)
}

func (c *Copier) handleRequest(_ context.Context, req Request) error {
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

func (c *Copier) copyIndex(srcIndex image.Index, src, dst image.Image) error {
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
