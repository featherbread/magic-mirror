package copy

import (
	"errors"
	"fmt"
	"log"

	mapset "github.com/deckarep/golang-set/v2"

	"go.alexhamlin.co/magic-mirror/internal/image"
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

	compareMode  CompareMode
	blobs        *blobCopier
	srcManifests *manifestCache
	platforms    *platformCopier
	dstManifests *manifestCache
	dstTracer    *blobTracer
}

func NewCopier(workers int, compareMode CompareMode) *Copier {
	blobs := newBlobCopier(workers)
	srcManifests := newManifestCache(workers)
	platforms := newPlatformCopier(compareMode, srcManifests, blobs)
	dstManifests := newManifestCache(workers)
	dstTracer := newBlobTracer(dstManifests, blobs)

	c := &Copier{
		compareMode:  compareMode,
		blobs:        blobs,
		srcManifests: srcManifests,
		platforms:    platforms,
		dstManifests: dstManifests,
		dstTracer:    dstTracer,
	}
	c.Queue = work.NewQueue(0, work.NoValueHandler(c.handleRequest))
	return c
}

func (c *Copier) Copy(src, dst image.Image) error {
	_, err := c.Queue.GetOrSubmit(Request{Src: src, Dst: dst}).Wait()
	return err
}

func (c *Copier) CopyAll(reqs ...Request) error {
	reqs, err := coalesceRequests(reqs)
	if err != nil {
		return err
	}
	_, err = c.Queue.GetOrSubmitAll(reqs...).Wait()
	return err
}

func (c *Copier) CloseSubmit() {
	// TODO: This is only safe after all Copier tasks are finished.
	c.Queue.CloseSubmit()
	c.platforms.CloseSubmit()
	c.srcManifests.CloseSubmit()
	// TODO: Since we don't block on destination tracing, these may not be safe to
	// clean up. Need to figure out a cancellation strategy.
	// c.destTracer.CloseSubmit()
	// c.destManifests.CloseSubmit()
	c.blobs.CloseSubmit()
}

func (c *Copier) handleRequest(req Request) error {
	log.Printf("[image]\tstarting copy from %s to %s", req.Src, req.Dst)

	srcTask := c.srcManifests.GetOrSubmit(req.Src)
	dstTask := c.dstManifests.GetOrSubmit(req.Dst)

	srcManifest, err := srcTask.Wait()
	if err != nil {
		return err
	}

	dstManifest, err := dstTask.Wait()
	if err == nil {
		c.dstTracer.QueueForTracing(req.Dst)
		if comparisons[c.compareMode](srcManifest, dstManifest) {
			log.Printf("[image]\tno change from %s to %s", req.Src, req.Dst)
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

	log.Printf("[image]\tfully copied %s to %s", req.Src, req.Dst)
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

	if c.compareMode == CompareModeEqual {
		return uploadManifest(dst, srcIndex)
	}

	newIndex := srcIndex.Parsed()
	newIndex.MediaType = string(image.OCIIndexMediaType)
	if newIndex.Annotations == nil {
		newIndex.Annotations = make(map[string]string)
	}
	newIndex.Annotations[annotationSourceDigest] = srcIndex.Descriptor().Digest.String()
	for i, dstManifest := range dstManifests {
		old := newIndex.Manifests[i]
		new := dstManifest.Descriptor()
		new.Annotations = old.Annotations
		new.Platform = old.Platform
		newIndex.Manifests[i] = new
	}
	return uploadManifest(dst, newIndex)
}
