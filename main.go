package main

import (
	"log"
	"os"
	"sync"
	"sync/atomic"

	"go.alexhamlin.co/magic-mirror/internal/blob"
	"go.alexhamlin.co/magic-mirror/internal/image"
	"go.alexhamlin.co/magic-mirror/internal/manifest"
)

func must[T any](x T, err error) T {
	if err != nil {
		panic(err)
	}
	return x
}

func main() {
	blobCopier := blob.NewCopier(10)
	defer blobCopier.CloseSubmit()

	manifestDownloader := manifest.NewDownloader(10)
	defer manifestDownloader.CloseSubmit()

	platformCopier := manifest.NewPlatformCopier(0, manifestDownloader, blobCopier)
	defer platformCopier.Close()

	imageCopier := manifest.NewImageCopier(0, manifestDownloader, platformCopier)
	defer imageCopier.Close()

	tasks := imageCopier.SubmitAll(
		manifest.ImageRequest{
			From: must(image.Parse("ghcr.io/ahamlinman/hypcast:latest")),
			To:   must(image.Parse("localhost:5000/imported/hypcast:latest")),
		},
		manifest.ImageRequest{
			From: must(image.Parse("ghcr.io/ahamlinman/hypcast:latest")),
			To:   must(image.Parse("localhost:5000/alsoimported/hypcast:latest")),
		},
		manifest.ImageRequest{
			From: must(image.Parse("ghcr.io/dexidp/dex:v2.35.3")),
			To:   must(image.Parse("localhost:5000/imported/dex:v2.35.3")),
		},
		manifest.ImageRequest{
			From: must(image.Parse("quay.io/minio/minio:RELEASE.2023-02-22T18-23-45Z")),
			To:   must(image.Parse("localhost:5000/imported/minio:RELEASE.2023-02-22T18-23-45Z")),
		},
		manifest.ImageRequest{
			From: must(image.Parse("quay.io/minio/minio:RELEASE.2023-02-27T18-10-45Z")),
			To:   must(image.Parse("localhost:5000/imported/minio:RELEASE.2023-02-27T18-10-45Z")),
		},
		manifest.ImageRequest{
			From: must(image.Parse("quay.io/minio/minio:release.2023-02-27t18-10-45z.fips")),
			To:   must(image.Parse("localhost:5000/imported/minio:RELEASE.2023-02-27T18-10-45Z.fips")),
		},
	)

	var hadError atomic.Bool
	var wg sync.WaitGroup
	wg.Add(len(tasks))
	for _, task := range tasks {
		task := task
		go func() {
			defer wg.Done()
			if err := task.Wait(); err != nil {
				hadError.Store(true)
				log.Printf("[main] copy error: %v", err)
			}
		}()
	}
	wg.Wait()

	if hadError.Load() {
		os.Exit(1)
	}
}
