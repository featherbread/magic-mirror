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

func main() {
	blobCopier := blob.NewCopier(5)
	defer blobCopier.Close()

	platformCopier := manifest.NewPlatformCopier(5, blobCopier)
	defer platformCopier.Close()

	imageCopier := manifest.NewImageCopier(5, platformCopier)
	defer imageCopier.Close()

	var tasks []manifest.ImageCopyTask

	hypcastImage := image.Image{
		Repository: image.Repository{
			Registry:  "ghcr.io",
			Namespace: "ahamlinman/hypcast",
		},
		Tag: "latest",
	}

	tasks = append(tasks, imageCopier.RequestCopy(
		hypcastImage,
		image.Image{
			Repository: image.Repository{
				Registry:  "localhost:5000",
				Namespace: "imported/hypcast",
			},
			Tag: "latest",
		},
	))

	tasks = append(tasks, imageCopier.RequestCopy(
		hypcastImage,
		image.Image{
			Repository: image.Repository{
				Registry:  "localhost:5000",
				Namespace: "alsoimported/hypcast",
			},
			Tag: "latest",
		},
	))

	tasks = append(tasks, imageCopier.RequestCopy(
		image.Image{
			Repository: image.Repository{
				Registry:  "index.docker.io",
				Namespace: "library/alpine",
			},
			Tag: "3.17",
		},
		image.Image{
			Repository: image.Repository{
				Registry:  "localhost:5000",
				Namespace: "imported/alpine",
			},
			Tag: "3.17",
		},
	))

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
