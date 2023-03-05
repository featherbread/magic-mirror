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
	blobCopier := blob.NewCopier(10)
	defer blobCopier.Close()

	manifestDownloader := manifest.NewDownloader(10)
	defer manifestDownloader.Close()

	platformCopier := manifest.NewPlatformCopier(0, manifestDownloader, blobCopier)
	defer platformCopier.Close()

	imageCopier := manifest.NewImageCopier(0, manifestDownloader, platformCopier)
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

	// tasks = append(tasks, imageCopier.RequestCopy(
	// 	image.Image{
	// 		Repository: image.Repository{
	// 			Registry:  "index.docker.io",
	// 			Namespace: "minio/minio",
	// 		},
	// 		Tag: "RELEASE.2023-02-22T18-23-45Z",
	// 	},
	// 	image.Image{
	// 		Repository: image.Repository{
	// 			Registry:  "localhost:5000",
	// 			Namespace: "imported/minio",
	// 		},
	// 		Tag: "RELEASE.2023-02-22T18-23-45Z",
	// 	},
	// ))

	// tasks = append(tasks, imageCopier.RequestCopy(
	// 	image.Image{
	// 		Repository: image.Repository{
	// 			Registry:  "index.docker.io",
	// 			Namespace: "minio/minio",
	// 		},
	// 		Tag: "RELEASE.2023-02-27T18-10-45Z",
	// 	},
	// 	image.Image{
	// 		Repository: image.Repository{
	// 			Registry:  "localhost:5000",
	// 			Namespace: "imported/minio",
	// 		},
	// 		Tag: "RELEASE.2023-02-27T18-10-45Z",
	// 	},
	// ))

	// tasks = append(tasks, imageCopier.RequestCopy(
	// 	image.Image{
	// 		Repository: image.Repository{
	// 			Registry:  "index.docker.io",
	// 			Namespace: "minio/minio",
	// 		},
	// 		Tag: "RELEASE.2023-02-27T18-10-45Z.fips",
	// 	},
	// 	image.Image{
	// 		Repository: image.Repository{
	// 			Registry:  "localhost:5000",
	// 			Namespace: "imported/minio",
	// 		},
	// 		Tag: "RELEASE.2023-02-27T18-10-45Z.fips",
	// 	},
	// ))

	// tasks = append(tasks, imageCopier.RequestCopy(
	// 	image.Image{
	// 		Repository: image.Repository{
	// 			Registry:  "index.docker.io",
	// 			Namespace: "library/alpine",
	// 		},
	// 		Tag: "3.17",
	// 	},
	// 	image.Image{
	// 		Repository: image.Repository{
	// 			Registry:  "localhost:5000",
	// 			Namespace: "imported/alpine",
	// 		},
	// 		Tag: "3.17",
	// 	},
	// ))

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
