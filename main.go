package main

import (
	"log"
	"os"

	"go.alexhamlin.co/magic-mirror/internal/image"
	"go.alexhamlin.co/magic-mirror/internal/image/copy"
)

func must[T any](x T, err error) T {
	if err != nil {
		panic(err)
	}
	return x
}

func main() {
	copier := copy.NewCopier(10, copy.CompareModeAnnotation)
	defer copier.CloseSubmit()

	err := copier.CopyAll(
		copy.Request{
			Src: must(image.Parse("ghcr.io/ahamlinman/hypcast:latest")),
			Dst: must(image.Parse("localhost:5000/imported/hypcast:latest")),
		},
		copy.Request{
			Src: must(image.Parse("ghcr.io/dexidp/dex:v2.35.3")),
			Dst: must(image.Parse("localhost:5000/imported/dex:v2.35.3")),
		},
		copy.Request{
			Src: must(image.Parse("ghcr.io/dexidp/dex:v2.35.3")),
			Dst: must(image.Parse("localhost:5000/imported/dex:v2.35.3")),
		},
		copy.Request{
			Src: must(image.Parse("ghcr.io/ahamlinman/hypcast:latest")),
			Dst: must(image.Parse("localhost:5000/alsoimported/hypcast:latest")),
		},
		copy.Request{
			Src: must(image.Parse("quay.io/minio/minio:RELEASE.2023-02-22T18-23-45Z")),
			Dst: must(image.Parse("localhost:5000/imported/minio:RELEASE.2023-02-22T18-23-45Z")),
		},
		copy.Request{
			Src: must(image.Parse("quay.io/minio/minio:RELEASE.2023-02-27T18-10-45Z")),
			Dst: must(image.Parse("localhost:5000/imported/minio:RELEASE.2023-02-27T18-10-45Z")),
		},
		copy.Request{
			Src: must(image.Parse("quay.io/minio/minio:RELEASE.2023-02-27T18-10-45Z.fips")),
			Dst: must(image.Parse("localhost:5000/imported/minio:fips")),
		},
		copy.Request{
			Src: must(image.Parse("quay.io/minio/minio:RELEASE.2023-02-27T18-10-45Z.fips")),
			Dst: must(image.Parse("localhost:5000/imported/minio:alsofips")),
		},
	)
	if err != nil {
		log.Printf("[main] some copies failed:\n%v", err)
		os.Exit(1)
	}
}
