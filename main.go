package main

import (
	"log"

	"go.alexhamlin.co/magic-mirror/internal/blobmirror"
	"go.alexhamlin.co/magic-mirror/internal/image"
)

func main() {
	engine := blobmirror.NewEngine()
	defer engine.Close()

	from := image.Repository{
		Registry: image.Registry("docker.io"),
		Path:     "library/alpine",
	}
	to := image.Repository{
		Registry: image.Registry("localhost:5000"),
		Path:     "imported/alpine",
	}

	digests := []image.Digest{
		"sha256:af6eaf76a39c2d3e7e0b8a0420486e3df33c4027d696c076a99a3d0ac09026af",
		"sha256:d74e625d91152966d38fe8a62c60daadb96d4b94c1a366de01fab5f334806239",
	}
	statuses := make([]*blobmirror.Status, len(digests))
	for i, dgst := range digests {
		statuses[i] = engine.Register(dgst, from, to)
	}

	for _, status := range statuses {
		<-status.Done
		log.Printf("[main]: Transfer done: %v", status.Err)
	}
}
