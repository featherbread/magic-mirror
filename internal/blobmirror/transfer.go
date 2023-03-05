package blobmirror

import (
	"fmt"
	"log"
	"math/rand"
	"time"

	"go.alexhamlin.co/magic-mirror/internal/image"
)

func transfer(dgst image.Digest, from []image.Repository, to image.Repository) error {
	if len(from) < 1 {
		return fmt.Errorf("no sources for blob %s", dgst)
	}

	log.Printf("[blobmirror] Transferring %s from %s to %s", dgst, from[0], to)

	duration := time.Duration(rand.Uint64() % uint64(5*time.Second))
	time.Sleep(duration)
	return nil
}
