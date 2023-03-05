package blobmirror

import (
	"fmt"
	"log"
	"time"

	"go.alexhamlin.co/magic-mirror/internal/image"
)

func transfer(dgst image.Digest, from []image.Repository, to image.Repository) error {
	if len(from) < 1 {
		return fmt.Errorf("no sources for blob %s", dgst)
	}

	log.Printf("[blobmirror] Transferring %s from %s to %s", dgst, from[0], to)
	time.Sleep(2 * time.Second)
	return nil
}
