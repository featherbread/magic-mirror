package main

import (
	"log"
	"sync"

	"go.alexhamlin.co/magic-mirror/internal/blob"
	"go.alexhamlin.co/magic-mirror/internal/image"
)

func main() {
	copier := blob.NewCopier(5)
	defer copier.Close()

	from := image.Repository{
		Registry:  image.Registry("ghcr.io"),
		Namespace: "ahamlinman/hypcast",
	}
	digests := []image.Digest{
		"sha256:1a8ac054707c16fa2a642d01ead1c0cd72bd806b3bf63b8a236a599a4f595687",
		"sha256:d54562aba9d793395314367d41b94a548620d5526ead5fce39a5ee7a06f1c5ab",
		"sha256:8b71c7bac08cea877b8f147b3f6e11cbf975bdd3aab03690dd3b1034a5e5d2e9",
		"sha256:9cc07399f45c0f61db2707029d9c6a1d3a3a3d0e5bb0be439b1944e4813e78b3",
		"sha256:daa2a7aaa8b3aceea6d0a40f07b3c74bcc31b5df580fd7a6171957e20a0d3ca3",
		"sha256:f2ea26ed4f148c5df0d9b17822d2128ae320df8ec9a3926b3743afd396b86fc6",
		"sha256:7efcac766705aee037feff011e549d6ec2898be89203cc47acb004674737741d",
		"sha256:ccef5726074c065acd84b669c0a98b6455b60f6b5c85cebbf849cf79b2cb38bb",
		"sha256:059c9532425d92a0bf88f1bd124e777817c5a05c9a5fe00603983d106818cbff",
		"sha256:daa2a7aaa8b3aceea6d0a40f07b3c74bcc31b5df580fd7a6171957e20a0d3ca3",
	}

	to1 := image.Repository{Registry: image.Registry("localhost:5000"), Namespace: "imported/hypcast"}
	to2 := image.Repository{Registry: image.Registry("localhost:5000"), Namespace: "alsoimported/hypcast"}

	blobTasks := make([]blob.CopyTask, 0, 2*len(digests))
	for _, dgst := range digests {
		blobTasks = append(blobTasks, copier.RequestCopy(dgst, from, to1))
		log.Printf("[main] requested to copy %s from %s to %s", dgst, from, to1)
	}
	for _, dgst := range digests {
		blobTasks = append(blobTasks, copier.RequestCopy(dgst, from, to2))
		log.Printf("[main] requested to copy %s from %s to %s", dgst, from, to2)
	}

	var wg sync.WaitGroup
	wg.Add(len(blobTasks))
	for _, task := range blobTasks {
		task := task
		go func() {
			defer wg.Done()
			err := task.Wait()
			log.Printf("[main] copy result: %v", err)
		}()
	}
	wg.Wait()
}
