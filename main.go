package main

func main() {}

type MirrorSpec struct {
	FromReference string
	ToReference   string
	Platforms     []string
}

type BlobCopyKey struct {
	FromRepository string
	ToRepository   string
}

type BlobCopyStatus struct {
	BlobCopyKey
	Done chan struct{}
}
