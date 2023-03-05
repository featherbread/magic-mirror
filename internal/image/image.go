package image

type Digest string

type Registry string

type Repository struct {
	Registry
	Path string
}

type Image struct {
	Repository
	Tag string
}
