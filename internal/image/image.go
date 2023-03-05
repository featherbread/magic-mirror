package image

type Digest string

type Registry string

type Repository struct {
	Registry
	Namespace string
}

type Image struct {
	Repository
	Tag string
}
