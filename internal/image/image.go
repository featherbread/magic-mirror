package image

import "fmt"

type Digest string

type Registry string

type Repository struct {
	Registry
	Namespace string
}

func (r Repository) String() string {
	return fmt.Sprintf("%s/%s", r.Registry, r.Namespace)
}

type Image struct {
	Repository
	Tag string
}

func (i Image) String() string {
	return fmt.Sprintf("%s:%s", i.Repository, i.Tag)
}
