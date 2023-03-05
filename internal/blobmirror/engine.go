package blobmirror

import (
	"sync"

	"go.alexhamlin.co/magic-mirror/internal/engine"
	"go.alexhamlin.co/magic-mirror/internal/image"
)

const Workers = 10

type Engine struct {
	engine *engine.Engine[Request, struct{}]

	sources   map[image.Digest]*SourceList
	sourcesMu sync.Mutex
}

type Request struct {
	Digest image.Digest
	To     image.Repository
}

func NewEngine() *Engine {
	e := &Engine{sources: make(map[image.Digest]*SourceList)}
	e.engine = engine.NewEngine(Workers, e.do)
	return e
}

func (e *Engine) do(req Request) (struct{}, error) {
	err := transfer(req.Digest, e.getSourceList(req.Digest).Copy(), req.To)
	if err != nil {
		e.registerSource(req.Digest, req.To)
	}
	return struct{}{}, err
}

func (e *Engine) Register(dgst image.Digest, from, to image.Repository) *engine.Task[struct{}] {
	e.registerSource(dgst, from)
	return e.engine.GetOrSubmit(Request{Digest: dgst, To: to})
}

func (e *Engine) Close() {
	e.engine.Close()
}

func (e *Engine) registerSource(dgst image.Digest, repo image.Repository) {
	e.getSourceList(dgst).Register(repo)
}

func (e *Engine) getSourceList(dgst image.Digest) *SourceList {
	e.sourcesMu.Lock()
	defer e.sourcesMu.Unlock()

	sourceList, ok := e.sources[dgst]
	if !ok {
		sourceList = new(SourceList)
		e.sources[dgst] = sourceList
	}
	return sourceList
}

type SourceList struct {
	Repositories []image.Repository
	mu           sync.Mutex
}

func (sl *SourceList) Copy() []image.Repository {
	sl.mu.Lock()
	defer sl.mu.Unlock()

	repos := make([]image.Repository, len(sl.Repositories))
	copy(repos, sl.Repositories)
	return repos
}

func (sl *SourceList) Register(repo image.Repository) {
	sl.mu.Lock()
	defer sl.mu.Unlock()

	if !sl.includesLocked(repo) {
		sl.Repositories = append(sl.Repositories, repo)
	}
}

func (sl *SourceList) Includes(repo image.Repository) bool {
	sl.mu.Lock()
	defer sl.mu.Unlock()

	return sl.includesLocked(repo)
}

func (sl *SourceList) includesLocked(repo image.Repository) bool {
	for _, have := range sl.Repositories {
		if have == repo {
			return true
		}
	}
	return false
}
