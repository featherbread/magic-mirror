package blobmirror

import (
	"sync"

	"go.alexhamlin.co/magic-mirror/internal/image"
)

const Workers = 10

type Engine struct {
	sources   map[image.Digest]*SourceList
	sourcesMu sync.Mutex

	statuses   map[Request]*Status
	statusesMu sync.Mutex

	pending chan Request
}

type Request struct {
	Digest image.Digest
	To     image.Repository
}

type Status struct {
	Done chan struct{}
	Err  error
}

func NewEngine() *Engine {
	e := &Engine{
		sources:  make(map[image.Digest]*SourceList),
		statuses: make(map[Request]*Status),
		pending:  make(chan Request),
	}
	for i := 0; i < Workers; i++ {
		go e.work()
	}
	return e
}

func (e *Engine) Register(dgst image.Digest, from, to image.Repository) *Status {
	e.registerSource(dgst, from)

	req := Request{Digest: dgst, To: to}

	e.statusesMu.Lock()
	defer e.statusesMu.Unlock()

	status, ok := e.statuses[req]
	if ok {
		return status
	}

	status = &Status{Done: make(chan struct{}), Err: nil}
	e.statuses[req] = status
	go func() { e.pending <- req }()
	return status
}

func (e *Engine) Close() {
	close(e.pending)
}

func (e *Engine) work() {
	for req := range e.pending {
		e.statusesMu.Lock()
		status := e.statuses[req]
		e.statusesMu.Unlock()
		status.Err = transfer(req.Digest, e.getSourceList(req.Digest).Copy(), req.To)
		close(status.Done)
	}
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
