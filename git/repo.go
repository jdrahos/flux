package git

import (
	"errors"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"

	"context"
	"time"

	"github.com/weaveworks/flux"
)

const (
	interval = 5 * time.Minute

	DefaultCloneTimeout = 2 * time.Minute
	CheckPushTag        = "flux-write-check"
)

var (
	ErrNoChanges = errors.New("no changes made in repo")
	ErrNotReady  = errors.New("git repo not ready")
	ErrNoConfig  = errors.New("git repo has not valid config")
)

// Remote points at a git repo somewhere.
type Remote struct {
	URL string // clone from here
}

// Config holds some values we use when working in the local copy of
// the repo.
type Config struct {
	Branch    string // branch we're syncing to
	Path      string // path within the repo containing files we care about
	SyncTag   string
	NotesRef  string
	UserName  string
	UserEmail string
	SetAuthor bool
}

type Repo struct {
	// Supplied to constructor
	origin Remote
	config Config

	// Bookkeeping
	status  flux.GitRepoStatus
	err     error
	notify  chan struct{}
	request chan request
	dir     string
	mu      sync.RWMutex
}

type request struct {
	reply chan error
	ctx   context.Context
}

func NewRepo(origin Remote, config Config) *Repo {
	r := &Repo{
		origin:  origin,
		config:  config,
		status:  flux.RepoNew,
		err:     nil,
		notify:  make(chan struct{}, 1), // `1` so that Notify doesn't block
		request: make(chan request, 0),  // `0` so that Sync blocks
	}
	return r
}

// Origin returns the Remote with which the Repo was constructed.
func (r *Repo) Origin() Remote {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.origin
}

func (r *Repo) Config() Config {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.config
}

// Dir returns the local directory into which the repo has been
// cloned, if it has been cloned.
func (r *Repo) Dir() string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.dir
}

func (r *Repo) setStatus(s flux.GitRepoStatus, err error) {
	r.mu.Lock()
	r.status = s
	r.err = err
	r.mu.Unlock()
}

// Status reports that readiness status of this Git repo: whether it
// has been cloned, whether it is writable, and if not, the error
// stopping it getting to the next state.
func (r *Repo) Status() (flux.GitRepoStatus, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.status, r.err
}

// Notify tells the repo that it should fetch from the origin as soon
// as possible. It does not block.
func (r *Repo) Notify() {
	select {
	case r.notify <- struct{}{}:
		// duly notified
	default:
		// notification already pending
	}
}

// Sync asks for the repo to be synced, and blocks until it has
// succeeded or failed.
func (r *Repo) Sync(ctx context.Context) error {
	r.mu.RLock()
	s := r.status
	r.mu.RUnlock()
	// FIXME(maybe): race here, the repo may revert to a non-ready
	// state, here, and never be restored to a ready state.
	switch s {
	case flux.RepoReady:
		req := request{reply: make(chan error), ctx: ctx}
		select {
		case r.request <- req:
		case <-ctx.Done():
			return ctx.Err()
		}
		select {
		case err := <-req.reply:
			return err
		case <-ctx.Done():
			return ctx.Err()
		}
	default:
		return ErrNotReady
	}
}

// Read lets the caller examine a read-only clone of the repo.
func (r *Repo) Read(ctx context.Context, readf func(dir string) error) error {
	r.mu.RLock()
	s := r.status
	r.mu.RUnlock()
	if s != flux.RepoReady {
		return ErrNotReady
	}

	dir, err := ioutil.TempDir(os.TempDir(), "flux-readclone")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)

	r.mu.RLock()
	path, err := clone(ctx, dir, r.dir, r.config.Branch)
	r.mu.RUnlock()
	if err != nil {
		return err
	}
	return readf(filepath.Join(path, r.config.Path))
}

// Start begins synchronising the repo by cloning it, then fetching
// the required tags and so on.
func (r *Repo) Start(shutdown <-chan struct{}, done *sync.WaitGroup) error {
	defer done.Done()

	for {
		r.mu.RLock()
		url := r.origin.URL
		dir := r.dir
		status := r.status
		r.mu.RUnlock()

		switch status {
		case flux.RepoNoConfig:
			// this is not going to change in the lifetime of this process
			return ErrNoConfig
		case flux.RepoNew:
			ctx := context.Background()
			rootdir, err := ioutil.TempDir(os.TempDir(), "flux-gitclone")
			if err != nil {
				return err
			}

			// FIXME(michael): bare clone or even mirror? these would
			// avoid the remote tracking, and in the case of --mirror,
			// fetch would fetch all refs, which might be convenient.
			dir, err = mirror(ctx, rootdir, url)
			if err == nil {
				r.mu.Lock()
				r.dir = dir
				err = r.fetchRefs(ctx)
				r.mu.Unlock()
			}
			if err == nil {
				r.setStatus(flux.RepoCloned, nil)
				continue // with new status, skipping timer
			} else {
				dir = ""
				os.RemoveAll(rootdir)
				r.setStatus(flux.RepoNew, err)
			}

		case flux.RepoCloned:
			err := checkPush(context.Background(), dir, url)
			if err == nil {
				r.setStatus(flux.RepoReady, nil)
				continue // with new status, skipping timer
			} else {
				r.setStatus(flux.RepoCloned, err)
			}

		case flux.RepoReady:
			if err := r.refreshLoop(shutdown); err != nil {
				r.setStatus(flux.RepoNew, err)
			}
			continue // with possible new status, skipping timer
		}

		tryAgain := time.NewTimer(10 * time.Second)
		select {
		case <-shutdown:
			if !tryAgain.Stop() {
				<-tryAgain.C
			}
			return nil
		case <-tryAgain.C:
			continue
		}
	}
}

func (r *Repo) refresh(ctx context.Context) error {
	// the lock here and below is difficult to avoid; possibly we
	// could clone to another repo and pull there, then swap when complete.
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.fetchRefs(ctx)
}

func (r *Repo) refreshLoop(shutdown <-chan struct{}) error {
	ctx := context.Background()
	gitPoll := time.NewTimer(interval)
	for {
		select {
		case <-shutdown:
			if !gitPoll.Stop() {
				<-gitPoll.C
			}
			return nil
		case <-gitPoll.C:
			if err := r.refresh(ctx); err != nil {
				return err
			}
			gitPoll = time.NewTimer(interval)
		case <-r.notify:
			if !gitPoll.Stop() {
				<-gitPoll.C
			}
			gitPoll = time.NewTimer(interval)
			if err := r.refresh(ctx); err != nil {
				return err
			}
		case req := <-r.request:
			err := r.refresh(ctx)
			req.reply <- err
			return err
		}
	}
}

// fetchRefs collects the refspecs to fetch from upstream, and gets
// them all in one go. It does not lock the repo, since it may be used
// for different purposes.
func (r *Repo) fetchRefs(ctx context.Context) error {
	notesRef, err := getNotesRef(ctx, r.dir, r.config.NotesRef)
	if err != nil {
		return err
	}

	for _, ref := range []string{r.config.Branch, notesRef} {
		refspec := ref + ":" + ref
		if err = fetch(ctx, r.dir, r.origin.URL, refspec); err != nil {
			return err
		}
	}
	return nil
}

// workingClone makes a non-bare clone, at `ref` (probably a branch),
// and returns the filesystem path to it.
func (r *Repo) workingClone(ctx context.Context, ref string) (string, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	working, err := ioutil.TempDir(os.TempDir(), "flux-working")
	if err != nil {
		return "", err
	}
	return clone(ctx, working, r.dir, ref)
}
