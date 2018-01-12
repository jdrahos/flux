package git

import (
	"errors"
	"io/ioutil"
	"os"
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
	status flux.GitRepoStatus
	err    error
	notify chan struct{}
	dir    string
	mu     sync.RWMutex
}

func NewRepo(origin Remote, config Config) *Repo {
	r := &Repo{
		origin: origin,
		config: config,
		status: flux.RepoNew,
		err:    nil,
		notify: make(chan struct{}, 1), // `1` so that Notify doesn't block
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

// Clone, and any read ops
// ... FIXME(michael): Any read ops?

// Start begins synchronising the repo by cloning it, then fetching
// the required tags and so on.
func (r *Repo) Start(shutdown <-chan struct{}, done *sync.WaitGroup) error {
	defer done.Done()

	for {
		r.mu.RLock()
		url := r.origin.URL
		dir := r.dir
		r.mu.RUnlock()

		ctx := context.Background()

		if dir == "" { // not cloned yet
			rootdir, err := ioutil.TempDir(os.TempDir(), "flux-gitclone")
			if err != nil {
				return err
			}

			// FIXME(michael): bare clone or even mirror? these would
			// avoid the remote tracking, and in the case of --mirror,
			// fetch would fetch all refs, which might be convenient.
			dir, err = clone(ctx, rootdir, url, "")
			if err == nil {
				r.mu.Lock()
				r.dir = dir
				err = r.fetchRefs(ctx)
				r.mu.Unlock()
			}

			if err == nil {
				r.setStatus(flux.RepoCloned, nil)
			} else {
				dir = ""
				os.RemoveAll(rootdir)
				r.setStatus(flux.RepoNew, err)
			}
		}

		if dir != "" { // not known to be writable yet
			err := checkPush(ctx, dir, url)
			if err == nil {
				r.setStatus(flux.RepoReady, nil)
				break
			} else {
				r.setStatus(flux.RepoCloned, err)
			}
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

	return r.refreshLoop(shutdown)
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
			if err := r.refresh(ctx); err != nil {
				return err
			}
			if !gitPoll.Stop() {
				<-gitPoll.C
			}
			gitPoll = time.NewTimer(interval)
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
