/*
Package sync provides the functionality for updating a Chart release
due to (git repo) changes of Charts, while no Custom Resource changes.

Helm operator regularly checks the Chart repo and if new commits are found
all Custom Resources related to the changed Charts are updates, resulting in new
Chart release(s).
*/
package chartsync

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
	"time"

	git "gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/plumbing"
	"gopkg.in/src-d/go-git.v4/plumbing/object"

	"github.com/go-kit/kit/log"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"

	//"gopkg.in/src-d/go-git.v4/plumbing"

	ifv1 "github.com/weaveworks/flux/apis/integrations.flux/v1"
	ifclientset "github.com/weaveworks/flux/integrations/client/clientset/versioned"
	iflister "github.com/weaveworks/flux/integrations/client/listers/integrations.flux/v1"
	helmgit "github.com/weaveworks/flux/integrations/helm/git"
	chartrelease "github.com/weaveworks/flux/integrations/helm/release"
)

type Polling struct {
	Interval time.Duration
	Timeout  time.Duration
}

type ChartChangeSync struct {
	logger log.Logger
	Polling
	release           *chartrelease.Release
	kubeClient        kubernetes.Clientset
	ifClient          ifclientset.Clientset
	fhrLister         iflister.FluxHelmResourceLister
	lastCheckedCommit plumbing.Hash
	sync.RWMutex
}

//  Run ... create a syncing loop
func (chs *ChartChangeSync) Run(stopCh <-chan struct{}) {

	// every UpdateInterval check the git repo
	// 		revision now
	//		pull
	//		revision after pull

	// input :
	//		release, fhrinformer ?,
	// check if repo cloned
	// get the charts dir subdirs:
	//		get all the subdirs
	//			collect all gitchartpaths into a list ($chart subdir)
	//			loop through the list:
	//					are there commits
	//						no changes => nothing to do
	//						changes =>
	//								find all fhrs with the relevant label
	//								loop through them and release the chart
	//

	/*
		pollTimer := time.NewTimer(chs.PollInterval)
		pullThen := func(k func(logger log.Logger) error) {
			defer func() {
				pollTimer.Stop()
				pollTimer = time.NewTimer(chs.PollInterval)
			}()
			ctx, cancel := context.WithTimeout(context.Background(), gitOpTimeout)
			defer cancel()
			if err := chs.release.Pull(ctx); err != nil {
				logger.Log("operation", "pull", "err", err)
				return
			}
			if err := k(logger); err != nil {
				logger.Log("operation", "after-pull", "err", err)
			}
		}
	*/
}

func GetNamespaces(logger log.Logger, kubeClient kubernetes.Clientset) ([]string, error) {
	ns := []string{}

	nso, err := kubeClient.CoreV1().Namespaces().List(metav1.ListOptions{})
	if err != nil {
		errm := fmt.Errorf("Failure while retrieving kybernetes namespaces: %#v", err)
		logger.Log("error", errm.Error())
		return nil, errm
	}

	for _, n := range nso.Items {
		ns = append(ns, n.GetName())
	}

	return ns, nil
}

func (chs *ChartChangeSync) getChartDirs() (map[string]string, error) {
	chartD := make(map[string]string)

	checkout := chs.release.Repo.ChartsSync
	repoRoot := checkout.Dir
	if repoRoot == "" {
		return nil, helmgit.ErrNoRepoCloned
	}
	chartsFullPath := filepath.Join(repoRoot, checkout.Config.Path)

	files, err := ioutil.ReadDir(chartsFullPath)
	if err != nil {
		errm := fmt.Errorf("Failure to access directory %s: %#v", chartsFullPath, err)
		chs.logger.Log("error", errm.Error())
		return nil, errm
	}

	// We only choose subdirectories that represent Charts
	for _, f := range files {
		if f.IsDir() {
			chartDir := filepath.Join(chartsFullPath, f.Name())
			chartMeta := filepath.Join(chartDir, "Chart.yaml")
			if _, err := os.Stat(chartMeta); os.IsNotExist(err) {
				continue
			}
			chartD[f.Name()] = chartDir
		}
	}

	return chartD, nil
}

// newCommits ... determines which charts need to be released
func (chs *ChartChangeSync) newCommits() (bool, error) {
	chs.Lock()
	defer chs.Unlock()

	checkout := chs.release.Repo.ChartsSync

	// get previous revision
	if checkout.Dir == "" {
		ctx, cancel := context.WithTimeout(context.Background(), chs.Polling.Timeout)
		err := checkout.Clone(ctx, helmgit.ChartsChangesClone)
		cancel()
		if err != nil {
			errm := fmt.Errorf("Failure while cloning repo : %#v", err)
			chs.logger.Log("error", errm.Error())
			return false, errm
		}
	}

	// pull
	err := checkout.Pull()
	if err != nil {
		errm := fmt.Errorf("Failure while pulling repo: %#v", err)
		chs.logger.Log("error", errm.Error())
		return false, errm
	}
	// get latest revision
	newRev, err := checkout.GetRevision()
	if err != nil {
		errm := fmt.Errorf("Failure while getting repo revision: %#v", err)
		chs.logger.Log("error", errm.Error())
		return false, errm
	}

	// if lastCheckedCommit field is missing then all charts need to be assessed
	oldRev := chs.lastCheckedCommit
	if oldRev.String() == "" {
		chs.lastCheckedCommit = newRev

		return true, nil
	}

	// go-git.v4 does not provide a possibility to find commit for a particular path.
	// So we find if there are any commits at all sice last time (since oldRev)
	commitIter, err := checkout.Repo.Log(&git.LogOptions{From: oldRev})
	if err != nil {
		errm := fmt.Errorf("Failure while getting commit info: %#v", err)
		chs.logger.Log("error", errm.Error())
		return false, errm
	}
	var count int
	err = commitIter.ForEach(func(c *object.Commit) error {
		count = count + 1
		return nil
	})
	if err != nil {
		errm := fmt.Errorf("Failure while getting commit info: %#v", err)
		chs.logger.Log("error", errm.Error())
		return false, errm
	}

	if count > 0 {
		return true, nil
	}
	return false, nil
}

func (chs *ChartChangeSync) getCustomResources(namespaces []string, chart string, nsFhrs map[string][]ifv1.FluxHelmResource) error {
	fhrs := []ifv1.FluxHelmResource{}

	chartSelector := map[string]string{
		"chart": chart,
	}
	labelsSet := labels.Set(chartSelector)
	listOptions := metav1.ListOptions{LabelSelector: labelsSet.AsSelector().String()}

	for _, ns := range namespaces {
		list, err := chs.ifClient.IntegrationsV1().FluxHelmResources(ns).List(listOptions)
		if err != nil {
			chs.logger.Log("error", fmt.Errorf("Failure while retrieving FluxHelmResources: %#v", err))
			continue
		}

		for _, fhr := range list.Items {
			fmt.Printf("\n>>>  %v \n\n", fhr)
			fhrs = append(fhrs, fhr)
		}
	}
	nsFhrs[chart] = fhrs

	return nil
}

// releaseCharts ... release a Chart if required
//		input:
//					chartD ... provides chart name and its directory information
//					fhr ...... provides chart name and all Custom Resources associated with this chart
//		does a dry run and compares the manifests (and value file?) If differences => release)
func (chs *ChartChangeSync) releaseCharts(chartD map[string]string, fhr map[string][]ifv1.FluxHelmResource) error {

	return nil
}
