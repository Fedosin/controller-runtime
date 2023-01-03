/*
Copyright 2023 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package lazyrestmapper

import (
	"sync"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/restmapper"
)

// LazyRESTMapper is a RESTMapper that will lazily query the provided
// client for discovery information to do REST mappings.
type LazyRESTMapper struct {
	mapper      meta.RESTMapper
	client      *discovery.DiscoveryClient
	knownGroups map[string]*restmapper.APIGroupResources

	// mutex to provide thread-safe mapper reloading
	mu sync.Mutex
}

// NewLazyRESTMapper initializes a LazyRESTMapper
func NewLazyRESTMapper(c *rest.Config) (meta.RESTMapper, error) {
	dc, err := discovery.NewDiscoveryClientForConfig(c)
	if err != nil {
		return nil, err
	}

	return &LazyRESTMapper{
		mapper:      restmapper.NewDiscoveryRESTMapper([]*restmapper.APIGroupResources{}),
		client:      dc,
		knownGroups: map[string]*restmapper.APIGroupResources{},
	}, nil
}

func (m *LazyRESTMapper) addKnownGroupAndReload(groupName string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	groupResources, err := getFilteredAPIGroupResources(m.client, groupName)
	if err != nil || groupResources == nil {
		return err
	}

	m.knownGroups[groupName] = groupResources

	updatedGroupResources := make([]*restmapper.APIGroupResources, 0, len(m.knownGroups))
	for _, v := range m.knownGroups {
		updatedGroupResources = append(updatedGroupResources, v)
	}

	m.mapper = restmapper.NewDiscoveryRESTMapper(updatedGroupResources)

	return nil
}

// KindFor implements Mapper.KindFor
func (m *LazyRESTMapper) KindFor(resource schema.GroupVersionResource) (schema.GroupVersionKind, error) {
	res, err := m.mapper.KindFor(resource)
	if meta.IsNoMatchError(err) {
		if err = m.addKnownGroupAndReload(resource.Group); err != nil {
			return res, err
		}
		res, err = m.mapper.KindFor(resource)
	}
	return res, err
}

// KindsFor implements Mapper.KindsFor
func (m *LazyRESTMapper) KindsFor(resource schema.GroupVersionResource) ([]schema.GroupVersionKind, error) {
	res, err := m.mapper.KindsFor(resource)
	if meta.IsNoMatchError(err) {
		if err = m.addKnownGroupAndReload(resource.Group); err != nil {
			return res, err
		}
		res, err = m.mapper.KindsFor(resource)
	}
	return res, err
}

// ResourceFor implements Mapper.ResourceFor
func (m *LazyRESTMapper) ResourceFor(input schema.GroupVersionResource) (schema.GroupVersionResource, error) {
	res, err := m.mapper.ResourceFor(input)
	if meta.IsNoMatchError(err) {
		if err = m.addKnownGroupAndReload(input.Group); err != nil {
			return res, err
		}
		res, err = m.mapper.ResourceFor(input)
	}
	return res, err
}

// ResourcesFor implements Mapper.ResourcesFor
func (m *LazyRESTMapper) ResourcesFor(input schema.GroupVersionResource) ([]schema.GroupVersionResource, error) {
	res, err := m.mapper.ResourcesFor(input)
	if meta.IsNoMatchError(err) {
		if err = m.addKnownGroupAndReload(input.Group); err != nil {
			return res, err
		}
		res, err = m.mapper.ResourcesFor(input)
	}
	return res, err
}

// RESTMapping implements Mapper.RESTMapping
func (m *LazyRESTMapper) RESTMapping(gk schema.GroupKind, versions ...string) (*meta.RESTMapping, error) {
	res, err := m.mapper.RESTMapping(gk, versions...)
	if meta.IsNoMatchError(err) {
		if err = m.addKnownGroupAndReload(gk.Group); err != nil {
			return res, err
		}
		res, err = m.mapper.RESTMapping(gk, versions...)
	}
	return res, err
}

// RESTMappings implements Mapper.RESTMappings
func (m *LazyRESTMapper) RESTMappings(gk schema.GroupKind, versions ...string) ([]*meta.RESTMapping, error) {
	res, err := m.mapper.RESTMappings(gk, versions...)
	if meta.IsNoMatchError(err) {
		if err = m.addKnownGroupAndReload(gk.Group); err != nil {
			return res, err
		}
		res, err = m.mapper.RESTMappings(gk, versions...)
	}
	return res, err
}

// ResourceSingularizer implements Mapper.ResourceSingularizer
func (m *LazyRESTMapper) ResourceSingularizer(resource string) (string, error) {
	return m.mapper.ResourceSingularizer(resource)
}

// fetchGroupVersionResources uses the discovery client to fetch the resources for the specified group in parallel.
// Mainly replicates the same named function from the client-go internals aside from the changed `apiGroups` argument type (uses slice instead of a single group).
// ref: https://github.com/kubernetes/kubernetes/blob/a84d877310ba5cf9237c8e8e3218229c202d3a1e/staging/src/k8s.io/client-go/discovery/discovery_client.go#L506
func fetchGroupVersionResources(d discovery.DiscoveryInterface, apiGroup metav1.APIGroup) (map[schema.GroupVersion]*metav1.APIResourceList, map[schema.GroupVersion]error) {
	groupVersionResources := make(map[schema.GroupVersion]*metav1.APIResourceList)
	failedGroups := make(map[schema.GroupVersion]error)

	wg := &sync.WaitGroup{}
	resultLock := &sync.Mutex{}
	for _, version := range apiGroup.Versions {
		groupVersion := schema.GroupVersion{Group: apiGroup.Name, Version: version.Version}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer utilruntime.HandleCrash()

			apiResourceList, err := d.ServerResourcesForGroupVersion(groupVersion.String())

			// lock to record results
			resultLock.Lock()
			defer resultLock.Unlock()

			if err != nil {
				// TODO: maybe restrict this to NotFound errors
				failedGroups[groupVersion] = err
			}
			if apiResourceList != nil {
				// even in case of error, some fallback might have been returned
				groupVersionResources[groupVersion] = apiResourceList
			}
		}()
	}
	wg.Wait()

	return groupVersionResources, failedGroups
}

// filteredServerGroupsAndResources returns the supported resources for groups filtered by passed predicate and versions.
// Mainly replicate ServerGroupsAndResources function from the client-go. The difference is that this function takes
// a group name as an argument for filtering out unwanted groups.
// ref: https://github.com/kubernetes/kubernetes/blob/a84d877310ba5cf9237c8e8e3218229c202d3a1e/staging/src/k8s.io/client-go/discovery/discovery_client.go#L383
func filteredServerGroupsAndResources(d discovery.DiscoveryInterface, groupName string) (*metav1.APIGroup, []*metav1.APIResourceList, error) {
	sgs, err := d.ServerGroups()
	if sgs == nil {
		return nil, nil, err
	}

	apiGroup := metav1.APIGroup{}
	for i := range sgs.Groups {
		if groupName == (&sgs.Groups[i]).Name {
			apiGroup = sgs.Groups[i]
		}
	}

	groupVersionResources, failedGroups := fetchGroupVersionResources(d, apiGroup)

	// order results by group/version discovery order
	result := []*metav1.APIResourceList{}
	for _, version := range apiGroup.Versions {
		gv := schema.GroupVersion{Group: apiGroup.Name, Version: version.Version}
		if resources, ok := groupVersionResources[gv]; ok {
			result = append(result, resources)
		}
	}

	if len(failedGroups) == 0 {
		return &apiGroup, result, nil
	}

	return &apiGroup, result, &discovery.ErrGroupDiscoveryFailed{Groups: failedGroups}
}

// getFilteredAPIGroupResources uses the provided discovery client to gather
// discovery information and populate a slice of APIGroupResources.
func getFilteredAPIGroupResources(cl discovery.DiscoveryInterface, groupName string) (*restmapper.APIGroupResources, error) {
	group, rs, err := filteredServerGroupsAndResources(cl, groupName)
	if rs == nil || group == nil {
		return nil, err
		// TODO track the errors and update callers to handle partial errors.
	}
	rsm := map[string]*metav1.APIResourceList{}
	for _, r := range rs {
		rsm[r.GroupVersion] = r
	}

	groupResources := &restmapper.APIGroupResources{
		Group:              *group,
		VersionedResources: make(map[string][]metav1.APIResource),
	}
	for _, version := range group.Versions {
		resources, ok := rsm[version.GroupVersion]
		if !ok {
			continue
		}
		groupResources.VersionedResources[version.Version] = resources.APIResources
	}

	return groupResources, nil
}
