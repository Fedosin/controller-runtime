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
	"testing"

	gmg "github.com/onsi/gomega"

	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
)

func setupEnvtest(t *testing.T) (*rest.Config, func(t *testing.T)) {
	t.Log("Setup envtest")
	g := gmg.NewWithT(t)
	testEnv := &envtest.Environment{}
	cfg, err := testEnv.Start()
	g.Expect(err).NotTo(gmg.HaveOccurred())
	g.Expect(cfg).NotTo(gmg.BeNil())

	teardownFunc := func(t *testing.T) {
		t.Log("Stop envtest")
		g.Expect(testEnv.Stop()).To(gmg.Succeed())
	}
	return cfg, teardownFunc
}

func TestLazyRestMapperProvider(t *testing.T) {
	restCfg, tearDownFn := setupEnvtest(t)
	defer tearDownFn(t)

	t.Run("getFilteredAPIGroupResources should return APIGroupResources based on passed arguments", func(t *testing.T) {
		g := gmg.NewWithT(t)

		discoveryClient, err := discovery.NewDiscoveryClientForConfig(restCfg)
		g.Expect(err).NotTo(gmg.HaveOccurred())

		// Get GroupResources for kubernetes core and kubernetes apps groups
		filteredAPIGroupResources, err := getFilteredAPIGroupResources(discoveryClient, "apps")
		g.Expect(err).NotTo(gmg.HaveOccurred())
		g.Expect(filteredAPIGroupResources.Group.Name).To(gmg.Equal("apps"))
	})

	t.Run("LazyRESTMapper should fetch data based on the request", func(t *testing.T) {
		g := gmg.NewWithT(t)

		lazyRestMapper, err := NewLazyRESTMapper(restCfg)
		g.Expect(err).NotTo(gmg.HaveOccurred())

		mapping, err := lazyRestMapper.RESTMapping(schema.GroupKind{Group: "apps", Kind: "deployment"})
		g.Expect(err).NotTo(gmg.HaveOccurred())
		g.Expect(mapping.GroupVersionKind.Kind).To(gmg.Equal("deployment"))

		mappings, err := lazyRestMapper.RESTMappings(schema.GroupKind{Group: "", Kind: "pod"})
		g.Expect(err).NotTo(gmg.HaveOccurred())
		g.Expect(len(mappings)).To(gmg.Equal(1))
		g.Expect(mappings[0].GroupVersionKind.Kind).To(gmg.Equal("pod"))

		kind, err := lazyRestMapper.KindFor(schema.GroupVersionResource{Group: "networking.k8s.io", Version: "v1", Resource: "ingresses"})
		g.Expect(err).NotTo(gmg.HaveOccurred())
		g.Expect(kind.Kind).To(gmg.Equal("Ingress"))

		kinds, err := lazyRestMapper.KindsFor(schema.GroupVersionResource{Group: "policy", Version: "v1", Resource: "poddisruptionbudgets"})
		g.Expect(err).NotTo(gmg.HaveOccurred())
		g.Expect(len(kinds)).To(gmg.Equal(1))
		g.Expect(kinds[0].Kind).To(gmg.Equal("PodDisruptionBudget"))

		resource, err := lazyRestMapper.ResourceFor(schema.GroupVersionResource{Group: "discovery.k8s.io", Version: "v1", Resource: "endpointslices"})
		g.Expect(err).NotTo(gmg.HaveOccurred())
		g.Expect(resource.Resource).To(gmg.Equal("endpointslices"))

		resources, err := lazyRestMapper.ResourcesFor(schema.GroupVersionResource{Group: "events.k8s.io", Version: "v1", Resource: "events"})
		g.Expect(err).NotTo(gmg.HaveOccurred())
		g.Expect(len(resources)).To(gmg.Equal(1))
		g.Expect(resources[0].Resource).To(gmg.Equal("events"))
	})

	t.Run("LazyRESTMapper should return an error if the resource doesn't exist", func(t *testing.T) {
		g := gmg.NewWithT(t)

		lazyRestMapper, err := NewLazyRESTMapper(restCfg)
		g.Expect(err).NotTo(gmg.HaveOccurred())

		_, err = lazyRestMapper.RESTMapping(schema.GroupKind{Group: "INVALID"})
		g.Expect(err).To(gmg.HaveOccurred())

		_, err = lazyRestMapper.RESTMappings(schema.GroupKind{Group: "INVALID"})
		g.Expect(err).To(gmg.HaveOccurred())

		_, err = lazyRestMapper.KindFor(schema.GroupVersionResource{Group: "INVALID"})
		g.Expect(err).To(gmg.HaveOccurred())

		_, err = lazyRestMapper.KindsFor(schema.GroupVersionResource{Group: "INVALID"})
		g.Expect(err).To(gmg.HaveOccurred())

		_, err = lazyRestMapper.ResourceFor(schema.GroupVersionResource{Group: "INVALID"})
		g.Expect(err).To(gmg.HaveOccurred())

		_, err = lazyRestMapper.ResourcesFor(schema.GroupVersionResource{Group: "INVALID"})
		g.Expect(err).To(gmg.HaveOccurred())
	})
}
