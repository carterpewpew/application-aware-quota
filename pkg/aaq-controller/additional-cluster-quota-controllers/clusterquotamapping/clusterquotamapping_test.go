package clusterquotamapping

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"

	quotav1 "github.com/openshift/api/quota/v1"
	v1alpha1 "kubevirt.io/application-aware-quota/staging/src/kubevirt.io/application-aware-quota-api/pkg/apis/core/v1alpha1"
)

type fakeNamespace struct {
	metav1.ObjectMeta
}

type fakeNamespaceLister struct {
	namespaces []metav1.Object
}

func (f *fakeNamespaceLister) Each(_ labels.Selector, fn func(metav1.Object) bool) error {
	for _, ns := range f.namespaces {
		if !fn(ns) {
			return nil
		}
	}
	return nil
}

func (f *fakeNamespaceLister) Get(name string) (metav1.Object, error) {
	for _, ns := range f.namespaces {
		if ns.GetName() == name {
			return ns, nil
		}
	}
	return nil, nil
}

func newTestQuota(name string) *v1alpha1.ApplicationAwareClusterResourceQuota {
	return &v1alpha1.ApplicationAwareClusterResourceQuota{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: v1alpha1.ApplicationAwareClusterResourceQuotaSpec{
			ClusterResourceQuotaSpec: quotav1.ClusterResourceQuotaSpec{
				Selector: quotav1.ClusterResourceQuotaSelector{
					AnnotationSelector: map[string]string{"quota": name},
				},
			},
		},
	}
}

func TestSyncQuota_CompletesOnFullIteration(t *testing.T) {
	quota := newTestQuota("test-quota")
	ns := &fakeNamespace{ObjectMeta: metav1.ObjectMeta{
		Name:        "ns1",
		Annotations: map[string]string{"quota": "test-quota"},
	}}

	mapper := NewClusterQuotaMapper()
	mapper.requireQuota(quota)
	mapper.requireNamespace(ns)

	c := &ClusterQuotaMappingController{
		namespaceLister:  &fakeNamespaceLister{namespaces: []metav1.Object{ns}},
		clusterQuotaMapper: mapper,
	}

	if err := c.syncQuota(quota); err != nil {
		t.Fatalf("syncQuota returned error: %v", err)
	}

	if _, ok := mapper.completedQuotaToSelector[quota.Name]; !ok {
		t.Error("expected completeQuota to be called after full iteration, but completedQuotaToSelector is missing the quota")
	}
}

func TestSyncQuota_DoesNotCompleteOnAbort(t *testing.T) {
	quota := newTestQuota("test-quota")
	ns1 := &fakeNamespace{ObjectMeta: metav1.ObjectMeta{
		Name:        "ns1",
		Annotations: map[string]string{"quota": "test-quota"},
	}}
	ns2 := &fakeNamespace{ObjectMeta: metav1.ObjectMeta{
		Name:        "ns2",
		Annotations: map[string]string{"quota": "test-quota"},
	}}

	mapper := NewClusterQuotaMapper()
	mapper.requireQuota(quota)
	mapper.requireNamespace(ns1)
	mapper.requireNamespace(ns2)

	c := &ClusterQuotaMappingController{
		namespaceLister:  &fakeNamespaceLister{namespaces: []metav1.Object{ns1, ns2}},
		clusterQuotaMapper: mapper,
	}

	// Simulate a concurrent quota update by changing the required selector
	// after the controller has started. This makes setMapping return quotaMatches=false
	// on the first namespace, triggering the abort path.
	staleQuota := newTestQuota("test-quota")
	staleQuota.Spec.Selector.AnnotationSelector = map[string]string{"quota": "old-value"}

	if err := c.syncQuota(staleQuota); err != nil {
		t.Fatalf("syncQuota returned error: %v", err)
	}

	if _, ok := mapper.completedQuotaToSelector[staleQuota.Name]; ok {
		t.Error("expected completeQuota NOT to be called after abort, but completedQuotaToSelector contains the quota")
	}
}
