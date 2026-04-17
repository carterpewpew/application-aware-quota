package clusterquotamapping

import (
	"sync"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	quotav1 "github.com/openshift/api/quota/v1"
	"kubevirt.io/application-aware-quota/staging/src/kubevirt.io/application-aware-quota-api/pkg/apis/core/v1alpha1"
)

// reentrantListener calls back into the mapper's read methods during notification,
// which would deadlock if listeners were invoked under the write lock.
type reentrantListener struct {
	mapper   *clusterQuotaMapper
	addCalls int
	rmCalls  int
}

func (l *reentrantListener) AddMapping(quotaName, namespaceName string) {
	l.addCalls++
	l.mapper.GetNamespacesFor(quotaName)
	l.mapper.GetClusterQuotasFor(namespaceName)
}

func (l *reentrantListener) RemoveMapping(quotaName, namespaceName string) {
	l.rmCalls++
	l.mapper.GetNamespacesFor(quotaName)
	l.mapper.GetClusterQuotasFor(namespaceName)
}

func testQuota(name string) *v1alpha1.ApplicationAwareClusterResourceQuota {
	return &v1alpha1.ApplicationAwareClusterResourceQuota{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: v1alpha1.ApplicationAwareClusterResourceQuotaSpec{
			ClusterResourceQuotaSpec: quotav1.ClusterResourceQuotaSpec{
				Selector: quotav1.ClusterResourceQuotaSelector{
					AnnotationSelector: map[string]string{"openshift.io/requester": "user-a"},
				},
			},
		},
	}
}

type fakeNamespace struct {
	metav1.ObjectMeta
}

func (f *fakeNamespace) GetNamespace() string { return "" }

func testNamespace(name string) *fakeNamespace {
	return &fakeNamespace{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Labels:      map[string]string{},
			Annotations: map[string]string{"openshift.io/requester": "user-a"},
		},
	}
}

func TestSetMappingNoDeadlockWithReentrantListener(t *testing.T) {
	mapper := NewClusterQuotaMapper()
	listener := &reentrantListener{mapper: mapper}
	mapper.AddListener(listener)

	quota := testQuota("quota1")
	ns := testNamespace("ns1")

	mapper.requireQuota(quota)
	mapper.requireNamespace(ns)

	done := make(chan struct{})
	go func() {
		defer close(done)
		mapper.setMapping(quota, ns, false)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("setMapping deadlocked with reentrant listener")
	}

	if listener.addCalls != 1 {
		t.Errorf("expected 1 AddMapping call, got %d", listener.addCalls)
	}
}

func TestSetMappingRemoveNoDeadlock(t *testing.T) {
	mapper := NewClusterQuotaMapper()
	listener := &reentrantListener{mapper: mapper}
	mapper.AddListener(listener)

	quota := testQuota("quota1")
	ns := testNamespace("ns1")

	mapper.requireQuota(quota)
	mapper.requireNamespace(ns)
	mapper.setMapping(quota, ns, false)

	done := make(chan struct{})
	go func() {
		defer close(done)
		mapper.setMapping(quota, ns, true)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("setMapping (remove) deadlocked with reentrant listener")
	}

	if listener.rmCalls != 1 {
		t.Errorf("expected 1 RemoveMapping call, got %d", listener.rmCalls)
	}
}

func TestRemoveQuotaNoDeadlock(t *testing.T) {
	mapper := NewClusterQuotaMapper()
	listener := &reentrantListener{mapper: mapper}
	mapper.AddListener(listener)

	quota := testQuota("quota1")
	ns := testNamespace("ns1")

	mapper.requireQuota(quota)
	mapper.requireNamespace(ns)
	mapper.setMapping(quota, ns, false)

	done := make(chan struct{})
	go func() {
		defer close(done)
		mapper.removeQuota("quota1")
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("removeQuota deadlocked with reentrant listener")
	}

	if listener.rmCalls != 1 {
		t.Errorf("expected 1 RemoveMapping call, got %d", listener.rmCalls)
	}
}

func TestRemoveNamespaceNoDeadlock(t *testing.T) {
	mapper := NewClusterQuotaMapper()
	listener := &reentrantListener{mapper: mapper}
	mapper.AddListener(listener)

	quota := testQuota("quota1")
	ns := testNamespace("ns1")

	mapper.requireQuota(quota)
	mapper.requireNamespace(ns)
	mapper.setMapping(quota, ns, false)

	done := make(chan struct{})
	go func() {
		defer close(done)
		mapper.removeNamespace("ns1")
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("removeNamespace deadlocked with reentrant listener")
	}

	if listener.rmCalls != 1 {
		t.Errorf("expected 1 RemoveMapping call, got %d", listener.rmCalls)
	}
}

func TestConcurrentSetMappingAndReads(t *testing.T) {
	mapper := NewClusterQuotaMapper()
	listener := &reentrantListener{mapper: mapper}
	mapper.AddListener(listener)

	quota := testQuota("quota1")
	ns := testNamespace("ns1")
	mapper.requireQuota(quota)
	mapper.requireNamespace(ns)

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			mapper.setMapping(quota, ns, false)
		}()
		go func() {
			defer wg.Done()
			mapper.GetNamespacesFor("quota1")
			mapper.GetClusterQuotasFor("ns1")
		}()
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("concurrent setMapping and reads deadlocked")
	}
}
