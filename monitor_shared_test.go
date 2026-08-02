//go:build linux || windows || darwin

package tun

import (
	"net/netip"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/metacubex/sing/common/control"
	"github.com/metacubex/sing/common/x/list"
)

type testInterfaceFinder struct{}

func (f *testInterfaceFinder) Update() error                             { return nil }
func (f *testInterfaceFinder) Interfaces() []control.Interface           { return nil }
func (f *testInterfaceFinder) ByName(string) (*control.Interface, error) { return nil, nil }
func (f *testInterfaceFinder) ByIndex(int) (*control.Interface, error)   { return nil, nil }
func (f *testInterfaceFinder) ByAddr(netip.Addr) (*control.Interface, error) {
	return nil, nil
}

type testNetworkUpdateMonitor struct {
	access    sync.Mutex
	callbacks list.List[NetworkUpdateCallback]
}

func (m *testNetworkUpdateMonitor) Start() error { return nil }
func (m *testNetworkUpdateMonitor) Close() error { return nil }

func (m *testNetworkUpdateMonitor) RegisterCallback(callback NetworkUpdateCallback) *list.Element[NetworkUpdateCallback] {
	m.access.Lock()
	defer m.access.Unlock()
	return m.callbacks.PushBack(callback)
}

func (m *testNetworkUpdateMonitor) UnregisterCallback(element *list.Element[NetworkUpdateCallback]) {
	m.access.Lock()
	defer m.access.Unlock()
	m.callbacks.Remove(element)
}

func (m *testNetworkUpdateMonitor) emit() {
	m.access.Lock()
	callbacks := m.callbacks.Array()
	m.access.Unlock()
	for _, callback := range callbacks {
		callback()
	}
}

func newTestDefaultInterfaceMonitor(t *testing.T, networkMonitor NetworkUpdateMonitor) *defaultInterfaceMonitor {
	t.Helper()
	monitor, err := NewDefaultInterfaceMonitor(networkMonitor, nil, DefaultInterfaceMonitorOptions{
		InterfaceFinder: &testInterfaceFinder{},
	})
	if err != nil {
		t.Fatal(err)
	}
	return monitor.(*defaultInterfaceMonitor)
}

func TestDefaultInterfaceMonitorRetriesAfterNoRoute(t *testing.T) {
	networkMonitor := &testNetworkUpdateMonitor{}
	monitor := newTestDefaultInterfaceMonitor(t, networkMonitor)
	monitor.retryDelays = []time.Duration{5 * time.Millisecond, 10 * time.Millisecond}

	var attempts atomic.Int32
	recovered := make(chan struct{})
	monitor.checkUpdateFunc = func() error {
		if attempts.Add(1) < 3 {
			return ErrNoRoute
		}
		select {
		case <-recovered:
		default:
			close(recovered)
		}
		return nil
	}

	if err := monitor.Start(); err != nil {
		t.Fatal(err)
	}
	defer monitor.Close()

	select {
	case <-recovered:
	case <-time.After(time.Second):
		t.Fatal("default interface monitor did not retry after ErrNoRoute")
	}

	time.Sleep(30 * time.Millisecond)
	if actual := attempts.Load(); actual != 3 {
		t.Fatalf("unexpected checks after recovery: got %d, want 3", actual)
	}
}

func TestDefaultInterfaceMonitorNetworkUpdateInterruptsBackoff(t *testing.T) {
	networkMonitor := &testNetworkUpdateMonitor{}
	monitor := newTestDefaultInterfaceMonitor(t, networkMonitor)
	monitor.retryDelays = []time.Duration{time.Hour}
	monitor.networkUpdateDelay = 5 * time.Millisecond

	var attempts atomic.Int32
	recovered := make(chan struct{})
	monitor.checkUpdateFunc = func() error {
		if attempts.Add(1) == 1 {
			return ErrNoRoute
		}
		close(recovered)
		return nil
	}

	if err := monitor.Start(); err != nil {
		t.Fatal(err)
	}
	defer monitor.Close()
	networkMonitor.emit()

	select {
	case <-recovered:
	case <-time.After(time.Second):
		t.Fatal("network update did not interrupt retry backoff")
	}
}

func TestDefaultInterfaceMonitorCloseStopsRetry(t *testing.T) {
	networkMonitor := &testNetworkUpdateMonitor{}
	monitor := newTestDefaultInterfaceMonitor(t, networkMonitor)
	monitor.retryDelays = []time.Duration{10 * time.Millisecond}

	var attempts atomic.Int32
	monitor.checkUpdateFunc = func() error {
		attempts.Add(1)
		return ErrNoRoute
	}

	if err := monitor.Start(); err != nil {
		t.Fatal(err)
	}
	if err := monitor.Close(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(30 * time.Millisecond)

	if actual := attempts.Load(); actual != 1 {
		t.Fatalf("retry ran after close: got %d checks, want 1", actual)
	}
}
