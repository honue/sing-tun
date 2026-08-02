//go:build linux || windows || darwin

package tun

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/metacubex/sing/common/control"
	"github.com/metacubex/sing/common/logger"
	"github.com/metacubex/sing/common/x/list"
)

func (m *networkUpdateMonitor) RegisterCallback(callback NetworkUpdateCallback) *list.Element[NetworkUpdateCallback] {
	m.access.Lock()
	defer m.access.Unlock()
	return m.callbacks.PushBack(callback)
}

func (m *networkUpdateMonitor) UnregisterCallback(element *list.Element[NetworkUpdateCallback]) {
	m.access.Lock()
	defer m.access.Unlock()
	m.callbacks.Remove(element)
}

func (m *networkUpdateMonitor) emit() {
	m.access.Lock()
	callbacks := m.callbacks.Array()
	m.access.Unlock()
	for _, callback := range callbacks {
		callback()
	}
}

type defaultInterfaceMonitor struct {
	interfaceFinder       control.InterfaceFinder
	overrideAndroidVPN    bool
	underNetworkExtension bool
	defaultInterface      atomic.Pointer[control.Interface]
	androidVPNEnabled     bool
	noRoute               bool
	networkMonitor        NetworkUpdateMonitor
	checkUpdateTimer      *time.Timer
	checkUpdateAccess     sync.Mutex
	checkUpdateRunning    sync.Mutex
	checkUpdateClosed     bool
	retryAttempt          int
	noRouteChecks         int
	retryDelays           []time.Duration
	networkUpdateDelay    time.Duration
	checkUpdateFunc       func() error
	element               *list.Element[NetworkUpdateCallback]
	access                sync.Mutex
	callbacks             list.List[DefaultInterfaceUpdateCallback]
	logger                logger.Logger
}

func NewDefaultInterfaceMonitor(networkMonitor NetworkUpdateMonitor, logger logger.Logger, options DefaultInterfaceMonitorOptions) (DefaultInterfaceMonitor, error) {
	return &defaultInterfaceMonitor{
		interfaceFinder:       options.InterfaceFinder,
		overrideAndroidVPN:    options.OverrideAndroidVPN,
		underNetworkExtension: options.UnderNetworkExtension,
		networkMonitor:        networkMonitor,
		logger:                logger,
		retryDelays: []time.Duration{
			time.Second,
			2 * time.Second,
			5 * time.Second,
			10 * time.Second,
			30 * time.Second,
		},
		networkUpdateDelay: time.Second,
	}, nil
}

func (m *defaultInterfaceMonitor) Start() error {
	m.element = m.networkMonitor.RegisterCallback(m.delayCheckUpdate)
	m.postCheckUpdate()
	return nil
}

func (m *defaultInterfaceMonitor) delayCheckUpdate() {
	m.checkUpdateAccess.Lock()
	defer m.checkUpdateAccess.Unlock()
	if m.checkUpdateClosed {
		return
	}
	m.retryAttempt = 0
	m.scheduleCheckLocked(m.networkUpdateDelay)
}

func (m *defaultInterfaceMonitor) postCheckUpdate() {
	m.checkUpdateRunning.Lock()
	defer m.checkUpdateRunning.Unlock()
	m.checkUpdateAccess.Lock()
	closed := m.checkUpdateClosed
	m.checkUpdateAccess.Unlock()
	if closed {
		return
	}
	err := m.interfaceFinder.Update()
	if err != nil {
		m.logger.Error("update interface: ", err)
		return
	}
	if m.checkUpdateFunc != nil {
		err = m.checkUpdateFunc()
	} else {
		err = m.checkUpdate()
	}
	if errors.Is(err, ErrNoRoute) {
		m.noRouteChecks++
		if !m.noRoute {
			m.noRoute = true
			m.defaultInterface.Store(nil)
			m.emit(nil, 0)
		}
		m.scheduleRetry()
	} else if err != nil {
		m.logger.Error("check interface: ", err)
	} else {
		wasNoRoute := m.noRoute
		noRouteChecks := m.noRouteChecks
		m.noRoute = false
		m.noRouteChecks = 0
		m.checkUpdateAccess.Lock()
		m.retryAttempt = 0
		m.checkUpdateAccess.Unlock()
		if wasNoRoute && m.logger != nil {
			if defaultInterface := m.defaultInterface.Load(); defaultInterface != nil {
				m.logger.Info("[TUN] default route recovered after ", noRouteChecks, " checks, interface => ", defaultInterface.Name)
			} else {
				m.logger.Info("[TUN] default route recovered after ", noRouteChecks, " checks")
			}
		}
	}
}

func (m *defaultInterfaceMonitor) scheduleRetry() {
	m.checkUpdateAccess.Lock()
	defer m.checkUpdateAccess.Unlock()
	if m.checkUpdateClosed || len(m.retryDelays) == 0 {
		return
	}
	retryIndex := m.retryAttempt
	if retryIndex >= len(m.retryDelays) {
		retryIndex = len(m.retryDelays) - 1
	}
	retryDelay := m.retryDelays[retryIndex]
	if m.logger != nil {
		m.logger.Debug("[TUN] no default route, retry detection in ", retryDelay)
	}
	m.scheduleCheckLocked(retryDelay)
	if m.retryAttempt < len(m.retryDelays)-1 {
		m.retryAttempt++
	}
}

func (m *defaultInterfaceMonitor) scheduleCheckLocked(delay time.Duration) {
	if m.checkUpdateTimer == nil {
		m.checkUpdateTimer = time.AfterFunc(delay, m.postCheckUpdate)
	} else {
		m.checkUpdateTimer.Reset(delay)
	}
}

func (m *defaultInterfaceMonitor) Close() error {
	if m.element != nil {
		m.networkMonitor.UnregisterCallback(m.element)
	}
	m.checkUpdateAccess.Lock()
	m.checkUpdateClosed = true
	if m.checkUpdateTimer != nil {
		m.checkUpdateTimer.Stop()
	}
	m.checkUpdateAccess.Unlock()
	m.checkUpdateRunning.Lock()
	m.checkUpdateRunning.Unlock()
	return nil
}

func (m *defaultInterfaceMonitor) DefaultInterface() *control.Interface {
	return m.defaultInterface.Load()
}

func (m *defaultInterfaceMonitor) OverrideAndroidVPN() bool {
	return m.overrideAndroidVPN
}

func (m *defaultInterfaceMonitor) AndroidVPNEnabled() bool {
	return m.androidVPNEnabled
}

func (m *defaultInterfaceMonitor) RegisterCallback(callback DefaultInterfaceUpdateCallback) *list.Element[DefaultInterfaceUpdateCallback] {
	m.access.Lock()
	defer m.access.Unlock()
	return m.callbacks.PushBack(callback)
}

func (m *defaultInterfaceMonitor) UnregisterCallback(element *list.Element[DefaultInterfaceUpdateCallback]) {
	m.access.Lock()
	defer m.access.Unlock()
	m.callbacks.Remove(element)
}

func (m *defaultInterfaceMonitor) emit(defaultInterface *control.Interface, flags int) {
	m.access.Lock()
	callbacks := m.callbacks.Array()
	m.access.Unlock()
	for _, callback := range callbacks {
		callback(defaultInterface, flags)
	}
}
