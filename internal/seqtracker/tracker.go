package seqtracker

import "sync"

// maxDevices is the upper bound on tracked devices to prevent unbounded memory.
const maxDevices = 10_000

// GapResult describes the outcome of a Track call.
type GapResult struct {
	// GapSize is the number of missing sequence numbers detected.
	// E.g. last_seq=2, new_seq=5 → GapSize=2 (seq 3 and 4 are missing).
	GapSize int64
	// Duplicate is true when the incoming seq has already been seen
	// (seq <= last_seq for that device).
	Duplicate bool
	// FirstSeen is true when this is the first message seen from the device.
	FirstSeen bool
}

type deviceState struct {
	lastSeq int64
}

// Tracker keeps per-device sequence state and detects gaps/duplicates.
// It is safe for concurrent use.
type Tracker struct {
	mu      sync.Mutex
	devices map[string]*deviceState
	// order tracks insertion order for simple LRU eviction.
	order []string
}

// New creates a ready-to-use Tracker.
func New() *Tracker {
	return &Tracker{
		devices: make(map[string]*deviceState, 256),
		order:   make([]string, 0, 256),
	}
}

// Track records a (deviceID, seq) pair and returns gap information.
//
// Rules:
//   - First message from a device: records seq, returns FirstSeen=true.
//   - seq == lastSeq+1: normal, no gap.
//   - seq >  lastSeq+1: gap detected, GapSize = seq - lastSeq - 1.
//   - seq <= lastSeq:   duplicate / out-of-order.
func (t *Tracker) Track(deviceID string, seq int64) GapResult {
	t.mu.Lock()
	defer t.mu.Unlock()

	state, exists := t.devices[deviceID]
	if !exists {
		t.evictIfNeeded()
		t.devices[deviceID] = &deviceState{lastSeq: seq}
		t.order = append(t.order, deviceID)
		return GapResult{FirstSeen: true}
	}

	if seq <= state.lastSeq {
		return GapResult{Duplicate: true}
	}

	gap := seq - state.lastSeq - 1
	state.lastSeq = seq
	if gap > 0 {
		return GapResult{GapSize: gap}
	}
	return GapResult{}
}

// DeviceCount returns the number of devices currently tracked.
func (t *Tracker) DeviceCount() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.devices)
}

// evictIfNeeded removes the oldest device if we've hit maxDevices.
// Must be called with t.mu held.
func (t *Tracker) evictIfNeeded() {
	for len(t.devices) >= maxDevices && len(t.order) > 0 {
		oldest := t.order[0]
		t.order = t.order[1:]
		delete(t.devices, oldest)
	}
}
