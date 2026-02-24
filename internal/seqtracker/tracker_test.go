package seqtracker

import (
	"sync"
	"testing"
)

func TestTrack_FirstMessage(t *testing.T) {
	tr := New()
	result := tr.Track("dev-1", 1)
	if !result.FirstSeen {
		t.Fatal("expected FirstSeen=true for new device")
	}
	if result.GapSize != 0 || result.Duplicate {
		t.Fatal("expected no gap and no duplicate for first message")
	}
	if tr.DeviceCount() != 1 {
		t.Fatalf("expected 1 device, got %d", tr.DeviceCount())
	}
}

func TestTrack_Sequential(t *testing.T) {
	tr := New()
	tr.Track("dev-1", 1)
	for seq := int64(2); seq <= 10; seq++ {
		result := tr.Track("dev-1", seq)
		if result.GapSize != 0 {
			t.Fatalf("seq %d: expected no gap, got %d", seq, result.GapSize)
		}
		if result.Duplicate {
			t.Fatalf("seq %d: unexpected duplicate", seq)
		}
		if result.FirstSeen {
			t.Fatalf("seq %d: unexpected first seen", seq)
		}
	}
}

func TestTrack_GapDetection(t *testing.T) {
	tr := New()
	tr.Track("dev-1", 1)
	tr.Track("dev-1", 2)

	// Skip seq 3 and 4
	result := tr.Track("dev-1", 5)
	if result.GapSize != 2 {
		t.Fatalf("expected gap=2, got %d", result.GapSize)
	}
	if result.Duplicate {
		t.Fatal("should not be duplicate")
	}
}

func TestTrack_Duplicate(t *testing.T) {
	tr := New()
	tr.Track("dev-1", 1)
	tr.Track("dev-1", 2)

	result := tr.Track("dev-1", 2)
	if !result.Duplicate {
		t.Fatal("expected duplicate=true")
	}
	if result.GapSize != 0 {
		t.Fatal("gap should be 0 for duplicate")
	}
}

func TestTrack_OutOfOrder(t *testing.T) {
	tr := New()
	tr.Track("dev-1", 1)
	tr.Track("dev-1", 3) // gap

	result := tr.Track("dev-1", 2) // out-of-order, seq <= lastSeq
	if !result.Duplicate {
		t.Fatal("expected duplicate for out-of-order message (seq <= lastSeq)")
	}
}

func TestTrack_MultipleDevices(t *testing.T) {
	tr := New()
	tr.Track("dev-1", 1)
	tr.Track("dev-2", 1)
	tr.Track("dev-1", 2)
	tr.Track("dev-2", 5) // gap of 3

	if tr.DeviceCount() != 2 {
		t.Fatalf("expected 2 devices, got %d", tr.DeviceCount())
	}
}

func TestTrack_LargeGap(t *testing.T) {
	tr := New()
	tr.Track("dev-1", 1)
	result := tr.Track("dev-1", 1001)
	if result.GapSize != 999 {
		t.Fatalf("expected gap=999, got %d", result.GapSize)
	}
}

func TestTrack_ConcurrentAccess(t *testing.T) {
	tr := New()
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			deviceID := "dev-concurrent"
			for seq := int64(1); seq <= 50; seq++ {
				tr.Track(deviceID, seq)
			}
		}(i)
	}
	wg.Wait()
	if tr.DeviceCount() != 1 {
		t.Fatalf("expected 1 device, got %d", tr.DeviceCount())
	}
}
