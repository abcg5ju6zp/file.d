package pipeline

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/ozontech/file.d/logger"
	"github.com/ozontech/file.d/offset"
	"go.uber.org/atomic"
	"gopkg.in/yaml.v2"
)

const (
	// defaultBarrierSaveInterval defines how often the progress table is flushed to disk.
	defaultBarrierSaveInterval = time.Second
	// maxBarrierEntries bounds the number of tracked event entries.
	maxBarrierEntries = 100_000

	// OffOutputOffsets disables durable per-destination progress when set as
	// the output_offsets_file setting.
	OffOutputOffsets = "off"
)

// DefaultOutputOffsetsDir is the default directory of the barrier progress file.
const DefaultOutputOffsetsDir = "offsets"

type storedBranch struct {
	SourceID uint64 `yaml:"source_id"`
	Offset   int64  `yaml:"offset"`
	Mask     uint64 `yaml:"mask"`
}

type storedBranches struct {
	Entries []storedBranch `yaml:"entries"`
}

type entryKey struct {
	sourceID uint64
	offset   int64
}

// barrierStore is the durable per-event, per-destination convergence table.
// Keys are (SourceID, Offset); the value is a bit set of converged branches.
// It is used to skip already finished destinations when the input replays
// uncommitted events after a process restart.
type barrierStore struct {
	path string

	mu      sync.Mutex
	entries map[uint64]map[int64]uint64
	ring    []entryKey // insertion order of entries, lazily compacted
	total   int
	dirty   bool

	shouldStop atomic.Bool
}

func newBarrierStore(path string) *barrierStore {
	s := &barrierStore{
		path:    path,
		entries: make(map[uint64]map[int64]uint64),
	}
	s.load()
	return s
}

func (s *barrierStore) load() {
	o := offset.NewOffset(s.path)
	o.Callback = loadSaverFn(s.loadFrom)
	if err := o.Load(); err != nil {
		// The file is only a performance optimization (missing progress just
		// causes a redelivery); move an unreadable file aside instead of
		// blocking the startup.
		logger.Errorf("can't load output offsets file %q: %s, it will be ignored", s.path, err.Error())
		_ = os.Rename(s.path, s.path+".corrupt")
	}
}

func (s *barrierStore) loadFrom(r io.Reader) error {
	b, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	if len(b) == 0 {
		return nil
	}
	var stored storedBranches
	if err := yaml.Unmarshal(b, &stored); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	for _, e := range stored.Entries {
		if e.Mask == 0 {
			continue
		}
		m := s.entries[e.SourceID]
		if m == nil {
			m = make(map[int64]uint64)
			s.entries[e.SourceID] = m
		}
		if _, exists := m[e.Offset]; !exists {
			s.total++
			s.ring = append(s.ring, entryKey{e.SourceID, e.Offset})
		}
		m[e.Offset] = e.Mask
	}
	return nil
}

// converged returns the converged branch set of the event (0 if unknown).
func (s *barrierStore) converged(sourceID uint64, off int64) uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.entries[sourceID][off]
}

// record marks one branch of the event as converged.
func (s *barrierStore) record(sourceID uint64, off int64, idx int) {
	s.mu.Lock()
	m := s.entries[sourceID]
	if m == nil {
		m = make(map[int64]uint64)
		s.entries[sourceID] = m
	}
	if _, exists := m[off]; !exists {
		s.total++
		s.ring = append(s.ring, entryKey{sourceID, off})
	}
	m[off] |= uint64(1) << idx
	s.dirty = true
	s.evictLocked()
	s.mu.Unlock()
}

// deleteEntry removes the progress record of a fully completed event.
func (s *barrierStore) deleteEntry(sourceID uint64, off int64) {
	s.mu.Lock()
	if offsets := s.entries[sourceID]; offsets != nil {
		if _, exists := offsets[off]; exists {
			delete(offsets, off)
			s.total--
			s.dirty = true
			if len(offsets) == 0 {
				delete(s.entries, sourceID)
			}
		}
	}
	s.mu.Unlock()
}

// evictLocked bounds the table size: fully completed events are removed by the
// router, so remaining entries represent unfinished events; eviction here is a
// last-resort guard and only causes a possible redelivery (at-least-once).
func (s *barrierStore) evictLocked() {
	for s.total > maxBarrierEntries {
		var key entryKey
		found := false
		for len(s.ring) > 0 {
			key = s.ring[0]
			s.ring = s.ring[1:]
			if _, ok := s.entries[key.sourceID][key.offset]; ok {
				found = true
				break
			}
		}
		if !found {
			return
		}
		delete(s.entries[key.sourceID], key.offset)
		if len(s.entries[key.sourceID]) == 0 {
			delete(s.entries, key.sourceID)
		}
		s.total--
		logger.Warnf("output offsets table exceeded %d entries, evicting stale entry source=%d offset=%d",
			maxBarrierEntries, key.sourceID, key.offset)
	}
}

func (s *barrierStore) snapshot() storedBranches {
	s.mu.Lock()
	stored := storedBranches{Entries: make([]storedBranch, 0, s.total)}
	for sourceID, offsets := range s.entries {
		for off, mask := range offsets {
			if mask == 0 {
				continue
			}
			stored.Entries = append(stored.Entries, storedBranch{
				SourceID: sourceID,
				Offset:   off,
				Mask:     mask,
			})
		}
	}
	s.dirty = false
	s.mu.Unlock()
	return stored
}

func (s *barrierStore) flush() error {
	stored := s.snapshot()
	b, err := yaml.Marshal(&stored)
	if err != nil {
		return fmt.Errorf("marshal output offsets: %w", err)
	}
	o := offset.NewOffset(s.path)
	o.Callback = saveSaverFn(b)
	return o.Save()
}

func (s *barrierStore) flushIfDirty() {
	s.mu.Lock()
	dirty := s.dirty
	s.mu.Unlock()
	if !dirty {
		return
	}
	if err := s.flush(); err != nil {
		logger.Errorf("can't save output offsets: %s", err.Error())
		s.mu.Lock()
		s.dirty = true
		s.mu.Unlock()
	}
}

func (s *barrierStore) start(interval time.Duration) {
	go func() {
		for {
			time.Sleep(interval)
			if s.shouldStop.Load() {
				return
			}
			s.flushIfDirty()
		}
	}()
}

// stop terminates the periodic flusher and performs a final flush.
func (s *barrierStore) stop() {
	s.shouldStop.Store(true)
	if err := s.flush(); err != nil {
		logger.Errorf("can't save output offsets: %s", err.Error())
	}
}

// loadSaverFn adapts a func to offset.LoadSaver.
type loadSaverFn func(io.Reader) error

func (f loadSaverFn) Load(r io.Reader) error { return f(r) }
func (f loadSaverFn) Save(io.Writer) error   { return nil }

type saveSaverFn []byte

func (f saveSaverFn) Save(w io.Writer) error {
	_, err := w.Write(f)
	return err
}
func (f saveSaverFn) Load(io.Reader) error { return nil }

// defaultOutputOffsetsFile returns the default barrier progress file path.
func defaultOutputOffsetsFile(pipelineName string) string {
	return filepath.Join(DefaultOutputOffsetsDir, pipelineName+".output_offsets.yaml")
}
