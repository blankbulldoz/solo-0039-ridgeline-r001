package volume

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"
)

// openImage opens a downloaded Snapshot file the same way the server opens
// RIDGE_DB: a fresh bbolt handle on the exact bytes that were delivered.
func openImage(t *testing.T, path string) *bolt.DB {
	t.Helper()
	db, err := bolt.Open(path, 0600, &bolt.Options{Timeout: 2 * time.Second})
	if err != nil {
		t.Fatalf("open downloaded image %q: %v", path, err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func imageSequence(t *testing.T, db *bolt.DB) uint64 {
	t.Helper()
	var seq uint64
	if err := db.View(func(tx *bolt.Tx) error {
		seq = Current(tx)
		return nil
	}); err != nil {
		t.Fatalf("read sequence from image: %v", err)
	}
	return seq
}

// writerCommit is one identifiable write activity, recorded with the wall-clock
// interval during which its bbolt update transaction was executing. The
// intervals are the evidence that downloads and writes truly overlap in time.
type writerCommit struct {
	Sequence   uint64
	StartedAt  time.Time
	Committed  time.Time
	PayloadKey string
}

// TestSnapshotSequenceMatchesFileUnderConcurrentWrites is the direct regression
// for the image header leading the file.
//
// Snapshot() used to read the sequence in a second read transaction after the
// tx.WriteTo transaction had closed. A commit landing in that gap made
// X-Ridge-Sequence advertise a sequence the downloaded bytes do not contain.
// The writer below keeps a commit in flight for every download so the gap
// almost always contains a commit against the old implementation.
func TestSnapshotSequenceMatchesFileUnderConcurrentWrites(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "live.db"))
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	// Seed enough payload so each snapshot copy takes a non-trivial amount of
	// time: this widens both the snapshot transaction and the old buggy gap
	// between its two transactions, giving concurrent commits room to land.
	root := db.Raw
	if err := root.Update(func(tx *bolt.Tx) error {
		rootBucket, err := tx.CreateBucketIfNotExists([]byte("regression-payload"))
		if err != nil {
			return err
		}
		chunk := make([]byte, 4096)
		for i := range chunk {
			chunk[i] = byte(i)
		}
		for i := 0; i < 64; i++ {
			if err := rootBucket.Put([]byte(fmt.Sprintf("seed-%04d", i)), chunk); err != nil {
				return err
			}
		}
		_, err = Next(tx)
		return err
	}); err != nil {
		t.Fatalf("seed db: %v", err)
	}

	const downloads = 40
	var (
		commitsMu sync.Mutex
		commits   []writerCommit
		stop      atomic.Bool
		overlaps  atomic.Int64
	)
	var wg sync.WaitGroup

	// Writer: one full update transaction per loop, each advancing the global
	// sequence by exactly one. Transactions run concurrently with snapshots.
	wg.Add(1)
	go func() {
		defer wg.Done()
		payload := make([]byte, 16*1024)
		for i := 0; !stop.Load(); i++ {
			key := fmt.Sprintf("write-%06d", i)
			started := time.Now()
			err := root.Update(func(tx *bolt.Tx) error {
				b := tx.Bucket([]byte("regression-payload"))
				if err := b.Put([]byte(key), payload); err != nil {
					return err
				}
				seq, err := Next(tx)
				if err != nil {
					return err
				}
				commitsMu.Lock()
				commits = append(commits, writerCommit{
					Sequence:   seq,
					StartedAt:  started,
					Committed:  time.Now(),
					PayloadKey: key,
				})
				commitsMu.Unlock()
				return nil
			})
			if err != nil {
				stop.Store(true)
				t.Errorf("writer update %d: %v", i, err)
				return
			}
		}
	}()

	headerAhead := 0
	for i := 0; i < downloads; i++ {
		dlStart := time.Now()
		image, err := db.Snapshot()
		dlEnd := time.Now()
		if err != nil {
			stop.Store(true)
			wg.Wait()
			t.Fatalf("snapshot %d: %v", i, err)
		}

		// The bytes must hash to the advertised digest.
		digest := sha256.Sum256(image.Data)
		if got := hex.EncodeToString(digest[:]); got != image.SHA256 {
			t.Fatalf("snapshot %d: sha256 mismatch header=%s bytes=%s", i, image.SHA256, got)
		}

		// Restore the exact delivered bytes independently and read the sequence
		// that is actually inside the image file.
		imagePath := filepath.Join(t.TempDir(), "restored-"+strconv.Itoa(i)+".db")
		if err := os.WriteFile(imagePath, image.Data, 0600); err != nil {
			t.Fatalf("write image %d: %v", i, err)
		}
		fileSeq := imageSequence(t, openImage(t, imagePath))

		// Core invariant under test: the header sequence is exactly the
		// sequence stored inside the file it was delivered with.
		if fileSeq != image.Sequence {
			headerAhead++
			t.Errorf("snapshot %d: X-Ridge-Sequence=%d but restored file sequence=%d (header leads file by %d)",
				i, image.Sequence, fileSeq, image.Sequence-fileSeq)
		}

		// Interleaving evidence: at least one writer transaction was alive at
		// some point during this download. This is what proves the test is not
		// merely running writes and downloads serially.
		commitsMu.Lock()
		for _, c := range commits {
			if c.StartedAt.Before(dlEnd) && c.Committed.After(dlStart) {
				overlaps.Add(1)
				break
			}
		}
		commitsMu.Unlock()
	}
	stop.Store(true)
	wg.Wait()

	if overlaps.Load() == 0 {
		t.Fatal("no write transaction overlapped any snapshot: test did not exercise write/export interleaving")
	}
	t.Logf("downloads=%d overlaps=%d header-ahead violations=%d total writer commits=%d",
		downloads, overlaps.Load(), headerAhead, len(commits))
	if headerAhead != 0 {
		t.Fatalf("%d/%d snapshots advertised sequences absent from their files", headerAhead, downloads)
	}
}

// TestSnapshotIsAnEarlierConsistentImage proves the other half of the
// contract: when writes genuinely advance after a snapshot begins, the
// response may legitimately describe an earlier, but still complete and
// internally consistent, image. The online database being ahead at export
// end must not be an error; the file must contain exactly its header
// sequence and no partial commits.
func TestSnapshotIsAnEarlierConsistentImage(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "live.db"))
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	root := db.Raw
	if err := root.Update(func(tx *bolt.Tx) error {
		if _, err := tx.CreateBucketIfNotExists([]byte("regression-payload")); err != nil {
			return err
		}
		chunk := make([]byte, 4096)
		for i := range chunk {
			chunk[i] = byte(i)
		}
		b := tx.Bucket([]byte("regression-payload"))
		for i := 0; i < 48; i++ {
			if err := b.Put([]byte(fmt.Sprintf("seed-%04d", i)), chunk); err != nil {
				return err
			}
		}
		_, err := Next(tx)
		return err
	}); err != nil {
		t.Fatalf("seed db: %v", err)
	}

	var (
		stop        atomic.Bool
		lastSeq     atomic.Uint64
		sawEarlier  bool
		overlaps    int
		mu          sync.Mutex
		commitTimes []time.Time
	)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		payload := make([]byte, 8192)
		for i := 0; !stop.Load(); i++ {
			err := root.Update(func(tx *bolt.Tx) error {
				if err := tx.Bucket([]byte("regression-payload")).
					Put([]byte(fmt.Sprintf("write-%06d", i)), payload); err != nil {
					return err
				}
				seq, err := Next(tx)
				if err != nil {
					return err
				}
				lastSeq.Store(seq)
				mu.Lock()
				commitTimes = append(commitTimes, time.Now())
				mu.Unlock()
				return nil
			})
			if err != nil {
				stop.Store(true)
				t.Errorf("writer update %d: %v", i, err)
				return
			}
		}
	}()

	const downloads = 30
	for i := 0; i < downloads; i++ {
		dlStart := time.Now()
		image, err := db.Snapshot()
		dlEnd := time.Now()
		if err != nil {
			stop.Store(true)
			wg.Wait()
			t.Fatalf("snapshot %d: %v", i, err)
		}

		imagePath := filepath.Join(t.TempDir(), "restored.db")
		if err := os.WriteFile(imagePath, image.Data, 0600); err != nil {
			t.Fatalf("write image %d: %v", i, err)
		}
		restored := openImage(t, imagePath)
		fileSeq := imageSequence(t, restored)

		if fileSeq != image.Sequence {
			t.Errorf("snapshot %d inconsistent: header=%d file=%d", i, image.Sequence, fileSeq)
		}

		// Every sequence up to and including the image's own sequence must be
		// present: walk the restored payload bucket and confirm it holds the
		// exact number of writer records the sequence claims. Sequence starts
		// at 1 from seeding; each writer commit adds one payload record and
		// one sequence increment.
		var writerRecords int
		if err := restored.View(func(tx *bolt.Tx) error {
			return tx.Bucket([]byte("regression-payload")).ForEach(func(k, _ []byte) error {
				if string(k[:6]) == "write-" {
					writerRecords++
				}
				return nil
			})
		}); err != nil {
			t.Fatalf("count restored records: %v", err)
		}
		// Image sequence = 1 (seed) + number of writer commits captured.
		if want := image.Sequence - 1; writerRecords != int(want) {
			t.Errorf("snapshot %d: file has %d writer records but header sequence implies %d",
				i, writerRecords, want)
		}

		// The live DB is allowed to be ahead of the completed image.
		liveNow := lastSeq.Load()
		if liveNow > image.Sequence {
			sawEarlier = true
		}
		mu.Lock()
		for _, at := range commitTimes {
			if at.After(dlStart) && at.Before(dlEnd) {
				overlaps++
				break
			}
		}
		mu.Unlock()
	}
	stop.Store(true)
	wg.Wait()

	if overlaps == 0 {
		t.Fatal("no commit landed during a download: interleaving was not demonstrated")
	}
	if !sawEarlier {
		t.Fatal("no image lagged the live database: the earlier-consistent-image path was not exercised")
	}
	t.Logf("downloads=%d overlaps=%d; images could lag the live DB while remaining internally consistent",
		downloads, overlaps)
}
