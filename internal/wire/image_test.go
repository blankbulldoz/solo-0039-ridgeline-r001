package wire

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"ridgeline/internal/engine"
	"ridgeline/internal/model"
	"ridgeline/internal/volume"

	bolt "go.etcd.io/bbolt"
)

// recordedCall is one identifiable write or rollup activity with the exact
// wall-clock interval during which its HTTP request was in flight. Interval
// overlap with a download is how the test proves requests truly interleaved
// instead of happening to complete serially.
type recordedCall struct {
	Sequence  uint64
	Value     float64
	StartedAt time.Time
	EndedAt   time.Time
}

type downloadedImage struct {
	Index       int
	HeaderSeq   uint64
	HeaderSHA   string
	Bytes       []byte
	StartedAt   time.Time
	EndedAt     time.Time
	RestoredSeq uint64
}

func jsonBody(t *testing.T, v any) []byte {
	t.Helper()
	data, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return data
}

func postJSON(t *testing.T, client *http.Client, url string, v any, wantStatus int) []byte {
	t.Helper()
	resp, err := client.Post(url, "application/json", bytes.NewReader(jsonBody(t, v)))
	if err != nil {
		t.Fatalf("POST %s: %v", url, err)
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != wantStatus {
		t.Fatalf("POST %s: status %d want %d: %s", url, resp.StatusCode, wantStatus, data)
	}
	return data
}

func putJSON(t *testing.T, client *http.Client, url string, v any, wantStatus int) []byte {
	t.Helper()
	req, err := http.NewRequest(http.MethodPut, url, bytes.NewReader(jsonBody(t, v)))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("PUT %s: %v", url, err)
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != wantStatus {
		t.Fatalf("PUT %s: status %d want %d: %s", url, resp.StatusCode, wantStatus, data)
	}
	return data
}

func globalSequence(t *testing.T, e *engine.Engine) uint64 {
	t.Helper()
	var seq uint64
	if err := e.DB.Raw.View(func(tx *bolt.Tx) error {
		seq = volume.Current(tx)
		return nil
	}); err != nil {
		t.Fatalf("read global sequence: %v", err)
	}
	return seq
}

// TestImageHeaderMatchesRestoredFileUnderConcurrentWrites reproduces the
// reported backup-inspection incident over real HTTP:
//
//   - an "archive" space holds sealed history so image copies have real work;
//   - a "live" space receives repeated corrections of one timestamp while a
//     count rollup is refreshed again and again (rollup refreshes advance the
//     global sequence without changing the live source sequence);
//   - GET /ridge/v1/image is downloaded repeatedly in parallel.
//
// For every download the test keeps the original headers, the raw bytes and
// the identifiable write activity, then restores the exact bytes through a
// second engine instance and proves header, checksum and file describe one and
// the same consistent backup.
func TestImageHeaderMatchesRestoredFileUnderConcurrentWrites(t *testing.T) {
	scratch := t.TempDir()
	e, err := engine.Open(filepath.Join(scratch, "source.db"))
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	t.Cleanup(func() { _ = e.Close() })

	server := httptest.NewServer(Handler(e))
	t.Cleanup(server.Close)
	client := &http.Client{Timeout: 30 * time.Second}
	base := server.URL + "/ridge/v1"

	namespacePolicy := func(name string) model.Namespace {
		return model.Namespace{
			Name: name, RetentionMS: 1 << 40, LatenessMS: 1 << 40, FutureMS: 1 << 40,
			MaxSeries: 10000, MaxWrite: 5000, MaxScan: 200000,
		}
	}
	postJSON(t, client, base+"/spaces", namespacePolicy("archive"), http.StatusOK)
	postJSON(t, client, base+"/spaces", namespacePolicy("live"), http.StatusOK)

	// Build sealed history in archive: four batches, then seal, so the image
	// copy traverses real compressed block payloads rather than an empty file.
	const batches, perBatch = 4, 200
	for batch := 0; batch < batches; batch++ {
		points := make([]model.Observation, 0, perBatch)
		for i := 0; i < perBatch; i++ {
			points = append(points, model.Observation{
				Labels: model.Labels{"__name__": "archived_temp", "rack": fmt.Sprintf("rack-%d", i%8)},
				Time:   int64(batch*1000 + i),
				Value:  float64(batch*perBatch + i),
			})
		}
		postJSON(t, client, base+"/spaces/archive/points",
			model.WriteRequest{Now: 1_000_000, Points: points}, http.StatusOK)
	}
	postJSON(t, client, base+"/spaces/archive/seal", struct{}{}, http.StatusOK)

	// Seed live with the measurement that will be corrected repeatedly.
	const fixedTimestamp int64 = 5000
	liveLabels := model.Labels{"__name__": "live_temp", "rack": "a"}
	firstWrite := postJSON(t, client, base+"/spaces/live/points", model.WriteRequest{
		Now: 1_000_000, Points: []model.Observation{{Labels: liveLabels, Time: fixedTimestamp, Value: 0}},
	}, http.StatusOK)
	var firstReceipt model.WriteReceipt
	if err := json.Unmarshal(firstWrite, &firstReceipt); err != nil {
		t.Fatal(err)
	}

	rollupQuery := model.Query{
		Start: 0, End: 10_000_000, Step: 100_000, Origin: 0,
		GroupBy: []string{"rack"}, Function: "count",
	}
	putJSON(t, client, base+"/spaces/live/rollups/live_count", rollupQuery, http.StatusOK)

	var (
		mu         sync.Mutex
		writes     []recordedCall
		rollups    []recordedCall
		bgErrors   []string
		stop       atomic.Bool
		writeValue atomic.Uint64
	)
	recordError := func(format string, args ...any) {
		mu.Lock()
		bgErrors = append(bgErrors, fmt.Sprintf(format, args...))
		mu.Unlock()
	}

	var wg sync.WaitGroup

	// Writer: repeated corrective writes of the same live timestamp. Every
	// request advances the global sequence by one and returns its receipt.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; !stop.Load(); i++ {
			value := float64(i + 1)
			payload := jsonBody(t, model.WriteRequest{Now: 1_000_000, Points: []model.Observation{
				{Labels: liveLabels, Time: fixedTimestamp, Value: value},
			}})
			started := time.Now()
			resp, err := client.Post(base+"/spaces/live/points", "application/json", bytes.NewReader(payload))
			ended := time.Now()
			if err != nil {
				if !stop.Load() {
					recordError("live write: %v", err)
				}
				return
			}
			data, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				recordError("live write status %d: %s", resp.StatusCode, data)
				return
			}
			var receipt model.WriteReceipt
			if err := json.Unmarshal(data, &receipt); err != nil {
				recordError("decode receipt: %v", err)
				return
			}
			writeValue.Store(uint64(value))
			mu.Lock()
			writes = append(writes, recordedCall{
				Sequence: receipt.Sequence, Value: value, StartedAt: started, EndedAt: ended,
			})
			mu.Unlock()
		}
	}()

	// Rollup refresher: refreshing materialization also takes a global
	// sequence, even though no live sample changes.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; !stop.Load(); i++ {
			payload := jsonBody(t, rollupQuery)
			started := time.Now()
			req, _ := http.NewRequest(http.MethodPut, base+"/spaces/live/rollups/live_count",
				bytes.NewReader(payload))
			req.Header.Set("Content-Type", "application/json")
			resp, err := client.Do(req)
			ended := time.Now()
			if err != nil {
				if !stop.Load() {
					recordError("rollup refresh: %v", err)
				}
				return
			}
			data, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				recordError("rollup status %d: %s", resp.StatusCode, data)
				return
			}
			var mat model.Materialization
			if err := json.Unmarshal(data, &mat); err != nil {
				recordError("decode rollup: %v", err)
				return
			}
			mu.Lock()
			rollups = append(rollups, recordedCall{
				Sequence: mat.Result.Sequence, StartedAt: started, EndedAt: ended,
			})
			mu.Unlock()
		}
	}()

	// Downloader: repeatedly fetch the image, preserving headers and raw bytes.
	const downloads = 50
	images := make([]downloadedImage, 0, downloads)
	for i := 0; i < downloads; i++ {
		started := time.Now()
		resp, err := client.Get(base + "/image")
		if err != nil {
			stop.Store(true)
			wg.Wait()
			t.Fatalf("GET image %d: %v", i, err)
		}
		data, _ := io.ReadAll(resp.Body)
		ended := time.Now()
		if resp.StatusCode != http.StatusOK {
			resp.Body.Close()
			stop.Store(true)
			wg.Wait()
			t.Fatalf("image %d status %d", i, resp.StatusCode)
		}
		headerSeq, err := strconv.ParseUint(resp.Header.Get("X-Ridge-Sequence"), 10, 64)
		if err != nil {
			t.Fatalf("image %d sequence header: %v", i, err)
		}
		headerSHA := resp.Header.Get("X-Ridge-SHA256")
		resp.Body.Close()

		images = append(images, downloadedImage{
			Index: i, HeaderSeq: headerSeq, HeaderSHA: headerSHA,
			Bytes: data, StartedAt: started, EndedAt: ended,
		})
	}
	stop.Store(true)
	wg.Wait()

	mu.Lock()
	bgCopy := append([]string(nil), bgErrors...)
	writesCopy := append([]recordedCall(nil), writes...)
	rollupsCopy := append([]recordedCall(nil), rollups...)
	mu.Unlock()
	if len(bgCopy) > 0 {
		t.Fatalf("background traffic errors: %v", bgCopy)
	}
	if len(writesCopy) == 0 || len(rollupsCopy) == 0 {
		t.Fatalf("missing concurrent activity: writes=%d rollups=%d", len(writesCopy), len(rollupsCopy))
	}

	// Deterministic demonstration of the rollup-only sequence advance:
	// one more refresh must raise the global sequence while the live space's
	// source sequence stays put.
	beforeSeq := globalSequence(t, e)
	liveBefore, err := e.Namespace("live")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.Materialize("live", "live_count", rollupQuery); err != nil {
		t.Fatalf("final rollup refresh: %v", err)
	}
	afterSeq := globalSequence(t, e)
	liveAfter, err := e.Namespace("live")
	if err != nil {
		t.Fatal(err)
	}
	if afterSeq != beforeSeq+1 {
		t.Fatalf("rollup refresh advanced global sequence by %d, want 1", afterSeq-beforeSeq)
	}
	if liveAfter.LastSequence != liveBefore.LastSequence {
		t.Fatalf("rollup refresh changed live source sequence: before=%d after=%d",
			liveBefore.LastSequence, liveAfter.LastSequence)
	}
	t.Logf("rollup refresh advances the global sequence (%d -> %d) but not live source sequence (%d)",
		beforeSeq, afterSeq, liveAfter.LastSequence)
	finalSeq := afterSeq

	overlaps := func(calls []recordedCall, img downloadedImage) bool {
		for _, c := range calls {
			if c.StartedAt.Before(img.EndedAt) && c.EndedAt.After(img.StartedAt) {
				return true
			}
		}
		return false
	}

	var writeOverlaps, rollupOverlaps, laggedImages int
	for idx := range images {
		img := images[idx]

		// 1. Checksum must match the exact delivered bytes.
		digest := sha256.Sum256(img.Bytes)
		if got := hex.EncodeToString(digest[:]); got != img.HeaderSHA {
			t.Fatalf("image %d: X-Ridge-SHA256=%s does not match bytes (%s)", img.Index, img.HeaderSHA, got)
		}

		// 2. Restore through a separate engine instance on a copy of the bytes.
		restorePath := filepath.Join(t.TempDir(), fmt.Sprintf("restored-%d.db", img.Index))
		if err := os.WriteFile(restorePath, img.Bytes, 0600); err != nil {
			t.Fatal(err)
		}
		restored, err := engine.Open(restorePath)
		if err != nil {
			t.Fatalf("image %d cannot be opened as a database: %v", img.Index, err)
		}
		restoredSeq := globalSequence(t, restored)
		images[idx].RestoredSeq = restoredSeq

		// 3. The reported bug: header sequence must equal the sequence that is
		// physically stored in the file delivered alongside it.
		if restoredSeq != img.HeaderSeq {
			t.Errorf("image %d: X-Ridge-Sequence=%d but restored file sequence=%d (header leads file by %d)",
				img.Index, img.HeaderSeq, restoredSeq, img.HeaderSeq-restoredSeq)
		}

		// 4. The restored file must hold all namespaces, including sealed
		// archive history that was never touched during the concurrent phase.
		archRange, err := restored.Range("archive", model.Query{
			Start: model.MinTime, End: model.MaxTime + 1,
		})
		if err != nil {
			t.Fatalf("image %d: archive missing from restored file: %v", img.Index, err)
		}
		var archivedPoints int
		for _, s := range archRange.Series {
			archivedPoints += len(s.Points)
		}
		if archivedPoints != batches*perBatch {
			t.Errorf("image %d: restored archive has %d points, want %d",
				img.Index, archivedPoints, batches*perBatch)
		}

		// 5. Point-level provenance: every write whose receipt sequence is at or
		// below the image header sequence must actually be recoverable from the
		// file. The highest such correction determines the expected live value.
		var expectedValue float64
		var expectedSeq uint64
		found := false
		for _, w := range writesCopy {
			if w.Sequence <= img.HeaderSeq && (!found || w.Sequence > expectedSeq) {
				expectedValue, expectedSeq, found = w.Value, w.Sequence, true
			}
		}
		liveRange, err := restored.Range("live", model.Query{Start: 0, End: 10_000_000})
		if err != nil {
			t.Fatalf("image %d: live missing from restored file: %v", img.Index, err)
		}
		if found {
			gotValue, gotSeq := locatePoint(t, liveRange, fixedTimestamp)
			if gotSeq != expectedSeq || gotValue != expectedValue {
				t.Errorf("image %d (seq %d): restored live point = (value=%v seq=%d), want (value=%v seq=%d)",
					img.Index, img.HeaderSeq, gotValue, gotSeq, expectedValue, expectedSeq)
			}
		}
		if err := restored.Close(); err != nil {
			t.Fatal(err)
		}

		if overlaps(writesCopy, img) {
			writeOverlaps++
		}
		if overlaps(rollupsCopy, img) {
			rollupOverlaps++
		}
		if img.HeaderSeq < finalSeq {
			laggedImages++
		}
	}

	// Interleaving evidence: without these assertions the traffic could have
	// run entirely serially and the test would prove nothing about concurrency.
	if writeOverlaps == 0 {
		t.Fatal("no corrective write overlapped a download in wall-clock time: interleaving not demonstrated")
	}
	if rollupOverlaps == 0 {
		t.Fatal("no rollup refresh overlapped a download in wall-clock time: interleaving not demonstrated")
	}
	if laggedImages == 0 {
		t.Fatal("no image described an earlier complete image while the live DB kept advancing")
	}
	t.Logf("downloads=%d writes=%d (overlapping %d) rollups=%d (overlapping %d) lagged-but-consistent images=%d finalSeq=%d",
		len(images), len(writesCopy), writeOverlaps, len(rollupsCopy), rollupOverlaps, laggedImages, finalSeq)
}

func locatePoint(t *testing.T, r model.RangeResult, timestamp int64) (float64, uint64) {
	t.Helper()
	for _, sp := range r.Series {
		for _, p := range sp.Points {
			if p.Time == timestamp {
				return p.Value, p.Sequence
			}
		}
	}
	t.Fatalf("timestamp %d not found in restored range", timestamp)
	return 0, 0
}
