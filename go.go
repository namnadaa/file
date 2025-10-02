package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

const (
	defaultDownloadDir       = "data/downloads"
	defaultTaskStateDir      = "data/tasks"
	defaultConcurrency       = 4
	defaultServerAddr        = ":8080"
	checkpointEveryNChanges  = 1
	progressFlushIntervalSec = 2
	clientTimeout            = 60
)

type TaskStatus string

const (
	TaskQueued    TaskStatus = "queued"
	TaskRunning   TaskStatus = "running"
	TaskPaused    TaskStatus = "paused"
	TaskCompleted TaskStatus = "completed"
	TaskFailed    TaskStatus = "failed"
	TaskCanceled  TaskStatus = "canceled"
)

type ItemStatus string

const (
	ItemQueued    ItemStatus = "queued"
	ItemRunning   ItemStatus = "running"
	ItemCompleted ItemStatus = "completed"
	ItemFailed    ItemStatus = "failed"
	ItemCanceled  ItemStatus = "canceled"
	ItemPaused    ItemStatus = "paused"
)

type DownloadItem struct {
	URL        string     `json:"url"`
	FileName   string     `json:"file_name"`
	Status     ItemStatus `json:"status"`
	Size       int64      `json:"size"`
	Downloaded int64      `json:"downloaded"`
	Error      string     `json:"error,omitempty"`
}

type Task struct {
	ID        string         `json:"id"`
	CreatedAt time.Time      `json:"created_at"`
	Status    TaskStatus     `json:"status"`
	Dir       string         `json:"dir"`
	Items     []DownloadItem `json:"items"`
	Notes     string         `json:"notes,omitempty"`
}

type job struct {
	taskID string
	index  int
}

type TaskManager struct {
	mu      sync.RWMutex
	tasks   map[string]*Task
	queue   chan job
	workers int
	paused  bool

	downloadRoot  string
	taskStateRoot string

	httpClient *http.Client
	wg         sync.WaitGroup

	changesSinceCheckpoint int
	shuttingDown           bool
}

func newTaskManager(downloadRoot, taskStateRoot string, workers int) *TaskManager {
	if workers <= 0 {
		workers = defaultConcurrency
	}
	_ = os.MkdirAll(downloadRoot, 0o755)
	_ = os.MkdirAll(taskStateRoot, 0o755)

	return &TaskManager{
		tasks:         make(map[string]*Task),
		queue:         make(chan job, 1024),
		workers:       workers,
		downloadRoot:  downloadRoot,
		taskStateRoot: taskStateRoot,
		httpClient: &http.Client{
			Timeout: clientTimeout * time.Second,
		},
	}
}

func (tm *TaskManager) Start(ctx context.Context) {
	tm.restoreTasks()

	for i := 0; i < tm.workers; i++ {
		tm.wg.Add(1)
		go tm.worker(ctx, i+1)
	}
}

func (tm *TaskManager) StopAndWait() {
	tm.mu.Lock()
	tm.paused = true
	tm.shuttingDown = true
	tm.mu.Unlock()

	tm.wg.Wait()

	tm.checkpointAll()
}

func (tm *TaskManager) worker(ctx context.Context, id int) {
	defer tm.wg.Done()

	ticker := time.NewTicker(time.Duration(progressFlushIntervalSec) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			tm.mu.Lock()
			tm.checkpointAll()
			tm.mu.Unlock()
		case j := <-tm.queue:
			if j.taskID == "" {
				continue
			}
			tm.mu.RLock()
			if tm.paused {
				tm.mu.RUnlock()
				time.Sleep(200 * time.Millisecond)
				tm.enqueueFront(j)
				continue
			}
			task := tm.tasks[j.taskID]
			tm.mu.RUnlock()
			if task == nil {
				continue
			}

			tm.processItem(ctx, task, j.index)
		}
	}
}

func (tm *TaskManager) processItem(ctx context.Context, t *Task, idx int) {
	tm.mu.Lock()
	if t.Status == TaskQueued {
		t.Status = TaskRunning
		tm.markChangedLocked()
	}
	if idx < 0 || idx >= len(t.Items) {
		tm.mu.Unlock()
		return
	}
	item := &t.Items[idx]
	if item.Status == ItemCompleted {
		tm.mu.Unlock()
		return
	}
	item.Status = ItemRunning
	item.Error = ""
	tm.markChangedLocked()
	tm.mu.Unlock()

	err := tm.downloadWithResume(ctx, t, idx)

	tm.mu.Lock()
	defer tm.mu.Unlock()

	if err != nil {
		item.Status = ItemFailed
		item.Error = err.Error()
	} else {
		item.Status = ItemCompleted
		item.Error = ""
	}

	allDone := true
	anyFailed := false
	for i := range t.Items {
		switch t.Items[i].Status {
		case ItemFailed:
			anyFailed = true
			allDone = false
		case ItemCompleted:
		default:
			allDone = false
		}
	}
	if allDone {
		if anyFailed {
			t.Status = TaskFailed
		} else {
			t.Status = TaskCompleted
		}
	} else if t.Status != TaskPaused && t.Status != TaskCanceled {
		t.Status = TaskRunning
	}
	tm.markChangedLocked()
}

func (tm *TaskManager) downloadWithResume(ctx context.Context, t *Task, idx int) error {
	item := &t.Items[idx]
	targetDir := filepath.Join(tm.downloadRoot, safeJoin(t.Dir))
	if err := os.MkdirAll(targetDir, 0o755); err != nil {
		return fmt.Errorf("mkdir: %w", err)
	}

	baseName := item.FileName
	if baseName == "" {
		baseName = deriveFileName(item.URL)
	}
	baseName = sanitizeFileName(baseName)
	partPath := filepath.Join(targetDir, baseName+".part")
	finalPath := filepath.Join(targetDir, baseName)

	if fi, err := os.Stat(finalPath); err == nil && fi.Size() > 0 {
		item.Downloaded = fi.Size()
		item.Size = fi.Size()
		return nil
	}

	var existing int64 = 0
	if fi, err := os.Stat(partPath); err == nil {
		existing = fi.Size()
		item.Downloaded = existing
	} else {
		item.Downloaded = 0
	}

	contentLen, acceptsRange := tm.headInfo(ctx, item.URL)
	if contentLen > 0 {
		item.Size = contentLen
	} else if item.Size == 0 {
		item.Size = -1
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, item.URL, nil)
	if err != nil {
		return err
	}
	if existing > 0 && acceptsRange {
		req.Header.Set("Range", "bytes="+strconv.FormatInt(existing, 10)+"-")
	}

	resp, err := tm.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		return fmt.Errorf("http %d", resp.StatusCode)
	}

	f, err := os.OpenFile(partPath, os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()

	if existing > 0 {
		if _, err := f.Seek(existing, io.SeekStart); err != nil {
			return err
		}
	}

	buf := make([]byte, 1<<20)
	lastFlush := time.Now()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		n, rerr := resp.Body.Read(buf)
		if n > 0 {
			if _, werr := f.Write(buf[:n]); werr != nil {
				return werr
			}
			item.Downloaded += int64(n)
			if time.Since(lastFlush) > time.Duration(progressFlushIntervalSec)*time.Second {
				_ = f.Sync()
				tm.mu.Lock()
				tm.markChangedLocked()
				tm.mu.Unlock()
				lastFlush = time.Now()
			}
		}
		if rerr != nil {
			if errors.Is(rerr, io.EOF) {
				break
			}
			return rerr
		}
	}

	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(partPath, finalPath); err != nil {
		return err
	}
	return nil
}

func (tm *TaskManager) headInfo(ctx context.Context, rawURL string) (size int64, acceptsRange bool) {
	size = -1
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, rawURL, nil)
	if err != nil {
		return
	}
	resp, err := tm.httpClient.Do(req)
	if err != nil {
		return
	}
	defer resp.Body.Close()

	if cl := resp.Header.Get("Content-Length"); cl != "" {
		if v, e := strconv.ParseInt(cl, 10, 64); e == nil {
			size = v
		}
	}
	if strings.Contains(strings.ToLower(resp.Header.Get("Accept-Ranges")), "bytes") {
		acceptsRange = true
	}
	return
}

func (tm *TaskManager) enqueueFront(j job) {
	select {
	case tm.queue <- j:
	default:
		go func() { tm.queue <- j }()
	}
}

func (tm *TaskManager) AddTask(urls []string, dir, notes string) (*Task, error) {
	if len(urls) == 0 {
		return nil, fmt.Errorf("empty urls")
	}
	id := newID()
	items := make([]DownloadItem, 0, len(urls))
	for _, u := range urls {
		items = append(items, DownloadItem{
			URL:      u,
			FileName: "",
			Status:   ItemQueued,
			Size:     -1,
		})
	}
	t := &Task{
		ID:        id,
		CreatedAt: time.Now(),
		Status:    TaskQueued,
		Dir:       dir,
		Items:     items,
		Notes:     notes,
	}

	tm.mu.Lock()
	tm.tasks[id] = t
	tm.markChangedLocked()
	tm.mu.Unlock()

	for i := range t.Items {
		tm.queue <- job{taskID: id, index: i}
	}
	return t, nil
}

func (tm *TaskManager) PauseProcessing() {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	tm.paused = true
	for _, t := range tm.tasks {
		if t.Status == TaskRunning {
			t.Status = TaskPaused
		}
		for i := range t.Items {
			if t.Items[i].Status == ItemRunning {
				t.Items[i].Status = ItemPaused
			}
		}
	}
	tm.markChangedLocked()
}

func (tm *TaskManager) ResumeProcessing() {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	tm.paused = false
	for _, t := range tm.tasks {
		if t.Status == TaskPaused {
			t.Status = TaskRunning
		}
		for i := range t.Items {
			switch t.Items[i].Status {
			case ItemPaused, ItemFailed, ItemQueued:
				go func(taskID string, idx int) { tm.queue <- job{taskID: taskID, index: idx} }(t.ID, i)
				if t.Items[i].Status == ItemPaused {
					t.Items[i].Status = ItemQueued
				}
			}
		}
	}
	tm.markChangedLocked()
}

func (tm *TaskManager) CancelTask(id string) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	t, ok := tm.tasks[id]
	if !ok {
		return fmt.Errorf("task not found")
	}
	t.Status = TaskCanceled
	for i := range t.Items {
		if t.Items[i].Status == ItemRunning || t.Items[i].Status == ItemQueued || t.Items[i].Status == ItemPaused {
			t.Items[i].Status = ItemCanceled
		}
	}
	tm.markChangedLocked()
	return nil
}

func (tm *TaskManager) GetTask(id string) (*Task, bool) {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	t, ok := tm.tasks[id]
	if !ok {
		return nil, false
	}
	cp := *t
	cp.Items = append([]DownloadItem(nil), t.Items...)
	return &cp, true
}

func (tm *TaskManager) ListTasks() []*Task {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	out := make([]*Task, 0, len(tm.tasks))
	for _, t := range tm.tasks {
		cp := *t
		cp.Items = append([]DownloadItem(nil), t.Items...)
		out = append(out, &cp)
	}
	return out
}

func (tm *TaskManager) restoreTasks() {
	entries, err := os.ReadDir(tm.taskStateRoot)
	if err != nil {
		return
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		b, err := os.ReadFile(filepath.Join(tm.taskStateRoot, e.Name()))
		if err != nil {
			continue
		}
		var t Task
		if err := json.Unmarshal(b, &t); err != nil {
			continue
		}
		if t.Status == TaskRunning {
			t.Status = TaskPaused
		}
		for i := range t.Items {
			switch t.Items[i].Status {
			case ItemRunning:
				t.Items[i].Status = ItemPaused
			}
		}
		tm.tasks[t.ID] = &t
		for i := range t.Items {
			switch t.Items[i].Status {
			case ItemQueued, ItemPaused, ItemFailed:
				tm.queue <- job{taskID: t.ID, index: i}
			}
		}
	}
}

func (tm *TaskManager) markChangedLocked() {
	tm.changesSinceCheckpoint++
	if tm.changesSinceCheckpoint >= checkpointEveryNChanges {
		tm.checkpointAll()
		tm.changesSinceCheckpoint = 0
	}
}

func (tm *TaskManager) checkpointAll() {
	for _, t := range tm.tasks {
		_ = tm.saveTask(t)
	}
}

func (tm *TaskManager) saveTask(t *Task) error {
	tmp := filepath.Join(tm.taskStateRoot, t.ID+".json.tmp")
	dst := filepath.Join(tm.taskStateRoot, t.ID+".json")
	b, err := json.MarshalIndent(t, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, dst)
}

type createTaskReq struct {
	URLs  []string `json:"urls"`
	Dir   string   `json:"dir,omitempty"`
	Notes string   `json:"notes,omitempty"`
}
type createTaskResp struct {
	ID string `json:"id"`
}

func (tm *TaskManager) handleCreateTask(w http.ResponseWriter, r *http.Request) {
	var req createTaskReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad json", http.StatusBadRequest)
		return
	}
	t, err := tm.AddTask(req.URLs, req.Dir, req.Notes)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	writeJSON(w, createTaskResp{ID: t.ID})
}

func (tm *TaskManager) handleGetTask(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, "/tasks/")
	t, ok := tm.GetTask(id)
	if !ok {
		http.NotFound(w, r)
		return
	}
	writeJSON(w, t)
}

func (tm *TaskManager) handleListTasks(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, tm.ListTasks())
}

func (tm *TaskManager) handlePause(w http.ResponseWriter, r *http.Request) {
	tm.PauseProcessing()
	writeJSON(w, map[string]string{"status": "paused"})
}

func (tm *TaskManager) handleResume(w http.ResponseWriter, r *http.Request) {
	tm.ResumeProcessing()
	writeJSON(w, map[string]string{"status": "resumed"})
}

func (tm *TaskManager) handleCancelTask(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, "/tasks/")
	if id == "" {
		http.Error(w, "missing id", http.StatusBadRequest)
		return
	}
	if err := tm.CancelTask(id); err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	writeJSON(w, map[string]string{"status": "canceled"})
}

func (tm *TaskManager) handleHealthz(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, map[string]string{"ok": "true"})
}

func main() {
	addr := getenvDefault("ADDR", defaultServerAddr)
	workers := getenvIntDefault("DOWNLOAD_CONCURRENCY", defaultConcurrency)
	downloadRoot := getenvDefault("DOWNLOAD_DIR", defaultDownloadDir)
	taskStateRoot := getenvDefault("TASK_STATE_DIR", defaultTaskStateDir)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tm := newTaskManager(downloadRoot, taskStateRoot, workers)
	tm.Start(ctx)

	mux := http.NewServeMux()
	mux.HandleFunc("POST /tasks", tm.handleCreateTask)
	mux.HandleFunc("GET /tasks", tm.handleListTasks)
	mux.HandleFunc("GET /tasks/", tm.handleGetTask)
	mux.HandleFunc("POST /tasks/{id}/cancel", tm.handleCancelTask)
	mux.HandleFunc("/tasks/", func(w http.ResponseWriter, r *http.Request) {

		if r.Method == "POST" && strings.HasSuffix(r.URL.Path, "/cancel") {
			id := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/tasks/"), "/cancel")
			if id == "" {
				http.Error(w, "missing id", http.StatusBadRequest)
				return
			}
			if err := tm.CancelTask(id); err != nil {
				http.Error(w, err.Error(), http.StatusNotFound)
				return
			}
			writeJSON(w, map[string]string{"status": "canceled"})
			return
		}
		http.NotFound(w, r)
	})
	mux.HandleFunc("POST /maintenance/pause", tm.handlePause)
	mux.HandleFunc("POST /maintenance/resume", tm.handleResume)
	mux.HandleFunc("GET /healthz", tm.handleHealthz)

	srv := &http.Server{
		Addr:    addr,
		Handler: logRequests(mux),
	}

	idleConnsClosed := make(chan struct{})
	go func() {
		defer close(idleConnsClosed)
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)
		<-sigCh

		tm.PauseProcessing()

		shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)

		cancelMain := cancel
		_ = cancelMain

		tm.StopAndWait()
	}()

	fmt.Printf("listening on %s\n", addr)
	if err := srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) && err != nil {
		fmt.Printf("server error: %v\n", err)
	}

	<-idleConnsClosed
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
}

func newID() string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return fmt.Sprintf("%d-%s", time.Now().UnixNano(), hex.EncodeToString(b[:]))
}

func sanitizeFileName(name string) string {
	name = strings.TrimSpace(name)
	name = strings.ReplaceAll(name, "..", "_")
	name = strings.ReplaceAll(name, "/", "_")
	name = strings.ReplaceAll(name, "\\", "_")
	if name == "" {
		name = "file"
	}
	return name
}

func safeJoin(p string) string {
	p = filepath.Clean("/" + p)
	return strings.TrimPrefix(p, "/")
}

func deriveFileName(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return "file"
	}
	base := path.Base(u.Path)
	if base == "" || base == "/" || base == "." {
		return "file"
	}
	return base
}

func getenvDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
func getenvIntDefault(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func logRequests(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		next.ServeHTTP(w, r)
		fmt.Printf("%s %s %s\n", r.Method, r.URL.Path, time.Since(start))
	})
}
