package jsonnet

import (
	"context"
	"encoding/json"
	"os"
	"path"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/google/go-jsonnet"
	"github.com/pkg/errors"

	"go/logging"
)

var (
	log = logging.NewLogger()
	// Upon hitting a loading error, we retry up to five times using a backoff
	// of `tries * backoff`, after which we panic to crash the server.
	reloadRetryBackoff = 100 * time.Millisecond
	reloadMaxRetries   = 5
	// Upon detecting a file change. How long to wait for other file changes.
	// This avoids attempting to load a config with the partially changed files.
	waitForOtherFileChanges = 200 * time.Millisecond
)

// config is the interface that every configuration object must implement.
type config interface {
	// Validates the config *recursively*.
	ValidateAll() error
}

// ParseFn is syntactic sugar for a config parsing method, which must be passed to the ReloadableConfig constructor.
type ParseFn func(content []byte) (config, error)

// Config wraps a user-supplied config, enriching it with a timestamp.
type Config struct {
	Version         string
	UpdateTimestamp time.Time
	Payload         any
}

// ReloadableConfig implements a jsonnet reloadable config.
type ReloadableConfig struct {
	filename string
	vm       *jsonnet.VM

	config                  *Config
	configMutex             sync.RWMutex
	reloadEventChannels     []chan struct{}
	reloadSignal            chan struct{}
	updateSignal            chan struct{}
	reloadMutex             sync.Mutex
	parseFn                 ParseFn
	observeEvaluationTimeFN func(float64)
	withoutFileWatcher      bool
	fileToWatch             string
	preReloadFN             func() error

	importPaths     map[string]struct{}
	fsnotifyWatcher *fsnotify.Watcher
	// Cache of non-file-based imports.
	// Updateable via the `SetData(key,value)` method.
	data map[string]jsonnet.Contents
	// Cache of file-based imports.
	// Automatically updated via fsnotify.
	filepathToContents map[string]jsonnet.Contents
}

// NewReloadableConfig instantiates and returns a new manager. Data can be used to inject content into the jsonnet.
func NewReloadableConfig(filename string, data map[string]string, parseFn ParseFn) (*ReloadableConfig, error) {
	fsnotifyWatcher, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, errors.Wrap(err, "instantiating fsnotify watcher")
	}
	dataContents := map[string]jsonnet.Contents{}
	for k, v := range data {
		dataContents[k] = jsonnet.MakeContents(v)
	}
	manager := &ReloadableConfig{
		filename:           filename,
		vm:                 jsonnet.MakeVM(),
		parseFn:            parseFn,
		fsnotifyWatcher:    fsnotifyWatcher,
		reloadSignal:       make(chan struct{}, 1), // Non-blocking writes.
		updateSignal:       make(chan struct{}, 1), // Non-blocking writes.
		data:               dataContents,
		filepathToContents: map[string]jsonnet.Contents{},
	}
	manager.vm.Importer(manager)
	return manager, nil
}

// MustNewReloadableConfig calls NewReloadableConfig and panics on error.
func MustNewReloadableConfig(filename string, data map[string]string, parseFn ParseFn) *ReloadableConfig {
	manager, err := NewReloadableConfig(filename, data, parseFn)
	if err != nil {
		log.Panic(err)
	}
	return manager
}

// ReloadEvents allows a user to subscribe to reload events.
func (m *ReloadableConfig) ReloadEvents() <-chan struct{} {
	ch := make(chan struct{}, 1)
	m.reloadEventChannels = append(m.reloadEventChannels, ch)
	return ch
}

// WithFileWatcher allows a user to override which file we watch for changes. The user passed in a file that will be executed prior to reloading the config.
// This can be useful for configuration tarballs.
func (m *ReloadableConfig) WithFileWatcher(filepath string, fn func() error) *ReloadableConfig {
	m.fileToWatch = filepath
	m.preReloadFN = fn
	return m
}

// WithoutFileWatcher deactivates file watching.
func (m *ReloadableConfig) WithoutFileWatcher() *ReloadableConfig {
	m.withoutFileWatcher = true
	return m
}

// SetObserveEvaluationTimeFN allows a caller to pass some metric observation function.
// ReloadableConfig will call this function with the seconds in float64 spent evaluating the file everytime
// it is evaluated.
func (m *ReloadableConfig) SetObserveEvaluationTimeFN(fn func(float64)) {
	m.observeEvaluationTimeFN = fn
}

// Close closes this manager gracefully.
func (m *ReloadableConfig) Close() {
	m.fsnotifyWatcher.Close()
	log.Info("config manager closed")
}

// GetConfig returns the latest config.
func (m *ReloadableConfig) GetConfig() *Config {
	m.configMutex.RLock()
	config := m.config
	m.configMutex.RUnlock()
	return config
}

func (m *ReloadableConfig) publishReloadSignal() {
	select {
	case m.reloadSignal <- struct{}{}:
	default:
		// If there is no space on the channel, a signal already exists there.
		// An additional signal would be redundant.
	}
}

func (m *ReloadableConfig) publishUpdateSignal() {
	select {
	case m.updateSignal <- struct{}{}:
	default:
		// If there is no space on the channel, a signal already exists there.
		// An additional signal would be redundant.
	}
}

// reloadConfig checks for a reload signal periodically.
func (m *ReloadableConfig) reloadConfig(ctx context.Context) {
	retries := 0
	for {
		select {
		case <-ctx.Done():
			log.Info("Exiting config reload routine")
			return
		case <-m.reloadSignal:
			time.Sleep(time.Duration(retries) * reloadRetryBackoff)
			if err := m.load(); err != nil {
				retries++
				log.Errorf("reloading config: %v", err)
				if retries == reloadMaxRetries {
					log.Panicf("Exceeded max reload retries")
				}
				m.publishReloadSignal()
				continue
			}
			retries = 0
			log.Debug("Reloaded config")
			m.publishUpdateSignal()
		}
	}
}

// UpdateData allows a caller to update some data field. Upon updating the data,
// this method will publish a signal to reload the config.
func (m *ReloadableConfig) UpdateData(key, value string) {
	m.reloadMutex.Lock()
	m.data[key] = jsonnet.MakeContents(value)
	m.reloadMutex.Unlock()
	m.publishReloadSignal()
}

// updateCacheOnFileChange acts everytime a config file is touched.
// It does two thing each file change: 1) deletes this file from the cache so that it will be re-fetched.
// 2) publishs a signal to reload (if there isn't already one).
func (m *ReloadableConfig) updateCacheOnFileChange(ctx context.Context) {
	// Whenever we see 'one' event
	drainEvents := func() {
		maxWait := time.After(waitForOtherFileChanges)
		for {
			select {
			case event, ok := <-m.fsnotifyWatcher.Events:
				if !ok {
					// `Close` must have been called.
					return
				}
				log.Infof("draining: file change: %s", event.String())
			case err, ok := <-m.fsnotifyWatcher.Errors:
				if !ok {
					// `Close` must have been called.
					return
				}
				log.Errorf("draining: fsnotify error: %v", err)
			case <-maxWait:
				// We've either seen all events or waited as long as we're willing to wait.
				return
			}
		}
	}

	for {
		select {
		case <-ctx.Done():
			log.Infof("Exiting fsnotify routine")
			return
		case event, ok := <-m.fsnotifyWatcher.Events:
			if !ok {
				// `Close` must have been called.
				log.Infof("Exiting fsnotify routine")
				return
			}
			// Note that we don't bother inspecting the actual event type.
			// We could do something more efficient, but that's probably overkill.
			log.Infof("found file change: %s", event.String())
			drainEvents()
			// Clear the entire cache and publish a reload signal.
			m.reloadMutex.Lock()
			m.filepathToContents = map[string]jsonnet.Contents{}
			m.reloadMutex.Unlock()
			log.Infof("triggering reload due to file change")
			m.publishReloadSignal()
		case err, ok := <-m.fsnotifyWatcher.Errors:
			if !ok {
				// `Close` must have been called.
				log.Infof("Exiting fsnotify routine")
				return
			}
			log.Errorf("fsnotify error: %v", err)
			drainEvents()
			// Clear the entire cache and publish a reload signal.
			m.reloadMutex.Lock()
			m.filepathToContents = map[string]jsonnet.Contents{}
			m.reloadMutex.Unlock()
			m.publishReloadSignal()
		}
	}
}

// Start a go routine that watches the filesystem for filechanges.
// Synchronously load the config to make sure we're in an ok state.
func (m *ReloadableConfig) Start(ctx context.Context) error {
	if err := m.load(); err != nil {
		return err
	}
	go m.reloadConfig(ctx)
	go m.updateCacheOnFileChange(ctx)
	return nil
}

// MustStart calls `Start` and panics on error.
func (m *ReloadableConfig) MustStart(ctx context.Context) {
	if err := m.Start(ctx); err != nil {
		log.Panic(err)
	}
}

// load the config synchronously.
func (m *ReloadableConfig) load() error {
	m.reloadMutex.Lock()
	defer m.reloadMutex.Unlock()
	if m.preReloadFN != nil {
		if err := m.preReloadFN(); err != nil {
			return errors.Wrap(err, "executing pre-reload function")
		}
		if err := m.fsnotifyWatcher.Add(m.fileToWatch); err != nil {
			return errors.Wrap(err, "could not watch file to watch")
		}
		log.Infof("watching %s for change", m.fileToWatch)
	}
	start := time.Now()
	if m.observeEvaluationTimeFN != nil {
		defer m.observeEvaluationTimeFN(time.Since(start).Seconds())
	}
	m.vm.Importer(m) // This flushes the internal vm cache, such that we can pick up file changes.
	// Reset import paths.
	m.importPaths = map[string]struct{}{}
	updateTimestamp := time.Now().UTC()
	content, err := m.vm.EvaluateFile(m.filename)
	if err != nil {
		return errors.Wrap(err, "evaluate file")
	}
	// Unmarshal into a generic interface, in order to mimick the python lib's to_dict functionality.
	// This is actually important as this step corrects jsonnet evaluation of floats such as 0.04 as 0.040000000001.
	var generic any
	if err := json.Unmarshal([]byte(content), &generic); err != nil {
		return errors.Wrap(err, "unmarshaling to generic interface")
	}
	sanitizedContent, err := json.Marshal(&generic)
	if err != nil {
		return errors.Wrap(err, "marshaling generic interface")
	}
	config, err := m.parseFn([]byte(sanitizedContent))
	if err != nil {
		return errors.Wrap(err, "parsing content")
	}
	if err := config.ValidateAll(); err != nil {
		return errors.Wrap(err, "failed validation")
	}

	// Look for version file.
	version := ""
	dir := path.Dir(m.filename)
	for {
		filepath := path.Join(dir, "version")
		bytes, err := os.ReadFile(filepath)
		if err != nil {
			newDir := path.Dir(dir)
			if dir == newDir {
				return errors.Errorf("could not find version file")
			}
			dir = newDir
			continue
		}
		version = string(bytes)
		break
	}
	if dir == "" {
		return errors.Errorf("could not find version file")
	}
	m.config = &Config{UpdateTimestamp: updateTimestamp, Payload: config, Version: version}
	for _, reloadEventChannel := range m.reloadEventChannels {
		select {
		case reloadEventChannel <- struct{}{}:
		default:
		}
	}
	return nil
}

// Import implements the jsonnet importer interface.
func (m *ReloadableConfig) Import(importedFrom, importedPath string) (jsonnet.Contents, string, error) {
	// Check non-file based imports.
	contents, ok := m.data[importedPath]
	if ok {
		return contents, importedPath, nil
	}

	// Compute the absolute path of this file.
	absolutePath := importedPath
	if !path.IsAbs(absolutePath) {
		if importedFrom == "" {
			return jsonnet.Contents{}, "", errors.Errorf("entrypoint %s must be specified using absolute path", importedPath)
		}
		dir := path.Dir(importedFrom)
		absolutePath = path.Join(dir, importedPath)
	}

	// Check cache.
	contents, ok = m.filepathToContents[absolutePath]
	if ok {
		return contents, absolutePath, nil
	}

	// Check filesystem.
	bytes, err := os.ReadFile(absolutePath)
	if err != nil {
		return jsonnet.Contents{}, "", err
	}
	contents = jsonnet.MakeContents(string(bytes))

	// Attempt to watch file. If it does not fail, cache the file.
	if m.fileToWatch == "" && !m.withoutFileWatcher {
		if err := m.fsnotifyWatcher.Add(absolutePath); err != nil {
			log.Errorf("failed to watch file %s: %v", absolutePath, err)
		} else {
			if _, ok := m.filepathToContents[absolutePath]; !ok {
				log.Infof("watching file %s for change", absolutePath)
				m.filepathToContents[absolutePath] = contents
			}
		}
	} else {
		// Cache the content - we don't need to watch these files for change anyway.
		if _, ok := m.filepathToContents[absolutePath]; !ok {
			m.filepathToContents[absolutePath] = contents
		}
	}
	return contents, absolutePath, nil
}
