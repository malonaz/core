package pbreflection

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"
)

var (
	memCache   = make(map[string]*cacheEntry)
	memCacheMu sync.RWMutex
)

// schema_cache.go - change cacheEntry
type cacheEntry struct {
	schema    *Schema
	expiresAt time.Time
}

type schemaData struct {
	FileDescriptors []*descriptorpb.FileDescriptorProto
	ServiceSet      []string
}

func (d *schemaData) MarshalJSON() ([]byte, error) {
	type wire struct {
		FileDescriptors [][]byte `json:"file_descriptors"`
		ServiceSet      []string `json:"service_set"`
	}
	w := wire{ServiceSet: d.ServiceSet}
	for _, fd := range d.FileDescriptors {
		b, err := proto.Marshal(fd)
		if err != nil {
			return nil, err
		}
		w.FileDescriptors = append(w.FileDescriptors, b)
	}
	return json.Marshal(w)
}

func (d *schemaData) UnmarshalJSON(data []byte) error {
	type wire struct {
		FileDescriptors [][]byte `json:"file_descriptors"`
		ServiceSet      []string `json:"service_set"`
	}
	var w wire
	if err := json.Unmarshal(data, &w); err != nil {
		return err
	}
	d.ServiceSet = w.ServiceSet
	d.FileDescriptors = make([]*descriptorpb.FileDescriptorProto, len(w.FileDescriptors))
	for i, b := range w.FileDescriptors {
		fd := new(descriptorpb.FileDescriptorProto)
		if err := proto.Unmarshal(b, fd); err != nil {
			return err
		}
		d.FileDescriptors[i] = fd
	}
	return nil
}

func hashKey(key string) string {
	hash := sha256.Sum256([]byte(key))
	return hex.EncodeToString(hash[:])
}

func WithMemCache(key string, ttl time.Duration) ResolveSchemaOption {
	return func(o *resolveSchemaOptions) {
		o.cacheKey = hashKey(key)
		o.cacheTTL = ttl
	}
}

func WithDiskCache(key, dir string, ttl time.Duration) ResolveSchemaOption {
	return func(o *resolveSchemaOptions) {
		o.cacheKey = hashKey(key)
		o.cacheTTL = ttl
		o.cacheDir = dir
	}
}

func loadFromFileCache(opts *resolveSchemaOptions) *schemaData {
	path := filepath.Join(opts.cacheDir, opts.cacheKey+".json")
	info, err := os.Stat(path)
	if err != nil || time.Since(info.ModTime()) > opts.cacheTTL {
		return nil
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	var data schemaData
	if err := json.Unmarshal(b, &data); err != nil {
		return nil
	}
	return &data
}

// schema_cache.go - update loadFromMemCache signature
func loadFromMemCache(opts *resolveSchemaOptions) *Schema {
	memCacheMu.RLock()
	defer memCacheMu.RUnlock()
	if entry, ok := memCache[opts.cacheKey]; ok && time.Now().Before(entry.expiresAt) {
		return entry.schema
	}
	return nil
}

// schema_cache.go - update saveToMemCache signature
func saveToMemCache(schema *Schema, opts *resolveSchemaOptions) {
	memCacheMu.Lock()
	defer memCacheMu.Unlock()
	memCache[opts.cacheKey] = &cacheEntry{schema: schema, expiresAt: time.Now().Add(opts.cacheTTL)}
}

func saveToFileCache(data *schemaData, opts *resolveSchemaOptions) {
	if err := os.MkdirAll(opts.cacheDir, 0755); err != nil {
		return
	}
	b, err := json.Marshal(data)
	if err != nil {
		return
	}
	_ = os.WriteFile(filepath.Join(opts.cacheDir, opts.cacheKey+".json"), b, 0644)
}
