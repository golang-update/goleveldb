package leveldb

import (
	"os"
	"testing"

	"github.cbhq.net/cloud/goleveldb/leveldb/opt"
)

func TestReadOptionsFromEnv(t *testing.T) {
	tests := []struct {
		name     string
		env      map[string]string
		validate func(*testing.T, *opt.Options)
	}{
		{
			name: "block_size",
			env: map[string]string{
				"LDB_BLOCK_SIZE": "8192",
			},
			validate: func(t *testing.T, o *opt.Options) {
				if o.BlockSize != 8192 {
					t.Errorf("expected BlockSize=8192, got %d", o.BlockSize)
				}
			},
		},
		{
			name: "compaction_table_size_and_multiplier",
			env: map[string]string{
				"LDB_COMPACTION_TABLE_SIZE":            "4194304",
				"LDB_COMPACTION_TABLE_SIZE_MULTIPLIER": "2.0",
			},
			validate: func(t *testing.T, o *opt.Options) {
				if o.CompactionTableSize != 4194304 {
					t.Errorf("expected CompactionTableSize=4194304, got %d", o.CompactionTableSize)
				}
				if o.CompactionTableSizeMultiplier != 2.0 {
					t.Errorf("expected CompactionTableSizeMultiplier=2.0, got %f", o.CompactionTableSizeMultiplier)
				}
			},
		},
		{
			name: "compaction_total_size_auto_adjustment",
			env: map[string]string{
				"LDB_COMPACTION_TABLE_SIZE": "4194304",
			},
			validate: func(t *testing.T, o *opt.Options) {
				if o.CompactionTotalSize != 4194304*5 {
					t.Errorf("expected CompactionTotalSize=%d, got %d", 4194304*5, o.CompactionTotalSize)
				}
			},
		},
		{
			name: "disable_features",
			env: map[string]string{
				"LDB_DISABLE_COMPRESSION":             "1",
				"LDB_NO_SYNC":                         "true",
				"LDB_DISABLE_BUFFER_POOL":             "true",
				"LDB_DISABLE_BLOCK_CACHE":             "true",
				"LDB_DISABLE_COMPACTION_BACKOFF":      "true",
				"LDB_DISABLE_LARGE_BATCH_TRANSACTION": "true",
				"LDB_DISABLE_SEEKS_COMPACTION":        "true",
			},
			validate: func(t *testing.T, o *opt.Options) {
				if o.Compression != opt.NoCompression {
					t.Error("expected Compression=NoCompression")
				}
				if !o.NoSync {
					t.Error("expected NoSync=true")
				}
				if !o.DisableBufferPool {
					t.Error("expected DisableBufferPool=true")
				}
				if !o.DisableBlockCache {
					t.Error("expected DisableBlockCache=true")
				}
				if !o.DisableCompactionBackoff {
					t.Error("expected DisableCompactionBackoff=true")
				}
				if !o.DisableLargeBatchTransaction {
					t.Error("expected DisableLargeBatchTransaction=true")
				}
				if !o.DisableSeeksCompaction {
					t.Error("expected DisableSeeksCompaction=true")
				}
			},
		},
		{
			name: "compaction_and_write_triggers",
			env: map[string]string{
				"LDB_COMPACTION_L0_TRIGGER":          "8",
				"LDB_WRITE_L0_PAUSE_TRIGGER":         "24",
				"LDB_WRITE_L0_SLOWDOWN_TRIGGER":      "16",
				"LDB_COMPACTION_EXPAND_LIMIT_FACTOR": "50",
			},
			validate: func(t *testing.T, o *opt.Options) {
				if o.CompactionL0Trigger != 8 {
					t.Errorf("expected CompactionL0Trigger=8, got %d", o.CompactionL0Trigger)
				}
				if o.WriteL0PauseTrigger != 24 {
					t.Errorf("expected WriteL0PauseTrigger=24, got %d", o.WriteL0PauseTrigger)
				}
				if o.WriteL0SlowdownTrigger != 16 {
					t.Errorf("expected WriteL0SlowdownTrigger=16, got %d", o.WriteL0SlowdownTrigger)
				}
				if o.CompactionExpandLimitFactor != 50 {
					t.Errorf("expected CompactionExpandLimitFactor=50, got %d", o.CompactionExpandLimitFactor)
				}
			},
		},
		{
			name: "negative_cases",
			env: map[string]string{
				"LDB_BLOCK_SIZE":     "invalid",
				"LDB_NO_SYNC":        "maybe",
				"LDB_WRITE_BUFFER":   "0.1",
				"LDB_FILTER_BASE_LG": "not_a_number",
			},
			validate: func(t *testing.T, o *opt.Options) {
				if o.BlockSize != opt.DefaultBlockSize {
					t.Errorf("invalid BlockSize should not change default, got %d", o.BlockSize)
				}
				if o.NoSync != false {
					t.Error("invalid NoSync should remain false")
				}
				if o.WriteBuffer != opt.DefaultWriteBuffer {
					t.Errorf("negative WriteBuffer should not change default, got %d", o.WriteBuffer)
				}
				if o.FilterBaseLg != opt.DefaultFilterBaseLg {
					t.Errorf("invalid FilterBaseLg should not change default, got %d", o.FilterBaseLg)
				}
			},
		},
		{
			name: "cache_settings",
			env: map[string]string{
				"LDB_BLOCK_CACHE_CAPACITY":      "16777216",
				"LDB_OPEN_FILES_CACHE_CAPACITY": "1000",
				"LDB_BLOCK_CACHE_EVICT_REMOVED": "true",
				"LDB_ITERATOR_SAMPLING_RATE":    "2097152",
			},
			validate: func(t *testing.T, o *opt.Options) {
				if o.BlockCacheCapacity != 16777216 {
					t.Errorf("expected BlockCacheCapacity=16777216, got %d", o.BlockCacheCapacity)
				}
				if o.OpenFilesCacheCapacity != 1000 {
					t.Errorf("expected OpenFilesCacheCapacity=1000, got %d", o.OpenFilesCacheCapacity)
				}
				if !o.BlockCacheEvictRemoved {
					t.Error("expected BlockCacheEvictRemoved=true")
				}
				if o.IteratorSamplingRate != 2097152 {
					t.Errorf("expected IteratorSamplingRate=2097152, got %d", o.IteratorSamplingRate)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Clear environment
			os.Clearenv()

			// Set test environment
			for k, v := range tt.env {
				os.Setenv(k, v)
			}

			// Create options and read from env
			o := &opt.Options{}
			o.BlockSize = opt.DefaultBlockSize
			o.WriteBuffer = opt.DefaultWriteBuffer
			o.FilterBaseLg = opt.DefaultFilterBaseLg

			readOptionsFromEnv(o)

			// Validate
			tt.validate(t, o)
		})
	}
}
