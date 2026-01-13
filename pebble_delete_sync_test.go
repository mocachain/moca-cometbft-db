//go:build pebbledb
// +build pebbledb

package db

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/stretchr/testify/require"
)

// Happy-path: DeleteSync should remove an existing key and return nil.
func TestPebbleDB_DeleteSync_Success(t *testing.T) {
	name := fmt.Sprintf("test_%x", randStr(12))
	dir := os.TempDir()
	db, err := NewDB(name, PebbleDBBackend, dir)
	require.NoError(t, err)
	defer cleanupDBDir(dir, name)

	key := []byte("k")
	require.NoError(t, db.Set(key, []byte("v")))
	require.NoError(t, db.DeleteSync(key))

	val, err := db.Get(key)
	require.NoError(t, err)
	require.Nil(t, val)
}

// Input validation path: DeleteSync should error on empty key.
func TestPebbleDB_DeleteSync_EmptyKey_Error(t *testing.T) {
	name := fmt.Sprintf("test_%x", randStr(12))
	dir := os.TempDir()
	db, err := NewDB(name, PebbleDBBackend, dir)
	require.NoError(t, err)
	defer cleanupDBDir(dir, name)

	err = db.DeleteSync([]byte{})
	require.Equal(t, errKeyEmpty, err)
}

// Error propagation: reopen the DB in read-only mode and ensure DeleteSync returns an error.
func TestPebbleDB_DeleteSync_ErrorPropagation_ReadOnly(t *testing.T) {
	name := fmt.Sprintf("test_%x", randStr(12))
	dir := os.TempDir()

	// First create a normal writable DB and seed a key, then close it.
	dbw, err := NewPebbleDB(name, dir)
	require.NoError(t, err)
	key := []byte("ro-key")
	require.NoError(t, dbw.Set(key, []byte("v")))
	require.NoError(t, dbw.Close())
	defer cleanupDBDir(dir, name)

	// Reopen the same DB path in read-only mode.
	roOpts := &pebble.Options{ReadOnly: true}
	// Ensure DB path exists
	_, statErr := os.Stat(filepath.Join(dir, name+".db"))
	require.NoError(t, statErr)
	dbro, err := NewPebbleDBWithOpts(name, dir, roOpts)
	require.NoError(t, err)
	// Attempt to delete in read-only mode should return an error, not nil.
	err = dbro.DeleteSync(key)
	require.Error(t, err)
	// Close read-only handle
	require.NoError(t, dbro.Close())
}
