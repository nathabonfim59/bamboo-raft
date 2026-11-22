package memstore_test

import (
	"testing"

	"github.com/nathabonfim59/bamboo-raft/internal/objstore/memstore"
	"github.com/nathabonfim59/bamboo-raft/internal/objstore/objstoretest"
)

func TestConformance(t *testing.T) {
	objstoretest.Run(t, memstore.New(), objstoretest.Options{})
}
