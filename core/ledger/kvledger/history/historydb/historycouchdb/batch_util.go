package historycouchdb

import (
	"errors"
	"sync"

	"github.com/hyperledger/fabric/core/ledger/util/couchdb"
)

// batch is executed in a separate goroutine.
type batch interface {
	execute() error
}

// updateCommitter implements `batch` interface. Each batch commits the portion of updates within a namespace assigned to it
type updateCommitter struct {
	db      *couchdb.CouchDatabase
	updates *nsUpdates
}

// nsUpdates encloses multiple CouchDocs
type nsUpdates struct {
	// slice of CouchDoc
	docs []*couchdb.CouchDoc
}

// UpdateBatch encloses the details of multiple `updates`
type UpdateBatch struct {
	// dbName -- kv updates
	updates map[string]*nsUpdates
}

// NewUpdateBatch constructs an instance of a Batch
func NewUpdateBatch() *UpdateBatch {
	return &UpdateBatch{make(map[string]*nsUpdates)}
}

func newNsUpdates() *nsUpdates {
	return &nsUpdates{make([]*couchdb.CouchDoc, 0)}
}

// Put adds a kv pair
func (batch *UpdateBatch) Put(dbName string, jsonBytes []byte) {
	if jsonBytes == nil {
		panic("Nil value not allowed")
	}
	batch.update(dbName, jsonBytes)
}

// Update updates the batch with a latest entry for a namespace and a key
func (batch *UpdateBatch) update(dbName string, jsonBytes []byte) {
	nsUpdates := batch.getOrCreateNsUpdates(dbName)
	nsUpdates.docs = append(nsUpdates.docs, &couchdb.CouchDoc{JSONValue: jsonBytes, Attachments: nil})
	batch.updates[dbName] = nsUpdates
}

func (batch *UpdateBatch) getOrCreateNsUpdates(dbName string) *nsUpdates {
	nsUpdates := batch.updates[dbName]
	if nsUpdates == nil {
		nsUpdates = newNsUpdates()
		batch.updates[dbName] = nsUpdates
	}
	return nsUpdates
}

// GetUpdatedDbNames returns the names of the dbs that are updated
func (batch *UpdateBatch) GetUpdatedDbNames() []string {
	dbNames := make([]string, len(batch.updates))
	i := 0
	for dbName := range batch.updates {
		dbNames[i] = dbName
		i++
	}
	return dbNames
}

func (updateCommitter *updateCommitter) execute() error {
	batchUpdateResp, err := updateCommitter.db.BatchUpdateDocuments(updateCommitter.updates.docs)
	if err != nil {
		return err
	}

	for _, updateDoc := range batchUpdateResp {
		if !updateDoc.Ok {
			return errors.New("Batch update failed inside execute func of updateCommitter")
		}
	}

	return nil
}

// executeBatches executes each batch in a separate goroutine and returns error if
// any of the batches return error during its execution
func executeBatches(batches []batch) error {
	logger.Debugf("Executing batches = %s", batches)
	numBatches := len(batches)
	if numBatches == 0 {
		return nil
	}
	if numBatches == 1 {
		return batches[0].execute()
	}
	var batchWG sync.WaitGroup
	batchWG.Add(numBatches)
	errsChan := make(chan error, numBatches)
	defer close(errsChan)
	for _, b := range batches {
		go func(b batch) {
			defer batchWG.Done()
			if err := b.execute(); err != nil {
				errsChan <- err
				return
			}
		}(b)
	}
	batchWG.Wait()
	if len(errsChan) > 0 {
		return <-errsChan
	}
	return nil
}
