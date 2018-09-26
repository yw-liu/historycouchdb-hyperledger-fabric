package historycouchdb

import (
	"errors"

	"github.com/hyperledger/fabric/core/ledger/util/couchdb"
)

// nsFlusher implements `batch` interface and a batch executes the function `couchdb.EnsureFullCommit()` for the given namespace
type nsFlusher struct {
	db *couchdb.CouchDatabase
}

func (historyDB *HistoryDB) ensureFullCommit(dbs []*couchdb.CouchDatabase) error {
	var flushers []batch
	for _, db := range dbs {
		flushers = append(flushers, &nsFlusher{db})
	}
	return executeBatches(flushers)
}

func (f *nsFlusher) execute() error {
	dbResponse, err := f.db.EnsureFullCommit()
	if err != nil || dbResponse.Ok != true {
		logger.Errorf("Failed to perform full commit, dbName=%s\n", f.db.DBName)
		return errors.New("Failed to perform full commit")
	}
	return nil
}
