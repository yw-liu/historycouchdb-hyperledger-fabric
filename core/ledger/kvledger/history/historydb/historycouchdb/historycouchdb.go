package historycouchdb

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/common/ledger/blkstorage"
	"github.com/hyperledger/fabric/core/common/ccprovider"
	"github.com/hyperledger/fabric/core/ledger"
	"github.com/hyperledger/fabric/core/ledger/kvledger/history/historydb"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/rwsetutil"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/version"
	"github.com/hyperledger/fabric/core/ledger/ledgerconfig"
	"github.com/hyperledger/fabric/core/ledger/util"
	"github.com/hyperledger/fabric/core/ledger/util/couchdb"
	"github.com/hyperledger/fabric/protos/common"
	putils "github.com/hyperledger/fabric/protos/utils"
)

var logger = flogging.MustGetLogger("historycouchdb")

// Savepoint docid (key) for couchdb
const savepointDocID = "historycouchdb_savepoint"

// historyDbNamePrefix db name prefix
const historyDbNamePrefix = "history_"

// querySkip defaulted to 0
const querySkip = 0

// HistoryDBProvider - implements interface HistoryDBProvider
type HistoryDBProvider struct {
	couchInstance *couchdb.CouchInstance
	databases     map[string]*HistoryDB
	mux           sync.Mutex
}

//HistoryDB implements HistoryDB interface
type HistoryDB struct {
	couchInstance *couchdb.CouchInstance
	metadataDB    *couchdb.CouchDatabase            // A database per channel to store metadata such as savepoint.
	chainName     string                            // The name of the chain/channel.
	namespaceDBs  map[string]*couchdb.CouchDatabase // One database per deployed chaincode.
	mux           sync.RWMutex
}

// NewHistoryDBProvider instantiates HistoryDBProvider
func NewHistoryDBProvider() *HistoryDBProvider {
	logger.Debugf("constructing CouchDB HistoryDBProvider")
	couchDBDef := couchdb.GetCouchDBDefinition()
	couchInstance, err := couchdb.CreateCouchInstance(couchDBDef.URL, couchDBDef.Username, couchDBDef.Password,
		couchDBDef.MaxRetries, couchDBDef.MaxRetriesOnStartup, couchDBDef.RequestTimeout)
	if err != nil {
		return nil
	}
	return &HistoryDBProvider{couchInstance, make(map[string]*HistoryDB), sync.Mutex{}}
}

// GetDBHandle gets the handle to a named database (here dbName == channelName ?)
func (provider *HistoryDBProvider) GetDBHandle(dbName string) (historydb.HistoryDB, error) {
	provider.mux.Lock()
	defer provider.mux.Unlock()

	// hdbName := fmt.Sprintf("%s%s", historyDbNamePrefix, dbName)
	hdbName := addHistoryDbNamePrefix(dbName)

	logger.Debugf("GetDBHandle: hdbName=[%s]", hdbName)
	hdb := provider.databases[hdbName]
	if hdb == nil {
		var err error
		hdb, err = newHistoryDB(provider.couchInstance, dbName)
		if err != nil {
			return nil, err
		}
		provider.databases[hdbName] = hdb
	}
	return hdb, nil
}

// Close closes the underlying db instance
func (provider *HistoryDBProvider) Close() {
	// No close needed on Couch
}

// Close implements method in HistoryDB interface
func (historyDB *HistoryDB) Close() {
	// no need to close db since a shared couch instance is used
}

// Open implements method in VersionedDB interface
func (historyDB *HistoryDB) Open() error {
	// no need to open db since a shared couch instance is used
	return nil
}

// GetDBType return db type
func (historyDB *HistoryDB) GetDBType() string {
	return "couchdb"
}

// ProcessIndexesForChaincodeDeploy creates indexes for a specified namespace
func (historyDB *HistoryDB) ProcessIndexesForChaincodeDeploy(namespace string, fileEntries []*ccprovider.TarFileEntry) error {

	dbName := historyDB.constructHistoryDbNameFromNs(namespace)
	db, err := historyDB.getDBHandleByDbName(dbName)

	if err != nil {
		return err
	}

	for _, fileEntry := range fileEntries {
		indexData := fileEntry.FileContent
		filename := fileEntry.FileHeader.Name
		_, err = db.CreateIndex(string(indexData))
		if err != nil {
			return fmt.Errorf("error during creation of index from file=[%s] for chain=[%s]. Error=%s",
				filename, namespace, err)
		}
	}

	return nil
}

// newHistoryDB constructs an instance of HistoryDB
func newHistoryDB(couchInstance *couchdb.CouchInstance, dbName string) (*HistoryDB, error) {
	// CreateCouchDatabase creates a CouchDB database object, as well as the underlying database if it does not exist
	chainName := dbName

	hdbName := addHistoryDbNamePrefix(couchdb.ConstructMetadataDBName(dbName))

	metadataDB, err := couchdb.CreateCouchDatabase(couchInstance, hdbName)
	if err != nil {
		return nil, err
	}
	namespaceDBMap := make(map[string]*couchdb.CouchDatabase)

	return &HistoryDB{couchInstance: couchInstance, metadataDB: metadataDB, chainName: chainName, namespaceDBs: namespaceDBMap,
		mux: sync.RWMutex{}}, nil
}

// Commit implements method in HistoryDB interface
func (historyDB *HistoryDB) Commit(block *common.Block) error {

	blockNo := block.Header.Number
	//Set the starting tranNo to 0
	var tranNo uint64

	dbBatch := NewUpdateBatch()

	logger.Debugf("Channel [%s]: Updating history database for blockNo [%v] with [%d] transactions",
		historyDB.chainName, blockNo, len(block.Data.Data))

	// Get the invalidation byte array for the block
	txsFilter := util.TxValidationFlags(block.Metadata.Metadata[common.BlockMetadataIndex_TRANSACTIONS_FILTER])

	ifHistoryRichQueryEnabled := ledgerconfig.IfHistoryRichQueryEnabled()
	logger.Debugf("[Commit] ifHistoryRichQueryEnabled=%v", ifHistoryRichQueryEnabled)

	var indexFieldsMap map[string][]string
	if ifHistoryRichQueryEnabled && indexFieldsMap == nil {
		indexFieldsMap = make(map[string][]string)
	}

	// write each tran's write set to history db
	for _, envBytes := range block.Data.Data {

		// If the tran is marked as invalid, skip it
		if txsFilter.IsInvalid(int(tranNo)) {
			logger.Debugf("Channel [%s]: Skipping history write for invalid transaction number %d",
				historyDB.chainName, tranNo)
			tranNo++
			continue
		}

		env, err := putils.GetEnvelopeFromBlock(envBytes)
		if err != nil {
			return err
		}

		payload, err := putils.GetPayload(env)
		if err != nil {
			return err
		}

		chdr, err := putils.UnmarshalChannelHeader(payload.Header.ChannelHeader)
		if err != nil {
			return err
		}

		if common.HeaderType(chdr.Type) == common.HeaderType_ENDORSER_TRANSACTION {

			// extract actions from the envelope message
			respPayload, err := putils.GetActionFromEnvelope(envBytes)
			if err != nil {
				return err
			}

			//preparation for extracting RWSet from transaction
			txRWSet := &rwsetutil.TxRwSet{}

			// Get the Result from the Action and then Unmarshal
			// it into a TxReadWriteSet using custom unmarshalling
			if err = txRWSet.FromProtoBytes(respPayload.Results); err != nil {
				return err
			}
			// for each transaction, loop through the namespaces and writesets
			// and add a history record for each write
			for _, nsRWSet := range txRWSet.NsRwSets {
				ns := nsRWSet.NameSpace

				for _, kvWrite := range nsRWSet.KvRwSet.Writes {
					writeKey := kvWrite.Key

					// logger.Debugf("Commit func: ns=%s, key=%s, blockNo=%v, tranNo=%v, historyDbName=%s\n", ns, writeKey, blockNo, tranNo, historyDB.constructHistoryDbNameFromNs(ns))
					logger.Debugf("[Commit] ns=%s, key=%s, isDel=%v, value=%v", ns, writeKey, kvWrite.IsDelete, string(kvWrite.Value))

					jMap := make(jsonMap)
					jMap[idField] = fmt.Sprintf("%s~%020v", writeKey, time.Now().UnixNano())
					jMap[blockNumField] = blockNo
					jMap[txNumField] = tranNo

					if ifHistoryRichQueryEnabled {
						// read index fields
						hdbName := historyDB.constructHistoryDbNameFromNs(ns)
						curIndexFields := indexFieldsMap[hdbName]
						if curIndexFields == nil {
							hdb, err := historyDB.getDBHandleByDbName(hdbName)
							if err == nil {
								curIndexFields, err = hdb.ListIndexFields()
								if err == nil {
									indexFieldsMap[hdbName] = curIndexFields
								}
							}
						}

						// read keys from value bytes
						var valueMap map[string]interface{}
						err := json.Unmarshal(kvWrite.Value, &valueMap)
						if err == nil {
							for k, v := range valueMap {
								for _, indexField := range curIndexFields {
									if k == indexField {
										jMap[k] = v
										break
									}
								}
							}
						}
					}

					jsonBytes, _ := json.Marshal(jMap)
					dbBatch.Put(historyDB.constructHistoryDbNameFromNs(ns), jsonBytes)
				}
			}

		} else {
			logger.Debugf("Skipping transaction [%d] since it is not an endorsement transaction\n", tranNo)
		}
		tranNo++
	}

	// add savepoint for recovery purpose
	height := version.NewHeight(blockNo, tranNo)

	// do batch update
	err := historyDB.applyUpdates(dbBatch, height)

	if err != nil {
		logger.Errorf("Channel [%s], blockNo [%v], failed to applyUpdates: %s\n", historyDB.chainName, blockNo, err.Error())
		return err
	}

	logger.Debugf("[Commit] indexFieldsMap=%v", indexFieldsMap)
	logger.Debugf("Channel [%s]: Updates committed to history database for blockNo [%v]", historyDB.chainName, blockNo)
	return nil
}

// GetLastSavepoint implements method in HistoryDB interface
func (historyDB *HistoryDB) GetLastSavepoint() (*version.Height, error) {
	var err error
	couchDoc, _, err := historyDB.metadataDB.ReadDoc(savepointDocID)
	if err != nil {
		logger.Errorf("Failed to read savepoint data %s\n", err.Error())
		return nil, err
	}
	// ReadDoc() not found (404) will result in nil response, in these cases return height nil
	if couchDoc == nil || couchDoc.JSONValue == nil {
		return nil, nil
	}
	return decodeSavepoint(couchDoc)
}

// ShouldRecover implements method in interface kvledger.Recoverer
func (historyDB *HistoryDB) ShouldRecover(lastAvailableBlock uint64) (bool, uint64, error) {
	if !ledgerconfig.IsHistoryDBEnabled() {
		return false, 0, nil
	}
	savepoint, err := historyDB.GetLastSavepoint()
	if err != nil {
		return false, 0, err
	}
	if savepoint == nil {
		return true, 0, nil
	}
	return savepoint.BlockNum != lastAvailableBlock, savepoint.BlockNum + 1, nil
}

// CommitLostBlock implements method in interface kvledger.Recoverer
func (historyDB *HistoryDB) CommitLostBlock(blockAndPvtdata *ledger.BlockAndPvtData) error {
	block := blockAndPvtdata.Block
	if err := historyDB.Commit(block); err != nil {
		return err
	}
	return nil
}

// NewHistoryQueryExecutor implements method in HistoryDB interface
func (historyDB *HistoryDB) NewHistoryQueryExecutor(blockStore blkstorage.BlockStore) (ledger.HistoryQueryExecutor, error) {
	// return &LevelHistoryDBQueryExecutor{historyDB, blockStore}, nil

	// Todo:
	return &CouchHistoryDBQueryExecutor{historyDB, blockStore}, nil
}

// applyUpdates implements method
func (historyDB *HistoryDB) applyUpdates(updates *UpdateBatch, height *version.Height) error {
	// TODO a note about https://jira.hyperledger.org/browse/FAB-8622
	// the function `Apply update can be split into three functions. Each carrying out one of the following three stages`.
	// The write lock is needed only for the stage 2.

	// stage 1 - PrepareForUpdates - db transforms the given batch in the form of underlying db
	// and keep it in memory
	var updateBatches []batch
	var err error
	if updateBatches, err = historyDB.buildCommitters(updates); err != nil {
		return err
	}
	// stage 2 - ApplyUpdates push the changes to the DB
	if err = executeBatches(updateBatches); err != nil {
		return err
	}

	// Stgae 3 - PostUpdateProcessing - flush and record savepoint.
	dbNames := updates.GetUpdatedDbNames()
	// Record a savepoint at a given height
	if err = historyDB.ensureFullCommitAndRecordSavepoint(height, dbNames); err != nil {
		logger.Errorf("Error during recordSavepoint: %s\n", err.Error())
		return err
	}
	return nil
}

// getDBHandleByDbName gets the handle to a named chaincode database (here dbName == chaincodeName ?)
func (historyDB *HistoryDB) getDBHandleByDbName(dbName string) (*couchdb.CouchDatabase, error) {
	historyDB.mux.RLock()
	db := historyDB.namespaceDBs[dbName]
	historyDB.mux.RUnlock()
	if db != nil {
		return db, nil
	}

	historyDB.mux.Lock()
	defer historyDB.mux.Unlock()
	db = historyDB.namespaceDBs[dbName]
	if db == nil {
		var err error
		db, err = couchdb.CreateCouchDatabase(historyDB.couchInstance, dbName)
		if err != nil {
			return nil, err
		}
		historyDB.namespaceDBs[dbName] = db
	}
	return db, nil
}

func (historyDB *HistoryDB) buildCommitters(updateBatch *UpdateBatch) ([]batch, error) {
	numBatches := len(updateBatch.updates)
	batches := make([]batch, numBatches)

	i := 0
	for dbName, v := range updateBatch.updates {
		hdb, err := historyDB.getDBHandleByDbName(dbName)
		if err != nil {
			logger.Errorf("Error during buildCommitters: %s\n", err.Error())
			return nil, err
		}

		batches[i] = &updateCommitter{db: hdb, updates: v}
		i++
	}

	return batches, nil
}

// constructHistoryDbNameFromNs construct historyDB name (here ns == chaincodeName ?)
func (historyDB *HistoryDB) constructHistoryDbNameFromNs(ns string) string {
	return addHistoryDbNamePrefix(couchdb.ConstructNamespaceDBName(historyDB.chainName, ns))
}

// ensureFullCommitAndRecordSavepoint flushes all the dbs (corresponding to `namespaces`) to disk
// and Record a savepoint in the metadata db.
// Couch parallelizes writes in cluster or sharded setup and ordering is not guaranteed.
// Hence we need to fence the savepoint with sync. So ensure_full_commit on all updated
// namespace DBs is called before savepoint to ensure all block writes are flushed. Savepoint
// itself is flushed to the metadataDB.
func (historyDB *HistoryDB) ensureFullCommitAndRecordSavepoint(height *version.Height, dbNames []string) error {
	// ensure full commit to flush all changes on updated namespaces until now to disk
	// namespace also includes empty namespace which is nothing but metadataDB
	var dbs []*couchdb.CouchDatabase
	for _, dbName := range dbNames {
		db, err := historyDB.getDBHandleByDbName(dbName)
		if err != nil {
			return err
		}
		dbs = append(dbs, db)
	}
	if err := historyDB.ensureFullCommit(dbs); err != nil {
		return err
	}
	// construct savepoint document and save
	savepointCouchDoc, err := encodeSavepoint(height)
	if err != nil {
		return err
	}
	_, err = historyDB.metadataDB.SaveDoc(savepointDocID, "", savepointCouchDoc)
	if err != nil {
		logger.Errorf("Failed to save the savepoint to DB %s\n", err.Error())
		return err
	}
	// Note: Ensure full commit on metadataDB after storing the savepoint is not necessary
	// as CouchDB syncs states to disk periodically (every 1 second). If peer fails before
	// syncing the savepoint to disk, ledger recovery process kicks in to ensure consistency
	// between CouchDB and block store on peer restart
	return nil
}

// QueryResult - a general interface for supporting different types of query results. Actual types differ for different queries
type QueryResult interface{}

// ResultsIterator helps in iterates over query results
type ResultsIterator interface {
	Next() (QueryResult, error)
	Close()
}

// getCouchdbRangeScanIterator gets a couchdb iterator
// startKey is inclusive
// endKey is exclusive
func (historyDB *HistoryDB) getCouchdbRangeScanIterator(namespace string, startKey string, endKey string) (*couchdbQueryScanner, error) {
	logger.Debug("Entered [getCouchdbRangeScanIterator]")
	// Get the querylimit from core.yaml
	queryLimit := ledgerconfig.GetQueryLimit()
	dbName := historyDB.constructHistoryDbNameFromNs(namespace)
	db, err := historyDB.getDBHandleByDbName(dbName)
	if err != nil {
		return nil, err
	}
	queryResult, err := db.ReadDocRange(startKey, endKey, queryLimit, querySkip)
	if err != nil {
		logger.Debugf("[getCouchdbRangeScanIterator] Error calling ReadDocRange(): %s\n", err.Error())
		return nil, err
	}
	logger.Debug("Exiting [getCouchdbRangeScanIterator]")
	return newCouchdbQueryScanner(namespace, *queryResult, nil), nil
}

// getCouchdbRangeScanIteratorWithSkip gets a couchdb iterator
// startKey is inclusive
// endKey is exclusive
func (historyDB *HistoryDB) getCouchdbRangeScanIteratorWithSkip(namespace, startKey, endKey string, skip int) (*couchdbQueryScanner, error) {
	logger.Debug("Entered [getCouchdbRangeScanIteratorWithSkip]")
	queryLimit := ledgerconfig.GetHistoryRecordsPerPage()
	logger.Debugf("[getCouchdbRangeScanIteratorWithSkip] namespace=%s, startKey=%s, endKey=%s, skip=%d, limit=%d", startKey, endKey, skip, queryLimit)
	dbName := historyDB.constructHistoryDbNameFromNs(namespace)
	db, err := historyDB.getDBHandleByDbName(dbName)
	if err != nil {
		return nil, err
	}

	if endKey == "" {
		endKey = constructEndKey(SplitKeyFromCompositeKey(startKey))
	}

	queryResult, responseMetainfo, err := db.ReadDocRangeWithRangeQueryResponse(startKey, endKey, queryLimit, skip, false)
	if err != nil {
		logger.Debugf("[getCouchdbRangeScanIteratorWithSkip] Error calling ReadDocRangeWithRangeQueryResponse(): %s\n", err.Error())
		return nil, err
	}

	initOffset := responseMetainfo.GetOffset()
	totalRows := responseMetainfo.GetTotalRows()
	splitArrOfStartKey := strings.Split(startKey, "~")
	if !(splitArrOfStartKey[1] == "" && skip == 0) {
		totalRows, initOffset, err = getRecordCountAndInitOffsetByKey(db, splitArrOfStartKey[0], false)
		if err != nil {
			return nil, err
		}
	} else {
		lastKeyResponseMetaInfo, err := getLastKeyDescRangeQueryResponseMetaInfo(db, splitArrOfStartKey[0], false)
		if err != nil {
			return nil, err
		}
		if totalRows < lastKeyResponseMetaInfo.GetTotalRows() {
			totalRows = lastKeyResponseMetaInfo.GetTotalRows()
		}
		totalRows = totalRows - responseMetainfo.GetOffset() - lastKeyResponseMetaInfo.GetOffset()
	}

	responseMetainfo.SetTotalRows(totalRows)
	responseMetainfo.SetOffset(responseMetainfo.GetOffset() - initOffset)

	logger.Debug("Exiting [getCouchdbRangeScanIteratorWithSkip]")
	return newCouchdbQueryScanner(namespace, *queryResult, responseMetainfo), nil
}

// getCouchdbRangeScanIteratorWithOrder gets a couchdb iterator
// startKey is inclusive
// endKey is exclusive
func (historyDB *HistoryDB) getCouchdbRangeScanIteratorWithOrder(namespace, startKey, endKey string, skip int, descending bool) (*couchdbQueryScanner, error) {
	logger.Debug("Entered [getCouchdbRangeScanIteratorWithOrder]")
	queryLimit := ledgerconfig.GetHistoryRecordsPerPage()
	logger.Debugf("[getCouchdbRangeScanIteratorWithOrder] namespace=%s, startKey=%s, endKey=%s, skip=%d, limit=%d, descending=%v", startKey, endKey, skip, queryLimit, descending)
	dbName := historyDB.constructHistoryDbNameFromNs(namespace)
	db, err := historyDB.getDBHandleByDbName(dbName)
	if err != nil {
		return nil, err
	}

	if endKey == "" {
		key := SplitKeyFromCompositeKey(startKey)
		if descending == false {
			endKey = constructEndKey(key)
		} else {
			endKey = constructStartKey(key)
		}
	}

	queryResult, responseMetainfo, err := db.ReadDocRangeWithRangeQueryResponse(startKey, endKey, queryLimit, skip, descending)
	if err != nil {
		logger.Debugf("[getCouchdbRangeScanIteratorWithOrder] Error calling ReadDocRangeWithRangeQueryResponse(): %s\n", err.Error())
		return nil, err
	}

	initOffset := responseMetainfo.GetOffset()
	totalRows := responseMetainfo.GetTotalRows()
	splitArrOfStartKey := strings.Split(startKey, "~")

	if startKeyIsBaseFirstKey(startKey, &splitArrOfStartKey, skip, descending) {
		lastKeyResponseMetaInfo, err := getLastKeyDescRangeQueryResponseMetaInfo(db, splitArrOfStartKey[0], descending)
		if err != nil {
			return nil, err
		}
		if totalRows < lastKeyResponseMetaInfo.GetTotalRows() {
			totalRows = lastKeyResponseMetaInfo.GetTotalRows()
		}
		totalRows = totalRows - responseMetainfo.GetOffset() - lastKeyResponseMetaInfo.GetOffset()
	} else {
		totalRows, initOffset, err = getRecordCountAndInitOffsetByKey(db, splitArrOfStartKey[0], descending)
		if err != nil {
			return nil, err
		}
	}

	responseMetainfo.SetTotalRows(totalRows)
	responseMetainfo.SetOffset(responseMetainfo.GetOffset() - initOffset)

	logger.Debug("Exiting [getCouchdbRangeScanIteratorWithOrder]")
	return newCouchdbQueryScanner(namespace, *queryResult, responseMetainfo), nil
}

func startKeyIsBaseFirstKey(startKey string, splitArr *[]string, skip int, descending bool) bool {
	if skip != 0 {
		return false
	}

	if descending == false && (*splitArr)[1] == "" {
		return true
	}

	if descending == true && startKey == constructEndKey((*splitArr)[0]) {
		return true
	}

	return false
}

func getRecordCountAndInitOffsetByKey(db *couchdb.CouchDatabase, key string, descending bool) (int, int, error) {
	firstKeyResponseMetaInfo, err := getFirstKeyAscRangeQueryResponseMetaInfo(db, key, descending)
	if err != nil {
		return 0, 0, err
	}

	lastKeyResponseMetaInfo, err := getLastKeyDescRangeQueryResponseMetaInfo(db, key, descending)
	if err != nil {
		return 0, 0, err
	}

	maxCount := firstKeyResponseMetaInfo.GetTotalRows()
	if lastKeyResponseMetaInfo.GetTotalRows() > maxCount {
		maxCount = lastKeyResponseMetaInfo.GetTotalRows()
	}

	return (maxCount - firstKeyResponseMetaInfo.GetOffset() - lastKeyResponseMetaInfo.GetOffset()), firstKeyResponseMetaInfo.GetOffset(), nil
}

func getFirstKeyAscRangeQueryResponseMetaInfo(db *couchdb.CouchDatabase, key string, descending bool) (*couchdb.RangeQueryResponseMetaInfo, error) {
	logger.Debug("Entered [getFirstKeyAscRangeQueryResponseMetaInfo]")

	var startKey, endKey string
	if descending == false {
		startKey = constructStartKey(key)
		endKey = constructEndKey(key)
	} else {
		startKey = constructEndKey(key)
		endKey = constructStartKey(key)
	}

	logger.Debugf("[getFirstKeyAscRangeQueryResponseMetaInfo] startKey=%s, endKey=%s, descending=%v", startKey, endKey, descending)
	logger.Debug("Exiting [getFirstKeyAscRangeQueryResponseMetaInfo]")
	return db.GetRangeQueryResponseMetaInfo(startKey, endKey, descending)
}

func getLastKeyDescRangeQueryResponseMetaInfo(db *couchdb.CouchDatabase, key string, descending bool) (*couchdb.RangeQueryResponseMetaInfo, error) {
	logger.Debug("Entered [getLastKeyDescRangeQueryResponseMetaInfo]")

	var startKey, endKey string
	if descending == true {
		startKey = constructStartKey(key)
		endKey = constructEndKey(key)
	} else {
		startKey = constructEndKey(key)
		endKey = constructStartKey(key)
	}

	logger.Debugf("[getLastKeyDescRangeQueryResponseMetaInfo] startKey=%s, endKey=%s, descending=%v", startKey, endKey, descending)
	logger.Debug("Exiting [getLastKeyDescRangeQueryResponseMetaInfo]")
	return db.GetRangeQueryResponseMetaInfo(startKey, endKey, !descending)
}
