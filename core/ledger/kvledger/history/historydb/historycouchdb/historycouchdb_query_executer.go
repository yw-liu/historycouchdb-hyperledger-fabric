package historycouchdb

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"

	commonledger "github.com/hyperledger/fabric/common/ledger"
	"github.com/hyperledger/fabric/common/ledger/blkstorage"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/rwsetutil"
	"github.com/hyperledger/fabric/core/ledger/ledgerconfig"
	"github.com/hyperledger/fabric/core/ledger/util/couchdb"
	"github.com/hyperledger/fabric/protos/common"
	"github.com/hyperledger/fabric/protos/ledger/queryresult"
	putils "github.com/hyperledger/fabric/protos/utils"
)

const (
	emptyBookmark string = ""
)

type historyType uint8

const (
	historyForKey historyType = iota + 1
	historyForQuery
)

var emptyQueryResponseMetaInfo = &couchdb.RangeQueryResponseMetaInfo{TotalRows: 0, Offset: 0}

// CouchHistoryDBQueryExecutor is a query executor against the CouchDB history DB
type CouchHistoryDBQueryExecutor struct {
	historyDB  *HistoryDB
	blockStore blkstorage.BlockStore
}

//HistoryScanner implements ResultsIterator for iterating through history results
type HistoryScanner struct {
	namespace      string
	key            string
	couchdbScanner couchdbQueryScanner
	blockStore     blkstorage.BlockStore
	bookmark       string
	hType          historyType
}

type blockTxNum struct {
	key      string
	blockNum uint64
	txNum    uint64
}

type couchdbQueryScanner struct {
	cursor                int
	namespace             string
	results               []couchdb.QueryResult
	queryResponseMetaInfo *couchdb.RangeQueryResponseMetaInfo
}

func (blkTxNum *blockTxNum) getKey() string {
	return blkTxNum.key
}

func (blkTxNum *blockTxNum) setKey(key string) {
	blkTxNum.key = key
}

func (blkTxNum *blockTxNum) getBlockNum() uint64 {
	return blkTxNum.blockNum
}

func (blkTxNum *blockTxNum) getTxNum() uint64 {
	return blkTxNum.txNum
}

// GetHistoryQueryResult implements method in interface `ledger.HistoryQueryExecutor`
func (q *CouchHistoryDBQueryExecutor) GetHistoryQueryResult(namespace string, query string) (commonledger.ResultsIterator, error) {
	logger.Debug("[CouchHistoryDBQueryExecutor] Entered GetHistoryQueryResult")

	if ledgerconfig.IfUseHistorycouchdb() == false {
		return nil, errors.New("Historycouchdb not enabled")
	}

	if ledgerconfig.IsCouchDBEnabled() == false {
		return nil, errors.New("CouchDB not enabled")
	}

	// Get the querylimit from core.yaml
	queryLimit := ledgerconfig.GetHistoryRecordsPerPage() //GetQueryLimit()
	// Explicit paging not yet supported.
	// Use queryLimit from config and 0 skip.
	queryString, err := applyAdditionalQueryOptions(query, queryLimit, 0)
	if err != nil {
		logger.Debugf("Error calling applyAdditionalQueryOptions(): %s\n", err.Error())
		return nil, err
	}
	dbName := q.historyDB.constructHistoryDbNameFromNs(namespace)
	db, err := q.historyDB.getDBHandleByDbName(dbName)
	if err != nil {
		return nil, err
	}

	queryResult, bookmark, err := db.QueryDocumentsBookmarkEnabled(queryString)
	if err != nil {
		logger.Debugf("Error calling QueryDocumQueryDocumentsBookmarkEnabledents(): %s\n", err.Error())
		return nil, err
	}

	logger.Debugf("[CouchHistoryDBQueryExecutor][GetHistoryQueryResult] queryResult=%v, bookmark=%s", queryResult, bookmark)

	logger.Debugf("[CouchHistoryDBQueryExecutor] Exiting GetHistoryQueryResult")

	// Attention: key arg of newHistoryScanner is not used
	return newHistoryScanner(namespace, "", *newCouchdbQueryScanner(namespace, *queryResult, emptyQueryResponseMetaInfo), q.blockStore, bookmark, historyForQuery), nil
}

// GetHistoryForKey implements method in interface `ledger.HistoryQueryExecutor`
func (q *CouchHistoryDBQueryExecutor) GetHistoryForKey(namespace string, key string) (commonledger.ResultsIterator, error) {
	logger.Debug("Entered GetHistoryForKey")

	if ledgerconfig.IsHistoryDBEnabled() == false {
		return nil, errors.New("History tracking not enabled - historyDatabase is false")
	}

	startKey := constructStartKey(key)
	endKey := constructEndKey(key)

	logger.Debugf("[GetHistoryForKey] startKey=%s, endKey=%s", startKey, endKey)

	// range scan to find any history records starting with namespace~key
	couchdbScanner, err := q.historyDB.getCouchdbRangeScanIterator(namespace, startKey, endKey)
	if err != nil {
		return nil, err
	}

	logger.Debug("Existing GetHistoryForKey")
	return newHistoryScanner(namespace, key, *couchdbScanner, q.blockStore, emptyBookmark, historyForKey), nil
}

// GetHistoryForKeyPageEnabled implements method in interface `ledger.HistoryQueryExecutor`
func (q *CouchHistoryDBQueryExecutor) GetHistoryForKeyPageEnabled(namespace, startKey, endKey string, skip int, descending bool) (commonledger.ResultsIterator, error) {
	logger.Debug("Entered GetHistoryForKeyPageEnabled")

	if !(ledgerconfig.IfUseHistorycouchdb() && ledgerconfig.IsCouchDBEnabled()) {
		return nil, errors.New("GetHistoryForKeyPageEnabled only supports historycouchdb, couchdb and historyDatabase must enabled")
	}

	logger.Debugf("[GetHistoryForKeyPageEnabled] namespace=%s, startKey=%s, endKey=%s, skip=%d, descending=%v", namespace, startKey, endKey, skip, descending)

	// range scan to find any history records starting with namespace~key
	couchdbScanner, err := q.historyDB.getCouchdbRangeScanIteratorWithOrder(namespace, startKey, endKey, skip, descending)
	if err != nil {
		return nil, err
	}

	key := SplitKeyFromCompositeKey(startKey)

	logger.Debug("Existing GetHistoryForKeyPageEnabled")
	return newHistoryScanner(namespace, key, *couchdbScanner, q.blockStore, emptyBookmark, historyForKey), nil
}

// GetResponseMetaInfo retrives TotalRows and Offset info
func (scanner *HistoryScanner) GetResponseMetaInfo() *couchdb.RangeQueryResponseMetaInfo {
	return scanner.couchdbScanner.queryResponseMetaInfo
}

// GetBookmark retrives bookmark
func (scanner *HistoryScanner) GetBookmark() string {
	return scanner.bookmark
}

// Next retrives next record
func (scanner *HistoryScanner) Next() (commonledger.QueryResult, error) {
	logger.Debugf("Entered HistoryScanner.Next")

	blockAndTxNum, err := scanner.couchdbScanner.Next()
	if blockAndTxNum == nil {
		return nil, err
	}

	logger.Debugf("[HistoryScanner.Next] blockAndTxNum=%v", blockAndTxNum)

	blkTxNum := blockAndTxNum.(*blockTxNum)

	keyToUse := scanner.key
	if keyToUse == "" {
		keyToUse = blkTxNum.getKey()
	}

	logger.Debugf("Found history record for namespace:%s key:%s at blockNumTranNum %v:%v\n",
		scanner.namespace, keyToUse, blkTxNum.getBlockNum(), blkTxNum.getTxNum())

	// Get the transaction from block storage that is associated with this history record
	tranEnvelope, err := scanner.blockStore.RetrieveTxByBlockNumTranNum(blkTxNum.getBlockNum(), blkTxNum.getTxNum())
	if err != nil {
		return nil, err
	}

	// Get the txid, key write value, timestamp, and delete indicator associated with this transaction
	var queryResult commonledger.QueryResult
	queryResult, err = getKeyModificationFromTran(tranEnvelope, scanner.namespace, keyToUse, scanner.hType)

	if err != nil {
		return nil, err
	}

	txid := queryResult.(*queryresult.KeyModification).TxId
	logger.Debugf("Found historic key value for namespace:%s key:%s from transaction %s\n",
		scanner.namespace, keyToUse, txid)

	return queryResult, nil
}

// Close close the scanner
func (scanner *HistoryScanner) Close() {
	scanner.couchdbScanner.Close()
}

// getKeyModificationFromTran inspects a transaction for writes to a given key
func getKeyModificationFromTran(tranEnvelope *common.Envelope, namespace string, key string, hType historyType) (commonledger.QueryResult, error) {
	logger.Debugf("Entering getKeyModificationFromTran, namespace=%s, key=%s\n", namespace, key)

	// extract action from the envelope
	payload, err := putils.GetPayload(tranEnvelope)
	if err != nil {
		return nil, err
	}

	tx, err := putils.GetTransaction(payload.Data)
	if err != nil {
		return nil, err
	}

	_, respPayload, err := putils.GetPayloads(tx.Actions[0])
	if err != nil {
		return nil, err
	}

	chdr, err := putils.UnmarshalChannelHeader(payload.Header.ChannelHeader)
	if err != nil {
		return nil, err
	}

	txID := chdr.TxId
	timestamp := chdr.Timestamp

	txRWSet := &rwsetutil.TxRwSet{}

	// Get the Result from the Action and then Unmarshal
	// it into a TxReadWriteSet using custom unmarshalling
	if err = txRWSet.FromProtoBytes(respPayload.Results); err != nil {
		return nil, err
	}

	// look for the namespace and key by looping through the transaction's ReadWriteSets
	for _, nsRWSet := range txRWSet.NsRwSets {
		if nsRWSet.NameSpace == namespace {
			// got the correct namespace, now find the key write
			for _, kvWrite := range nsRWSet.KvRwSet.Writes {
				if kvWrite.Key == key {

					resultValueBytes := kvWrite.Value

					if hType == historyForQuery {
						// add key to Value bytes
						var valMap map[string]interface{}
						err := json.Unmarshal(kvWrite.Value, &valMap)
						if err == nil {
							valMap[keyField] = key
							newValBytes, jsonErr := json.Marshal(valMap)
							if jsonErr == nil {
								resultValueBytes = newValBytes
							}
						}
					}

					return &queryresult.KeyModification{TxId: txID, Value: resultValueBytes,
						Timestamp: timestamp, IsDelete: kvWrite.IsDelete}, nil
				}
			} // end keys loop
			return nil, errors.New("Key not found in namespace's writeset")
		} // end if
	} //end namespaces loop
	return nil, errors.New("Namespace not found in transaction's ReadWriteSets")
}

func newHistoryScanner(namespace string, key string,
	couchdbScanner couchdbQueryScanner, blockStore blkstorage.BlockStore, bookmark string, hType historyType) *HistoryScanner {

	return &HistoryScanner{namespace, key, couchdbScanner, blockStore, bookmark, hType}
}

func newCouchdbQueryScanner(namespace string, queryResults []couchdb.QueryResult, queryResponseMetaInfo *couchdb.RangeQueryResponseMetaInfo) *couchdbQueryScanner {
	return &couchdbQueryScanner{-1, namespace, queryResults, queryResponseMetaInfo}
}

func (couchScanner *couchdbQueryScanner) Next() (QueryResult, error) {
	couchScanner.cursor++
	if couchScanner.cursor >= len(couchScanner.results) {
		return nil, nil
	}
	selectedResultRecord := couchScanner.results[couchScanner.cursor]

	blockAndTxNum, err := couchDocToBlockTxNum(&couchdb.CouchDoc{JSONValue: selectedResultRecord.Value, Attachments: selectedResultRecord.Attachments})
	if err != nil {
		return nil, err
	}

	targetKey, _ := SplitKeyFromCouchDocID(selectedResultRecord.ID)
	blockAndTxNum.setKey(targetKey)
	return blockAndTxNum, nil
}

func (couchScanner *couchdbQueryScanner) Close() {
	couchScanner = nil
}

func couchDocToBlockTxNum(doc *couchdb.CouchDoc) (*blockTxNum, error) {
	var err error
	// create a generic map unmarshal the json
	jsonResult := make(map[string]interface{})
	decoder := json.NewDecoder(bytes.NewBuffer(doc.JSONValue))
	decoder.UseNumber()
	if err = decoder.Decode(&jsonResult); err != nil {
		return nil, err
	}

	// verify the blockNumField field exists
	if _, fieldFound := jsonResult[blockNumField]; !fieldFound {
		return nil, fmt.Errorf("The blockNumField field %s was not found", blockNumField)
	}

	// verify the txNumField field exists
	if _, fieldFound := jsonResult[txNumField]; !fieldFound {
		return nil, fmt.Errorf("The txNumField field %s was not found", txNumField)
	}

	// blockNum := jsonResult[blockNumField].(uint64)
	// txNum := jsonResult[txNumField].(uint64)

	blockNum, err := strconv.ParseUint(jsonResult[blockNumField].(json.Number).String(), 10, 64)
	if err != nil {
		return nil, err
	}
	txNum, err := strconv.ParseUint(jsonResult[txNumField].(json.Number).String(), 10, 64)
	if err != nil {
		return nil, err
	}

	return &blockTxNum{blockNum: blockNum, txNum: txNum}, nil
}

// applyAdditionalQueryOptions will add additional fields to the query required for query processing
func applyAdditionalQueryOptions(queryString string, queryLimit, querySkip int) (string, error) {
	const jsonQueryFields = "fields"
	const jsonQueryLimit = "limit"
	const jsonQuerySkip = "skip"
	//create a generic map for the query json
	jsonQueryMap := make(map[string]interface{})
	//unmarshal the selector json into the generic map
	decoder := json.NewDecoder(bytes.NewBuffer([]byte(queryString)))
	decoder.UseNumber()
	err := decoder.Decode(&jsonQueryMap)
	if err != nil {
		return "", err
	}
	if fieldsJSONArray, ok := jsonQueryMap[jsonQueryFields]; ok {
		switch fieldsJSONArray.(type) {
		case []interface{}:
			//Add the "_id" field,  these are needed by default
			jsonQueryMap[jsonQueryFields] = append(fieldsJSONArray.([]interface{}),
				idField)
		default:
			return "", fmt.Errorf("fields definition must be an array")
		}
	}
	// Add limit
	// This will override any limit passed in the query.
	// Explicit paging not yet supported.
	jsonQueryMap[jsonQueryLimit] = queryLimit
	// Add skip
	// This will override any skip passed in the query.
	// Explicit paging not yet supported.
	jsonQueryMap[jsonQuerySkip] = querySkip
	//Marshal the updated json query
	editedQuery, err := json.Marshal(jsonQueryMap)
	if err != nil {
		return "", err
	}
	logger.Debugf("Rewritten query: %s", editedQuery)
	return string(editedQuery), nil
}
