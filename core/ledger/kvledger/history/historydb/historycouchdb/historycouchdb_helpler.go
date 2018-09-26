package historycouchdb

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"

	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/version"
	"github.com/hyperledger/fabric/core/ledger/util/couchdb"
)

const (
	idField               = "_id"
	blockNumField         = "BlockNum"
	txNumField            = "TxNum"
	keyField              = "key"
	maxUint64Value uint64 = math.MaxUint64
)

// couchSavepointData data for couchdb
type couchSavepointData struct {
	BlockNum uint64 `json:"BlockNum"`
	TxNum    uint64 `json:"TxNum"`
}

type jsonMap map[string]interface{}

// SplitKeyFromCouchDocID get key from a given couchdoc id
func SplitKeyFromCouchDocID(couchDocID string) (string, error) {
	splitArr := strings.Split(couchDocID, "~")
	if len(splitArr) == 2 {
		return splitArr[0], nil
	}
	return "", errors.New("SplitKeyFromCouchDocID got error")
}

// SplitKeyFromCompositeKey get key from a given compositeKey
func SplitKeyFromCompositeKey(compositeKey string) string {
	splitArr := strings.Split(compositeKey, "~")
	return splitArr[0]
}

func encodeSavepoint(height *version.Height) (*couchdb.CouchDoc, error) {
	var err error
	var savepointDoc couchSavepointData
	// construct savepoint document
	savepointDoc.BlockNum = height.BlockNum
	savepointDoc.TxNum = height.TxNum
	savepointDocJSON, err := json.Marshal(savepointDoc)
	if err != nil {
		logger.Errorf("Failed to create savepoint data %s\n", err.Error())
		return nil, err
	}
	return &couchdb.CouchDoc{JSONValue: savepointDocJSON, Attachments: nil}, nil
}

func decodeSavepoint(couchDoc *couchdb.CouchDoc) (*version.Height, error) {
	savepointDoc := &couchSavepointData{}
	if err := json.Unmarshal(couchDoc.JSONValue, &savepointDoc); err != nil {
		logger.Errorf("Failed to unmarshal savepoint data %s\n", err.Error())
		return nil, err
	}
	return &version.Height{BlockNum: savepointDoc.BlockNum, TxNum: savepointDoc.TxNum}, nil
}

func addHistoryDbNamePrefix(ns string) string {
	return fmt.Sprintf("%s%s", historyDbNamePrefix, ns)
}

func constructStartKey(key string) string {
	return fmt.Sprintf("%s~", key)
}

func constructEndKey(key string) string {
	return fmt.Sprintf("%s~%v", key, maxUint64Value)
}
