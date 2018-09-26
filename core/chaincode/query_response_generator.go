/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package chaincode

import (
	"errors"

	commonledger "github.com/hyperledger/fabric/common/ledger"
	"github.com/hyperledger/fabric/core/ledger/kvledger/history/historydb/historycouchdb"
	pb "github.com/hyperledger/fabric/protos/peer"
)

type QueryResponseGenerator struct {
	MaxResultLimit int
}

// NewQueryResponse takes an iterator and fetch state to construct QueryResponse
func (q *QueryResponseGenerator) BuildQueryResponse(txContext *TransactionContext, iter commonledger.ResultsIterator, iterID string) (*pb.QueryResponse, error) {
	pendingQueryResults := txContext.GetPendingQueryResult(iterID)
	for {
		queryResult, err := iter.Next()
		switch {
		case err != nil:
			chaincodeLogger.Errorf("Failed to get query result from iterator")
			txContext.CleanupQueryContext(iterID)
			return nil, err

		case queryResult == nil:
			// nil response from iterator indicates end of query results
			batch := pendingQueryResults.Cut()
			txContext.CleanupQueryContext(iterID)
			return &pb.QueryResponse{Results: batch, HasMore: false, Id: iterID}, nil

		case pendingQueryResults.Size() == q.MaxResultLimit:
			// max number of results queued up, cut batch, then add current result to pending batch
			batch := pendingQueryResults.Cut()
			if err := pendingQueryResults.Add(queryResult); err != nil {
				txContext.CleanupQueryContext(iterID)
				return nil, err
			}
			return &pb.QueryResponse{Results: batch, HasMore: true, Id: iterID}, nil

		default:
			if err := pendingQueryResults.Add(queryResult); err != nil {
				txContext.CleanupQueryContext(iterID)
				return nil, err
			}
		}
	}
}

// BuildQueryResponseWithMetaInfo takes an iterator and fetch state to construct QueryResponse
func (q *QueryResponseGenerator) BuildQueryResponseWithMetaInfo(txContext *TransactionContext, iter commonledger.ResultsIterator, iterID string) (*pb.QueryResponseWithMetaInfo, error) {
	pendingQueryResults := txContext.GetPendingQueryResult(iterID)
	for {
		queryResult, err := iter.Next()
		switch {
		case err != nil:
			chaincodeLogger.Errorf("Failed to get query result from iterator")
			txContext.CleanupQueryContext(iterID)
			return nil, err

		case queryResult == nil:
			// nil response from iterator indicates end of query results
			batch := pendingQueryResults.Cut()
			txContext.CleanupQueryContext(iterID)

			if historyScanner, ok := iter.(*historycouchdb.HistoryScanner); ok {
				metaInfo := historyScanner.GetResponseMetaInfo()
				return &pb.QueryResponseWithMetaInfo{Results: batch, HasMore: false, Id: iterID, TotalRows: int32(metaInfo.GetTotalRows()), Offset: int32(metaInfo.GetOffset()), Bookmark: historyScanner.GetBookmark()}, nil
			}
			return nil, errors.New("iter is not type of *historycouchdb.HistoryScanner")

		case pendingQueryResults.Size() == q.MaxResultLimit:
			// max number of results queued up, cut batch, then add current result to pending batch
			batch := pendingQueryResults.Cut()
			if err := pendingQueryResults.Add(queryResult); err != nil {
				txContext.CleanupQueryContext(iterID)
				return nil, err
			}

			if historyScanner, ok := iter.(*historycouchdb.HistoryScanner); ok {
				metaInfo := historyScanner.GetResponseMetaInfo()
				return &pb.QueryResponseWithMetaInfo{Results: batch, HasMore: true, Id: iterID, TotalRows: int32(metaInfo.GetTotalRows()), Offset: int32(metaInfo.GetOffset()), Bookmark: historyScanner.GetBookmark()}, nil
			}
			return nil, errors.New("iter is not type of *historycouchdb.HistoryScanner")

		default:
			if err := pendingQueryResults.Add(queryResult); err != nil {
				txContext.CleanupQueryContext(iterID)
				return nil, err
			}
		}
	}
}
