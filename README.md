# Historical Transaction Data Query API Enrichment for Hyperledger Fabric

This project provides pagination and rich query functionalities over historical transaction data that official historyleveldb of hyperledger fabric lacks of. The basic idea is using CouchDB to store blockchain metadata instead of LevelDB, then pagination and rich query could be achieved by leveraging corresponding CouchDB features. This prototype source code is based on Hyperledger Fabric v1.2 release, and users could choose CouchDB or LevelDB as the HistoryDB according to their needs by setting environment variable.

PS:
Please ignore changes to the following files,
`core/ledger/kvledger/history/historydb/historyleveldb/historyleveldb.go`
`core/scc/lscc/lscc.go`
`examples/cluster/config/core.yaml`
`integration/runner/testdata/core.yaml`
`integration/world/testdata/peer0.org1.example.com-core.yaml`
`integration/world/testdata/peer0.org2.example.com-core.yaml`
`integration/world/testdata/peer1.org1.example.com-core.yaml`
`integration/world/testdata/peer1.org2.example.com-core.yaml`
`sampleconfig/core.yaml`

## How to use

### Prerequisites

1. Set up development environment: read official doc at https://hyperledger-fabric.readthedocs.io/en/release-1.2/dev-setup/devenv.html

### Build
At the root path of this project

1. Run `make clean-all` if it's not your first build

2. [Optional: this is a workaround for not being able to download some files during build] 
    
    (1) Download gotools binaries from https://pan.baidu.com/s/1KCtG4FDyqSY16H1Xuc97Pg

    ( Or download and build gotools yourself, then put those binaries to `build/docker/gotools/bin` )

    (2) Run `cp -r build .build`

3. Run `make all`

### Pull docker images
After the build is completed, run `docker images` to see if  images such as `hyperledger/fabric-couchdb` and `hyperledger/fabric-ca`
alreay exist. If not, pull these images from hyperledger fabric official repository. 

### Set environment variables for peer containers

    - CORE_LEDGER_HISTORY_USEHISTORYCOUCHDB=true  // endble historycouchdb
    - CORE_LEDGER_HISTORY_ENABLERICHQUERY=true    // enable rich query
    - CORE_LEDGER_HISTORY_RECORDSPERPAGE=10       // records per page (page size)

### APIs added

`GetHistoryForKeyPageEnabled(namespace, startKey, endKey string, skip int, descending bool) (commonledger.ResultsIterator, error)` // users could use skip parameter to navigate to a given page or set skip to 0 and achieve pagination in a linked list way


`GetHistoryQueryResult(namespace string, query string) (commonledger.ResultsIterator, error)` // bookmark value could be included in the query parameter to achieve pagination of rich query result
