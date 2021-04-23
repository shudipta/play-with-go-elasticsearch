package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/elastic/go-elasticsearch/v7/estransport"
	"io"
	"log"
	"math/rand"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/dustin/go-humanize"

	"github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esutil"
)

type Article struct {
	Group     int       `json:"group,omitempty"`
	ID        int       `json:"id,omitempty"`
	Title     string    `json:"title,omitempty"`
	Body      string    `json:"body,omitempty"`
	Published time.Time `json:"published,omitempty"`
	Author    *Author   `json:"author,omitempty"`
}

type Author struct {
	FirstName string `json:"first_name,omitempty"`
	LastName  string `json:"last_name,omitempty"`
}

var (
	indexName  string
	numWorkers int
	flushBytes int
	numItems   int

	del_only bool
	cr_only bool
)

func init() {
	flag.StringVar(&indexName, "index", "test-bulk-example", "Index name")
	flag.IntVar(&numWorkers, "workers", runtime.NumCPU(), "Number of indexer workers")
	flag.IntVar(&flushBytes, "flush", 5e+6, "Flush threshold in bytes")
	flag.IntVar(&numItems, "count", 2, "Number of documents to generate")

	flag.BoolVar(&del_only, "del-only", false, "only delete index")
	flag.BoolVar(&cr_only, "cr-only", false, "only create index")

	flag.Parse()

	rand.Seed(time.Now().UnixNano())
}

func main() {
	log.SetFlags(0)

	var (
		articles []*Article
		//countSuccessful uint64

		//res *esapi.Response
		err error
	)

	log.Printf(
		"\x1b[1mBulkIndexer\x1b[0m: documents [%s] workers [%d] flush [%s]",
		humanize.Comma(int64(numItems)), numWorkers, humanize.Bytes(uint64(flushBytes)))
	log.Println(strings.Repeat("▁", 65))

	// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
	//
	// Use a third-party package for implementing the backoff function
	//
	retryBackoff := backoff.NewExponentialBackOff()
	// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

	// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
	//
	// Create the Elasticsearch client
	//
	// NOTE: For optimal performance, consider using a third-party HTTP transport package.
	//       See an example in the "benchmarks" folder.
	//
	es, err := elasticsearch.NewClient(elasticsearch.Config{
		// Retry on 429 TooManyRequests statuses
		//
		RetryOnStatus: []int{502, 503, 504, 429},

		// Configure the backoff function
		//
		RetryBackoff: func(i int) time.Duration {
			if i == 1 {
				retryBackoff.Reset()
			}
			return retryBackoff.NextBackOff()
		},

		// Retry up to 5 attempts
		//
		MaxRetries: 5,

		Logger: &estransport.TextLogger{Output: os.Stdout},
	})
	if err != nil {
		log.Fatalf("Error creating the client: %s", err)
	}
	// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

	// Generate the articles collection
	//
	//names := []string{"Alice", "John", "Mary"}
	for i := 1; i <= numItems; i++ {
		articles = append(articles, &Article{
			ID:        i,
			Title: strings.Join([]string{"Title", strconv.Itoa(i)}, "_"),
			//Body:      "Lorem ipsum dolor sit amet...",
			//Published: time.Now().Round(time.Second).UTC().AddDate(0, 0, i),
			//Author: Author{
			//	FirstName: names[rand.Intn(len(names))],
			//	LastName:  "Smith",
			//},
		})
	}
	log.Printf("→ Generated %s articles", humanize.Comma(int64(len(articles))))

	// Re-create the index
	//
	if del_only {
		deleteIndex(es, "test-index-0")
		deleteIndex(es, "test-index-1")
		return
	}
	if cr_only {
		createIndex(es, "test-index-0")
		createIndex(es, "test-index-1")
		return
	}

	start := time.Now().UTC()
	bi := bulkInit(es)
	// Loop over the collection - 0
	//
	for i, _ := range articles {
		a := articles[i]
		a.Group = 0
		// Prepare the data payload: encode article to JSON
		//

		// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
		//
		// Add an item to the BulkIndexer
		//
		bulkAdd(bi, strconv.Itoa(a.ID), "test-index-0", "create", a)
		// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
	}

	// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
	// Close the indexer
	//
	bulkClose(bi)
	bulkStates(bi, start)
	// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

	start = time.Now().UTC()
	bi = bulkInit(es)
	// Loop over the collection - 1
	//
	for _, a := range articles {
		a.Group = 1
		// Prepare the data payload: encode article to JSON
		//
		//data, err := json.Marshal(map[string]Article{"doc":*a})

		// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
		//
		// Add an item to the BulkIndexer
		//
		bulkAdd(bi, strconv.Itoa(a.ID), "test-index-1", "create", a)
		// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
	}

	// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
	// Close the indexer
	//
	bulkClose(bi)
	// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

	bulkStates(bi, start)
}

func deleteIndex(es *elasticsearch.Client, idx string) {
	res, err := es.Indices.Delete([]string{idx}, es.Indices.Delete.WithIgnoreUnavailable(true))
	if err != nil || res.IsError() {
		log.Fatalf("Cannot delete index %q: %s", idx, err)
	}
	res.Body.Close()
}

func createIndex(es *elasticsearch.Client, idx string) {
	res, err := es.Indices.Create(idx)
	if err != nil {
		log.Fatalf("Cannot create index %q: %s", idx, err)
	}
	if res.IsError() {
		log.Fatalf("Cannot create index %q: %s", idx, res)
	}
	res.Body.Close()
}

func bulkInit(es *elasticsearch.Client) esutil.BulkIndexer {
	// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
	//
	// Create the BulkIndexer
	//
	// NOTE: For optimal performance, consider using a third-party JSON decoding package.
	//       See an example in the "benchmarks" folder.
	//
	bi, err := esutil.NewBulkIndexer(esutil.BulkIndexerConfig{
		//Index:         indexName,        // The default index name
		Client:        es,               // The Elasticsearch client
		NumWorkers:    numWorkers,       // The number of worker goroutines
		FlushBytes:    int(flushBytes),  // The flush threshold in bytes
		FlushInterval: 30 * time.Second, // The periodic flush interval

		Decoder: &DefaultJSONDecoder{},
	})
	if err != nil {
		log.Fatalf("Error creating the indexer: %s", err)
	}
	// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
	return bi
}

func bulkAdd(bi esutil.BulkIndexer, id, idx, action string, data interface{}) {
	biItem := esutil.BulkIndexerItem{
		Index: idx,

		// Action field configures the operation to perform (index, create, delete, update)
		Action: action,

		// DocumentID is the (optional) document ID
		DocumentID: id,

		// OnSuccess is called for each successful operation
		OnSuccess: func(ctx context.Context, item esutil.BulkIndexerItem, res esutil.BulkIndexerResponseItem) {
			//atomic.AddUint64(&countSuccessful, 1)
		},

		// OnFailure is called for each failed operation
		OnFailure: func(ctx context.Context, item esutil.BulkIndexerItem, res esutil.BulkIndexerResponseItem, err error) {
			log.Println("++++++ err = ", err, " ++++++++", serializeData(res, true))
			if err != nil {
				log.Printf("ERROR: %s", err)
			} else {
				log.Printf("ERROR: [ status: %v; result: %v; err_type: %s; err_reason: %s]",
					res.Status, res.Result, res.Error.Type, res.Error.Reason)
			}
		},
	}
	if action == "update" {
		data = map[string]interface{}{
			"doc": data,
		}
	}
	if action != "delete" {
		// Body is an `io.Reader` with the payload
		biItem.Body = esutil.NewJSONReader(data)
		fmt.Println("===========", serializeData(data))
	}
	err := bi.Add(
		context.Background(),
		biItem,
	)

	if err != nil {
		log.Fatalf("Unexpected error: %s", err)
	}
}

func bulkClose(bi esutil.BulkIndexer) {
	if err := bi.Close(context.Background()); err != nil {
		log.Fatalf("Unexpected error: %s", err)
	}
}

func bulkStates(bi esutil.BulkIndexer, start time.Time) {
	biStats := bi.Stats()

	// Report the results: number of indexed docs, number of errors, duration, indexing rate
	//
	log.Println(strings.Repeat("▔", 65))

	dur := time.Since(start)

	jb, _ := json.MarshalIndent(biStats, "", "  ")
	log.Println(string(jb))
	if biStats.NumFailed > 0 {
		log.Printf(
			"Indexed [%s] documents with [%s] errors in %s (%s docs/sec)",
			humanize.Comma(int64(biStats.NumFlushed)),
			humanize.Comma(int64(biStats.NumFailed)),
			dur.Truncate(time.Millisecond),
			humanize.Comma(int64(1000.0/float64(dur/time.Millisecond)*float64(biStats.NumFlushed))),
		)
	} else {
		log.Printf(
			"Sucessfuly indexed [%s] documents in %s (%s docs/sec)",
			humanize.Comma(int64(biStats.NumFlushed)),
			dur.Truncate(time.Millisecond),
			humanize.Comma(int64(1000.0/float64(dur/time.Millisecond)*float64(biStats.NumFlushed))),
		)
	}
}

type DefaultJSONDecoder struct {
	resp esutil.BulkIndexerResponse
}

func (d DefaultJSONDecoder) UnmarshalFromReader(r io.Reader, blk *esutil.BulkIndexerResponse) error {
	d.resp = *blk
	return json.NewDecoder(r).Decode(blk)
}

func serializeData(data interface{}, opts ...interface{}) interface{} {
	var (
		res []byte
		err error
	)

	if len(opts) > 0 && opts[0].(bool) {
		res, err = json.MarshalIndent(data, "", "  ")
	} else {
		res, err = json.Marshal(data)
	}
	if err != nil {
		log.Printf("Cannot encode data %+v: %v", data, err)
		return data
	}
	return string(res)
}
