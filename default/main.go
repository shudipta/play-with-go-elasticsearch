package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esapi"
	"github.com/elastic/go-elasticsearch/v7/estransport"
	"os"

	//"github.com/elastic/go-elasticsearch/v7/esapi"
	"github.com/elastic/go-elasticsearch/v7/esutil"
	"log"
	"sync"

	//"strconv"
	"strings"
)

var (
	actions string
)

func init() {
	flag.StringVar(&actions, "actions", "create,update,search,delete", "actions to be taken in order")
	flag.Parse()
}

func deletebyquery(es *elasticsearch.Client) {
	// Delete by query
	res, err := es.DeleteByQuery(
		[]string{"foo-index"},
		esutil.NewJSONReader(map[string]interface{}{
			"query": map[string]interface{}{
				"match": map[string]interface{}{
					//"foo.name": "Popular",
					"name": "updtd",
				},
			},
		}),
		es.DeleteByQuery.WithContext(context.Background()),
		es.DeleteByQuery.WithDocumentType("foo_document"),
		//es.Search.WithTrackTotalHits(true),
		es.DeleteByQuery.WithPretty(),
	)
	if err != nil {
		log.Fatalf("Error getting response: %s", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		log.Printf("[%s] Error deleting documents by query", res.Status())
	} else {
		// Deserialize the response into a map.
		var r map[string]interface{}
		if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
			log.Printf("Error parsing the response body: %s", err)
		} else {
			jb, _ := json.MarshalIndent(r, "", "  ")
			log.Println(string(jb))
			// Print the response status and indexed document version.
			log.Printf("[%s] deleted=%v", res.Status(), r["deleted"].(float64))
		}
	}
}

func delete(es *elasticsearch.Client) {
	var wg sync.WaitGroup

	for i, title := range []string{"Test One",
		"Test Two",
	} {
		wg.Add(1)

		go func(i int, title string) {
			defer wg.Done()

			// Set up the request object.
			req := esapi.DeleteRequest{
				Index: "foo-index",
				//DocumentType: "foo_document",
				DocumentID: "e73ad3f5-b370-4c73-86a3-fb63f8caf90c-" + title,
				//IfPrimaryTerm:       nil,
				//IfSeqNo:             nil,
				//Refresh:             "",
				//Routing:             "",
				//Timeout:             0,
				//Version:             nil,
				//VersionType:         "",
				//WaitForActiveShards: "",
				Pretty: true,
				//Human:               true,
				//ErrorTrace:          true,
				//FilterPath:          nil,
				//Header:              nil,
			}

			// Perform the request with the client.
			ires, ierr := req.Do(context.Background(), es)
			if ierr != nil {
				log.Fatalf("Error getting response: %s", ierr)
			}
			defer ires.Body.Close()

			log.Println(ires)
			if ires.IsError() {
				log.Printf("Error deleting document ID=%d; response: %v", i+1, ires)
			} else {
				// Deserialize the response into a map.
				var r map[string]interface{}
				if err := json.NewDecoder(ires.Body).Decode(&r); err != nil {
					log.Printf("Error parsing the response body: %s", err)
				} else {
					jb, _ := json.MarshalIndent(r, "", "  ")
					log.Println(string(jb))
					// Print the response status and indexed document version.
					log.Printf("[%s] %s; version=%d", ires.Status(), r["result"], int(r["_version"].(float64)))
				}
			}
		}(i, title)
	}
	wg.Wait()

	log.Println(strings.Repeat("-", 37))
}

func create(es *elasticsearch.Client) {
	var wg sync.WaitGroup

	for i, title := range []string{"Test One",
		"Test Two",
	} {
		wg.Add(1)

		go func(i int, title string) {
			defer wg.Done()

			// Build the request body.
			var b strings.Builder
			b.WriteString(`{
"id": "e73ad3f5-b370-4c73-86a3-fb63f8caf90c-` + title + `",
"foo": { "name": "Popular"},
"name": "Welcome Foo",
}`)
			//b.WriteString(title)
			//b.WriteString(`"}`)

			// Set up the request object.
			req := esapi.CreateRequest{
				Index: "foo-index",
				//DocumentType: "foo_document",
				DocumentID: "e73ad3f5-b370-4c73-86a3-fb63f8caf90c-" + title,
				Body:       strings.NewReader(b.String()),
				//Refresh:    "true",
				Pretty: true,
			}

			// Perform the request with the client.
			ires, ierr := req.Do(context.Background(), es)
			if ierr != nil {
				log.Fatalf("Error getting response: %s", ierr)
			}
			defer ires.Body.Close()

			log.Println(ires)
			if ires.IsError() {
				log.Printf("Error creating document ID=%d => %v", i+1, ires)
			} else {
				// Deserialize the response into a map.
				var r map[string]interface{}
				if err := json.NewDecoder(ires.Body).Decode(&r); err != nil {
					log.Printf("Error parsing the response body: %s", err)
				} else {
					jb, _ := json.MarshalIndent(r, "", "  ")
					log.Println(string(jb))
					// Print the response status and indexed document version.
					log.Printf("[%s] %s; version=%d", ires.Status(), r["result"], int(r["_version"].(float64)))
				}
			}
		}(i, title)
	}
	wg.Wait()

	log.Println(strings.Repeat("-", 37))
}

func update(es *elasticsearch.Client) {
	var wg sync.WaitGroup

	for i, title := range []string{"Test One",
		"Test Two",
	} {
		wg.Add(1)

		go func(i int, title string) {
			defer wg.Done()

			// Build the request body.
			var b strings.Builder

			b.WriteString(`{
"id": "e73ad3f5-b370-4c73-86a3-fb63f8caf90c-` + title + `",
"name": "Welcome Foo - updtd ` + title + `"
}`)
			a := struct {
				ID   string `json:"id"`
				Name string `json:"name"`
			}{
				ID:   "e73ad3f5-b370-4c73-86a3-fb63f8caf90c-" + title,
				Name: "Welcome Foo - updtd " + title,
			}

			log.Println(b.String())
			// Set up the request object.
			// Perform the request with the client.
			ires, ierr := esapi.UpdateRequest{
				//Index:      "foo-index",
				//DocumentType: "foo_document",
				DocumentID: "e73ad3f5-b370-4c73-86a3-fb63f8caf90c-" + title,
				Body: esutil.NewJSONReader(map[string]interface{}{
					"doc": &a,
				}),
				//Body:       strings.NewReader(b.String()),
				//Body:       bytes.NewReader([]byte(`{
				//"id": "e73ad3f5-b370-4c73-86a3-fb63f8caf90c-`+title+`",
				//"name": "Welcome Foo"
				//}`)),
				//Refresh:    "true",
				Pretty: true,
			}.Do(context.Background(), es)
			if ierr != nil {
				log.Fatalf("Error getting response: %s", ierr)
			}
			defer ires.Body.Close()

			log.Println(ires)
			if ires.IsError() {
				log.Printf("Error updating document ID=%d => %s", i+1, ires)
			} else {
				// Deserialize the response into a map.
				var r map[string]interface{}
				if err := json.NewDecoder(ires.Body).Decode(&r); err != nil {
					log.Printf("Error parsing the response body: %s", err)
				} else {
					jb, _ := json.MarshalIndent(r, "", "  ")
					log.Println(string(jb))
					// Print the response status and indexed document version.
					log.Printf("[%s] %s; version=%d", ires.Status(), r["result"], int(r["_version"].(float64)))
				}
			}

		}(i, title)
	}
	wg.Wait()

	log.Println(strings.Repeat("-", 37))
}

func index(es *elasticsearch.Client) {
	var wg sync.WaitGroup

	for i, title := range []string{"Test One",
		"Test Two",
	} {
		wg.Add(1)

		go func(i int, title string) {
			defer wg.Done()

			// Build the request body.
			var b strings.Builder

			b.WriteString(`{
"id": "e73ad3f5-b370-4c73-86a3-fb63f8caf90c-` + title + `",
"name": "Welcome Foo - updtd ` + title + `"
}`)
			a := struct {
				ID   string `json:"id"`
				Name string `json:"name"`
			}{
				ID:   "e73ad3f5-b370-4c73-86a3-fb63f8caf90c-" + title,
				Name: "Welcome Foo - updtd " + title,
			}

			log.Println(b.String())
			// Set up the request object.
			// Perform the request with the client.
			ires, ierr := esapi.IndexRequest{
				//Index:      "foo-index",
				//DocumentType: "foo_document",
				DocumentID: "e73ad3f5-b370-4c73-86a3-fb63f8caf90c-" + title,
				Body:       esutil.NewJSONReader(&a),

				//Body:       strings.NewReader(b.String()),
				//Body:       bytes.NewReader([]byte(`{
				//"id": "e73ad3f5-b370-4c73-86a3-fb63f8caf90c-`+title+`",
				//"name": "Welcome Foo"
				//}`)),
				//Refresh:    "true",
				Pretty: true,
			}.Do(context.Background(), es)
			if ierr != nil {
				log.Fatalf("Error getting response: %s", ierr)
			}
			defer ires.Body.Close()

			if ires.IsError() {
				log.Printf("Error indexing document ID=%d => %s", i+1, ires)
			} else {
				// Deserialize the response into a map.
				var r map[string]interface{}
				if err := json.NewDecoder(ires.Body).Decode(&r); err != nil {
					log.Printf("Error parsing the response body: %s", err)
				} else {
					jb, _ := json.MarshalIndent(r, "", "  ")
					log.Println(string(jb))
					// Print the response status and indexed document version.
					log.Printf("[%s] %s; version=%d", ires.Status(), r["result"], int(r["_version"].(float64)))
				}
			}

		}(i, title)
	}
	wg.Wait()

	log.Println(strings.Repeat("-", 37))
}

func search(es *elasticsearch.Client) {
	var r map[string]interface{}

	query := map[string]interface{}{
		"query": map[string]interface{}{
			"match": map[string]interface{}{
				//"foo.name": "Popular",
				"id": "-Test ",
			},
		},
	}
	qb, _ := json.MarshalIndent(query, "", "  ")
	log.Println(string(qb))
	// Perform the search request.
	res, err := es.Search(
		es.Search.WithContext(context.Background()),
		es.Search.WithIndex("foo-index"),
		es.Search.WithDocumentType("foo_document"),
		es.Search.WithBody(esutil.NewJSONReader(&query)),
		//es.Search.WithTrackTotalHits(true),
		es.Search.WithPretty(),
	)
	if err != nil {
		log.Fatalf("Error getting response: %s", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		log.Printf("Error searching document with query=%v: \n--->>>\n%v", query, res)

		var e map[string]interface{}
		if err := json.NewDecoder(res.Body).Decode(&e); err != nil {
			log.Fatalf("Error parsing the response body: %s", err)
		} else {
			// Print the response status and error information.
			log.Fatalf("[%s] %s: %s",
				res.Status(),
				e["error"].(map[string]interface{})["type"],
				e["error"].(map[string]interface{})["reason"],
			)
		}
	}

	if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
		log.Fatalf("Error parsing the response body: %s", err)
	}
	// Print the response status, number of results, and request duration.
	// Print the ID and document source for each hit.
	//jb, _ := json.MarshalIndent(r, "", "  ")
	//log.Println(string(jb))
	log.Printf(
		"[%s] %d hits; took: %dms",
		res.Status(),
		int(r["hits"].(map[string]interface{})["total"].(map[string]interface{})["value"].(float64)),
		int(r["took"].(float64)),
	)
	for _, hit := range r["hits"].(map[string]interface{})["hits"].([]interface{}) {
		log.Printf(" * ID=%s ------- name=%s", hit.(map[string]interface{})["_source"].(map[string]interface{})["id"],
			hit.(map[string]interface{})["_source"].(map[string]interface{})["name"])
	}

	log.Println(strings.Repeat("=", 37))
}

func putAlias(es *elasticsearch.Client) {
	fmt.Println("+++++++++++++ put alias ++++++++++")
	// Build the request body.
	a := map[string]interface{}{
		"actions": []map[string]interface{}{
			{
				"add": map[string]interface{}{
					//"index": "foo-index",
					"alias": "alias3",
				},
			},
		},
	}

	// Set up the request object.
	// Perform the request with the client.
	ires, ierr := esapi.IndicesPutAliasRequest{
		Index: []string{"foo-index"},
		Body:  esutil.NewJSONReader(&a),
		//Body:       strings.NewReader(b.String()),
		//Body:       bytes.NewReader([]byte(`{
		Pretty: true,
	}.Do(context.Background(), es)
	if ierr != nil {
		log.Fatalf("Error getting response: %s", ierr)
	}
	defer ires.Body.Close()

	if ires.IsError() {
		log.Printf("Error put aliasing => %s", ires)
	}

	log.Println(strings.Repeat("-", 37))
}

func putIndex(es *elasticsearch.Client) {
	fmt.Println("+++++++++++++ put index ++++++++++")
	// Build the request body.
	a := map[string]interface{}{
		"settings": map[string]interface{}{
			"number_of_shards":   2,
			"number_of_replicas": 2,
		},
		"mappings": map[string]interface{}{
			"properties": map[string]interface{}{
				"field1": map[string]interface{}{"type": "text"},
			},
		},
	}

	// Set up the request object.
	// Perform the request with the client.
	ires, ierr := esapi.IndicesCreateRequest{
		Index: "test-index",
		Body:  esutil.NewJSONReader(&a),
		//Body:       strings.NewReader(b.String()),
		//Body:       bytes.NewReader([]byte(`{
		Pretty: true,
	}.Do(context.Background(), es)
	if ierr != nil {
		log.Fatalf("Error getting response: %s", ierr)
	}
	defer ires.Body.Close()

	if ires.IsError() {
		log.Printf("Error create index => %s", ires)
	}

	log.Println(strings.Repeat("-", 37))
}

func deleteIndex(es *elasticsearch.Client) {
	fmt.Println("+++++++++++++ delete index ++++++++++")
	// Set up the request object.
	// Perform the request with the client.
	ires, ierr := esapi.IndicesDeleteRequest{
		Index: []string{"test-index"},
		//AllowNoIndices:    nil,
		//ExpandWildcards:   "",
		IgnoreUnavailable: func(v bool) *bool { return &v }(true),
		//MasterTimeout:     0,
		//Timeout:           0,
		//Body:       strings.NewReader(b.String()),
		//Body:       bytes.NewReader([]byte(`{
		Pretty: true,
		//Human:      false,
		//ErrorTrace: false,
		//FilterPath: nil,
		//Header:     nil,
	}.Do(context.Background(), es)
	if ierr != nil {
		log.Fatalf("Error getting response: %s", ierr)
	}
	defer ires.Body.Close()

	if ires.IsError() {
		log.Printf("Error create index => %s", ires)
	}

	log.Println(strings.Repeat("-", 37))
}

func main() {
	log.SetFlags(0)

	var r map[string]interface{}

	// Initialize a client with the default settings.
	//
	// An `ELASTICSEARCH_URL` environment variable will be used when exported.
	//
	//es, err := elasticsearch.NewDefaultClient()
	//if err != nil {
	//	log.Fatalf("Error creating the client: %s", err)
	//}
	es, err := elasticsearch.NewClient(elasticsearch.Config{
		Logger: &estransport.TextLogger{Output: os.Stdout},
	})
	if err != nil {
		log.Fatalf("Error creating the client: %s", err)
	}
	//run(es, "Text")

	// 1. Get cluster info
	//
	res, err := es.Info()
	if err != nil {
		log.Fatalf("Error getting response: %s", err)
	}
	// Check response status
	if res.IsError() {
		log.Fatalf("Error: %s", res.String())
	}
	// Deserialize the response into a map.
	if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
		log.Fatalf("Error parsing the response body: %s", err)
	}
	// Print client and server version numbers.
	log.Printf("Client: %s", elasticsearch.Version)
	log.Printf("Server: %s", r["version"].(map[string]interface{})["number"])
	log.Println(strings.Repeat("~", 37))

	parts := strings.Split(actions, ",")
	for i := range parts {
		switch parts[i] {
		case "create":
			create(es)
		case "index":
			index(es)
		case "update":
			update(es)
		case "search":
			search(es)
		case "delete":
			delete(es)
		case "put-alias":
			putAlias(es)
		case "put-index":
			putIndex(es)
		case "del-index":
			deleteIndex(es)
		}
	}
}
