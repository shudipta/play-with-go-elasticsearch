package main

import (
	"context"
	"encoding/json"
	"flag"
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

func deletebyquery(es *elasticsearch.Client)  {
	// Delete by query
	res, err := es.DeleteByQuery(
		[]string{"collection-index"},
		esutil.NewJSONReader(map[string]interface{}{
			"query": map[string]interface{}{
				"match": map[string]interface{}{
					//"collection.name": "Popular",
					"name": "updtd",
				},
			},
		}),
		es.DeleteByQuery.WithContext(context.Background()),
		es.DeleteByQuery.WithDocumentType("collection_document"),
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
				Index:      "collection-index",
				DocumentType: "collection_document",
				DocumentID: "e73ad3f5-b370-4c73-86a3-fb63f8caf90c-"+title,
				//IfPrimaryTerm:       nil,
				//IfSeqNo:             nil,
				//Refresh:             "",
				//Routing:             "",
				//Timeout:             0,
				//Version:             nil,
				//VersionType:         "",
				//WaitForActiveShards: "",
				Pretty:              true,
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
"id": "e73ad3f5-b370-4c73-86a3-fb63f8caf90c-`+title+`",
"collection_id": "713e5117-cc65-438f-8744-96e951e1412c",
"restaurant_id": "d81fc30a-8919-4ecc-a200-ae9593d364ef",
"collection": { "name": "Popular","icon": "http://food-m.p-stageenv.xyz/food/icon_ic_Most%20Popular.png","priority": 0,"cover": "http://food-m.p-stageenv.xyz/food/col_cover_Most%20Popular.png","banner": "http://food-m.p-stageenv.xyz/food/col_banner_Collection-Banner-Most%20Popular.png","is_visible": true},
"location": { "lat": 23.789301,"lon": 90.403732},
"cuisines": [ { "id": "611d287c-786a-4971-93d6-f8b173f84989","name": "Fast Food"},{ "id": "0a67c68e-ae86-45c1-be0d-344432002f73","name": "Chinese"}],
"schedules": [ { "opening": { "day": 2,"time": 43200},"closing": { "day": 2,"time": 79200}},{ "opening": { "day": 7,"time": 43200},"closing": { "day": 7,"time": 79200}},{ "opening": { "day": 4,"time": 43200},"closing": { "day": 4,"time": 79200}},{ "opening": { "day": 6,"time": 43200},"closing": { "day": 6,"time": 79200}},{ "opening": { "day": 5,"time": 43200},"closing": { "day": 5,"time": 79200}},{ "opening": { "day": 1,"time": 43200},"closing": { "day": 1,"time": 79200}},{ "opening": { "day": 3,"time": 43200},"closing": { "day": 3,"time": 79200}}],
"address": "House-24, Road-8, Block-F, Banani",
"name": "Foodiz Avenue",
"logo": null,
"min_delivery_time": "60 min",
"min_order_value": 50,
"min_delivery_fee": 60,
"visible_in_app": true,
"is_flagged": false,
"discount_type": "PERCENTAGE",
"discount": 0,
"search_score": 2,
"sort_order": 7,
"banner": "http://food-m.p-stageenv.xyz/food/29_banner.jpg",
"average_rating": 5,
"free_delivery": false,
"radius_promotion": false,
"delivery_fee": 60,
"discounted_delivery_fee": 19,
"rating_count": 19,
"accepting_orders": true,
"is_tong": false,
"is_pharma": false,
"custom_radius": null,
"badges": null
}`)
			//b.WriteString(title)
			//b.WriteString(`"}`)


			// Set up the request object.
			req := esapi.CreateRequest{
				Index:      "collection-index",
				DocumentType: "collection_document",
				DocumentID: "e73ad3f5-b370-4c73-86a3-fb63f8caf90c-"+title,
				Body:       strings.NewReader(b.String()),
				//Refresh:    "true",

			}

			// Perform the request with the client.
			ires, ierr := req.Do(context.Background(), es)
			if ierr != nil {
				log.Fatalf("Error getting response: %s", ierr)
			}
			defer ires.Body.Close()

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
"id": "e73ad3f5-b370-4c73-86a3-fb63f8caf90c-`+title+`",
"name": "Foodiz Avenue - updtd `+title+`"
}`)
			a := struct{
				ID string `json:"id"`
				Name string `json:"name"`
			}{
				ID:   "e73ad3f5-b370-4c73-86a3-fb63f8caf90c-"+title,
				Name: "Foodiz Avenue - updtd "+title,
			}

			log.Println(b.String())
			// Set up the request object.
			// Perform the request with the client.
			ires, ierr := esapi.UpdateRequest{
				Index:      "collection-index",
				DocumentType: "collection_document",
				DocumentID: "e73ad3f5-b370-4c73-86a3-fb63f8caf90c-"+title,
				Body:       esutil.NewJSONReader(map[string]interface{}{
					"doc": &a,
				}),
				//Body:       strings.NewReader(b.String()),
				//Body:       bytes.NewReader([]byte(`{
//"id": "e73ad3f5-b370-4c73-86a3-fb63f8caf90c-`+title+`",
//"name": "Foodiz Avenue"
//}`)),
				//Refresh:    "true",
			}.Do(context.Background(), es)
			if ierr != nil {
				log.Fatalf("Error getting response: %s", ierr)
			}
			defer ires.Body.Close()

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
"id": "e73ad3f5-b370-4c73-86a3-fb63f8caf90c-`+title+`",
"name": "Foodiz Avenue - updtd `+title+`"
}`)
			a := struct{
				ID string `json:"id"`
				Name string `json:"name"`
			}{
				ID:   "e73ad3f5-b370-4c73-86a3-fb63f8caf90c-"+title,
				Name: "Foodiz Avenue - updtd "+title,
			}

			log.Println(b.String())
			// Set up the request object.
			// Perform the request with the client.
			ires, ierr := esapi.IndexRequest{
				Index:      "collection-index",
				DocumentType: "collection_document",
				DocumentID: "e73ad3f5-b370-4c73-86a3-fb63f8caf90c-"+title,
				Body:       esutil.NewJSONReader(&a),

				//Body:       strings.NewReader(b.String()),
				//Body:       bytes.NewReader([]byte(`{
//"id": "e73ad3f5-b370-4c73-86a3-fb63f8caf90c-`+title+`",
//"name": "Foodiz Avenue"
//}`)),
				//Refresh:    "true",
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
				//"collection.name": "Popular",
				"id": "-Test ",
			},
		},
	}
	qb, _ := json.MarshalIndent(query, "", "  ")
	log.Println(string(qb))
	// Perform the search request.
	res, err := es.Search(
		es.Search.WithContext(context.Background()),
		es.Search.WithIndex("collection-index"),
		es.Search.WithDocumentType("collection_document"),
		es.Search.WithBody(esutil.NewJSONReader(&query)),
		//es.Search.WithTrackTotalHits(true),
		es.Search.WithPretty(),
	)
	if err != nil {
		log.Fatalf("Error getting response: %s", err)
	}
	defer res.Body.Close()

	if res.IsError() {
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

func main() {
	log.SetFlags(0)

	var r  map[string]interface{}

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
		}
	}
}
