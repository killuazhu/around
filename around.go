package main

import (
	"cloud.google.com/go/bigtable"
	"cloud.google.com/go/storage"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"reflect"
	"strconv"

	//"github.com/olivere/elastic"
	"github.com/pborman/uuid"
	elastic "gopkg.in/olivere/elastic.v3"

	"github.com/auth0/go-jwt-middleware"
	"github.com/dgrijalva/jwt-go"
	"github.com/gorilla/mux"

	"github.com/go-redis/redis"
)

type Location struct {
	Lat float64 `json:"lat"`
	Lon float64 `json:"lon"`
}

type Post struct {
	User     string   `jason:"user"`
	Message  string   `json:"message"`
	Location Location `json:"location"`
	Url      string   `json:"url"`
}

/*
{
       user: "jack",
       message: "hello",
       location :
       {
              lat : 100,
              lon : 10
       }
}

自动映射 `jason:"lat"` annotation
*/

const (
	DISTANCE        = "200km"
	INDEX           = "around"
	TYPE            = "post"
	ES_URL          = "http://104.196.68.65:9200"
	BUCKET_NAME     = "post-image-195403"
	PROJECT_ID      = "kyle-gae-195403"
	BT_INSTANCE     = "around-post"
	ENABLE_MEMCACHE = true
	REDIS_URL       = "redis-15461.c1.us-central1-2.gce.cloud.redislabs.com:15461"
	// update redis password
	REDIS_PASS      = ""
)

var mySigningKey = []byte("kyle_secret")

func main() {

	// createIndex()

	fmt.Println("started-service")
	// http.HandleFunc("/", handlerRoot)
	// http.HandleFunc("/post", handlerPost)
	// //http.HandleFunc("/search", handlerSearch)
	// http.HandleFunc("/search", handlerSearchEs)
	// log.Fatal(http.ListenAndServe(":8080", nil))

	r := mux.NewRouter()

	var jwtMiddleware = jwtmiddleware.New(jwtmiddleware.Options{
		ValidationKeyGetter: func(token *jwt.Token) (interface{}, error) {
			return mySigningKey, nil
		},
		SigningMethod: jwt.SigningMethodHS256,
	})

	r.Handle("/post", jwtMiddleware.Handler(http.HandlerFunc(handlerPost))).Methods("POST")
	r.Handle("/search", jwtMiddleware.Handler(http.HandlerFunc(handlerSearchEs))).Methods("GET")
	r.Handle("/login", http.HandlerFunc(loginHandler)).Methods("POST")
	r.Handle("/signup", http.HandlerFunc(signupHandler)).Methods("POST")

	http.Handle("/", r)
	log.Fatal(http.ListenAndServe(":8080", nil))

}

func createIndex() {
	// Create a client
	client, err := elastic.NewClient(elastic.SetURL(ES_URL), elastic.SetSniff(false))
	if err != nil {
		panic(err)
		return
	}

	// Use the IndexExists service to check if a specified index exists.
	exists, err := client.IndexExists(INDEX).Do()
	if err != nil {
		panic(err)
	}
	if !exists {
		// Create a new index.
		mapping := `{
                    "mappings":{
                           "post":{
                                  "properties":{
                                         "location":{
                                                "type":"geo_point"
                                         }
                                  }
                           }
                    }
             }
             `
		_, err := client.CreateIndex(INDEX).Body(mapping).Do()
		if err != nil {
			// Handle error
			panic(err)
		}
	}
}

func handlerRoot(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	fmt.Fprint(w, "Hello World!")
}

func handlerSearchEs(w http.ResponseWriter, r *http.Request) {
	fmt.Println("Received one request for search")
	lat, _ := strconv.ParseFloat(r.URL.Query().Get("lat"), 64)
	lon, _ := strconv.ParseFloat(r.URL.Query().Get("lon"), 64)

	// range is optional
	ran := DISTANCE
	if val := r.URL.Query().Get("range"); val != "" {
		ran = val + "km"
	}
	fmt.Printf("Search received: %f %f %s\n", lat, lon, ran)

	// Create a client
	client, err := elastic.NewClient(elastic.SetURL(ES_URL), elastic.SetSniff(false))
	if err != nil {
		panic(err)
		return
	}

	// query from Redis first
	key := r.URL.Query().Get("lat") + ":" + r.URL.Query().Get("lon") + ":" + ran
	if ENABLE_MEMCACHE {
		rs_client := redis.NewClient(&redis.Options{
			Addr:     REDIS_URL,
			Password: REDIS_PASS, // no password set
			DB:       0,          // use default DB
		})

		val, err := rs_client.Get(key).Result()
		if err != nil {
			fmt.Printf("Redis cannot find the key %s as %v.\n", key, err)
		} else {
			fmt.Printf("Redis find the key %s.\n", key)
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte(val))
			return
		}
	}

	// Define geo distance query as specified in
	// https://www.elastic.co/guide/en/elasticsearch/reference/5.2/query-dsl-geo-distance-query.html
	q := elastic.NewGeoDistanceQuery("location")
	q = q.Distance(ran).Lat(lat).Lon(lon)

	// Some delay may range from seconds to minutes. So if you don't get enough results. Try it later.
	searchResult, err := client.Search().
		Index(INDEX).
		Query(q).
		Pretty(true).
		Do()
	if err != nil {
		// Handle error
		panic(err)
	}

	// searchResult is of type SearchResult and returns hits, suggestions,
	// and all kinds of other information from Elasticsearch.
	fmt.Printf("Query took %d milliseconds\n", searchResult.TookInMillis)
	// TotalHits is another convenience function that works even when something goes wrong.
	fmt.Printf("Found a total of %d post\n", searchResult.TotalHits())

	// Each is a convenience function that iterates over hits in a search result.
	// It makes sure you don't need to check for nil values in the response.
	// However, it ignores errors in serialization.
	var typ Post
	var ps []Post
	for _, item := range searchResult.Each(reflect.TypeOf(typ)) { // instance of
		p := item.(Post) // p = (Post) item
		fmt.Printf("Post by %s: %s at lat %v and lon %v\n", p.User, p.Message, p.Location.Lat, p.Location.Lon)
		// TODO(student homework): Perform filtering based on keywords such as web spam etc.
		ps = append(ps, p)

	}
	js, err := json.Marshal(ps)
	if err != nil {
		panic(err)
		return
	}

	// write to redis
	if ENABLE_MEMCACHE {
		//Student question: please complete the Set here
		rs_client := redis.NewClient(&redis.Options{
			Addr:     REDIS_URL,
			Password: REDIS_PASS, // no password set
			DB:       0,          // use default DB
		})

		err := rs_client.Set(key, js, 0).Err()
		if err != nil {
			panic(err)
			fmt.Printf("Cannot write to Redis with key %s as %v.\n", key, err)
		} else {
			fmt.Printf("Redis write is successful with the key %s.\n", key)
		}
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Write(js)

}

func handlerSearch(w http.ResponseWriter, r *http.Request) {
	fmt.Println("Received a search request")
	lat := r.URL.Query().Get("lat")
	lt, _ := strconv.ParseFloat(lat, 64)
	lon := r.URL.Query().Get("lon")
	ln, _ := strconv.ParseFloat(lon, 64)

	ran := DISTANCE
	if val := r.URL.Query().Get("range"); val != "" {
		ran = val + "km"
	}

	fmt.Printf("Received request %f %f %s\n", lt, ln, ran)

	fmt.Println("range is ", ran)
	p := &Post{
		User:    "1111",
		Message: "This is whatever",
		Location: Location{
			Lat: lt,
			Lon: ln,
		},
	}

	js, err := json.Marshal(p)
	if err != nil {
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(js)

	fmt.Fprintf(w, "Lat is %s Lon is %s\n", lat, lon)
	fmt.Fprintf(w, "Lat is %f Lon is %f\n", lt, ln)
}

func handlerPost(w http.ResponseWriter, r *http.Request) {
	fmt.Println("Received a request for post")
	// decoder := json.NewDecoder(r.Body)
	// var p Post
	// if err := decoder.Decode(&p); err != nil {
	// 	panic(err)
	// 	return
	// }

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type,Authorization")

	// handle authentication
	user := r.Context().Value("user")
	claims := user.(*jwt.Token).Claims
	username := claims.(jwt.MapClaims)["username"]

	// 32 << 20 is the maxMemory param for ParseMultipartForm, equals to 32MB (1MB = 1024 * 1024 bytes = 2^20 bytes)
	// After you call ParseMultipartForm, the file will be saved in the server memory with maxMemory size.
	// If the file size is larger than maxMemory, the rest of the data will be saved in a system temporary file.
	r.ParseMultipartForm(32 << 20)

	// Parse from form data.
	fmt.Printf("Received one post request message %s\n", r.FormValue("message"))
	lat, _ := strconv.ParseFloat(r.FormValue("lat"), 64)
	lon, _ := strconv.ParseFloat(r.FormValue("lon"), 64)
	p := &Post{
		User:    username.(string),
		Message: r.FormValue("message"),
		Location: Location{
			Lat: lat,
			Lon: lon,
		},
	}

	id := uuid.New()

	file, _, err := r.FormFile("image")
	if err != nil {
		http.Error(w, "Image is not available", http.StatusInternalServerError)
		fmt.Printf("Image is not available %v.\n", err)
		return
	}
	defer file.Close()

	ctx := context.Background()

	// replace it with your real bucket name.
	_, attrs, err := saveToGCS(ctx, file, BUCKET_NAME, id)
	if err != nil {
		http.Error(w, "GCS is not setup", http.StatusInternalServerError)
		fmt.Printf("GCS is not setup %v\n", err)
		return
	}

	// Update the media link after saving to GCS.
	p.Url = attrs.MediaLink

	// Save to ES.
	saveToES(p, id)

	// Save to BigTable.
	// saveToBigTable(p, id)

	fmt.Println("The post message is ", p.Message)

	js, err := json.Marshal(p)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(201)
	w.Write(js)
}

func saveToBigTable(p *Post, id string) {
	ctx := context.Background()
	// you must update project name here
	bt_client, err := bigtable.NewClient(ctx, PROJECT_ID, BT_INSTANCE)
	if err != nil {
		panic(err)
		return
	}

	tbl := bt_client.Open("post")
	mut := bigtable.NewMutation()
	t := bigtable.Now()

	mut.Set("post", "user", t, []byte(p.User))
	mut.Set("post", "message", t, []byte(p.Message))
	mut.Set("location", "lat", t, []byte(strconv.FormatFloat(p.Location.Lat, 'f', -1, 64)))
	mut.Set("location", "lon", t, []byte(strconv.FormatFloat(p.Location.Lon, 'f', -1, 64)))

	err = tbl.Apply(ctx, id, mut)
	if err != nil {
		panic(err)
		return
	}
	fmt.Printf("Post is saved to BigTable: %s\n", p.Message)
}

func saveToGCS(ctx context.Context, file io.Reader, bucketName, name string) (*storage.ObjectHandle, *storage.ObjectAttrs, error) {
	// Creates a client.
	client, err := storage.NewClient(ctx)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	// Creates a Bucket instance.
	bucket := client.Bucket(bucketName)

	// Creates the new bucket.
	// if err := bucket.Create(ctx, PROJECT_ID, nil); err != nil {
	// log.Fatalf("Failed to create bucket: %v", err)
	// }

	// fmt.Printf("Bucket %v created.\n", bucketName)

	if _, err = bucket.Attrs(ctx); err != nil {
		return nil, nil, err
	}

	obj := bucket.Object(name)

	wc := obj.NewWriter(ctx)
	if _, err = io.Copy(wc, file); err != nil {
		return nil, nil, err
	}
	if err := wc.Close(); err != nil {
		return nil, nil, err
	}

	if err := obj.ACL().Set(ctx, storage.AllUsers, storage.RoleReader); err != nil {
		return nil, nil, err
	}

	attrs, err := obj.Attrs(ctx)
	fmt.Printf("Post is saved to GCS: %s\n", attrs.MediaLink)
	return obj, attrs, err
}

func saveToES(p *Post, id string) {
	// Create a client
	esClient, err := elastic.NewClient(elastic.SetURL(ES_URL), elastic.SetSniff(false))
	if err != nil {
		panic(err)
		return
	}

	// Save it to index
	_, err = esClient.Index().
		Index(INDEX).
		Type(TYPE).
		Id(id).
		BodyJson(p).
		Refresh(true).
		Do()
	if err != nil {
		panic(err)
		return
	}

	fmt.Printf("Post is saved to Index: %s\n", p.Message)

}
