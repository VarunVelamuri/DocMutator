package main

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"time"

	"gopkg.in/couchbase/gocb.v1"
)

var options struct {
	bucket     string
	username   string
	password   string
	kvaddress  string
	numDocs    int
	numThreads int
	xattrs     bool
}

func argParse() {

	flag.StringVar(&options.bucket, "bucket", "default",
		"buckets to connect")
	flag.StringVar(&options.username, "username", "Administrator",
		"Cluster username")
	flag.StringVar(&options.password, "password", "asdasd",
		"Cluster password")
	flag.StringVar(&options.kvaddress, "kvaddress", "127.0.0.1:9000",
		"KV address")
	flag.IntVar(&options.numDocs, "numDocs", 1000,
		"Number of documents that can be populated")
	flag.IntVar(&options.numThreads, "numThreads", 4,
		"Number of threads to populate the docs")
	flag.BoolVar(&options.xattrs, "xattrs", false,
		"Generate with xattrs")
	flag.Parse()
}

func usage() {
	fmt.Fprintf(os.Stderr, "Usage : %s [OPTIONS] <addr> \n", os.Args[0])
	flag.PrintDefaults()
}

func main() {
	argParse()
	fmt.Printf("Username: %v, password: %v, numDocs: %v\n", options.username, options.password, options.numDocs)

	populateDocs()
}

func genKeyValues(numDocs int) []map[string]interface{} {
	keyValues := make([]map[string]interface{}, options.numThreads)
	seed := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < options.numThreads; i++ {
		keyValues[i] = make(map[string]interface{})
	}

	bodySize := 100000

	for i := 0; i < numDocs; i++ {

		docId := String(32, seed)
		doc := generateJson(String(bodySize, seed), seed)
		keyValues[i%options.numThreads][docId] = doc
	}

	return keyValues
}

func populateDocs() {
	docs := genKeyValues(options.numDocs)
	var wg sync.WaitGroup
	for i := 0; i < options.numThreads; i++ {
		wg.Add(1)

		go func(docs map[string]interface{}) {
			defer wg.Done()
			cluster, _ := gocb.Connect("http://" + options.kvaddress)
			cluster.Authenticate(gocb.PasswordAuthenticator{
				Username: options.username,
				Password: options.password,
			})

			bucket, _ := cluster.OpenBucket(options.bucket, "")

			for {
				for key, value := range docs {
					//if bytes, err := GetBytes(value); err == nil {
					bucket.Insert(key, value, 0)
					//bucket.MutateIn(key, 0, 0).UpsertEx("_sync.rev", "1000", gocb.SubdocFlagXattr|gocb.SubdocFlagCreatePath).Execute()
					//bucket.MutateIn(key, 0, 0).UpsertEx("_sync.sequence", "1000", gocb.SubdocFlagXattr|gocb.SubdocFlagCreatePath).Execute()
					//}
					//tc.HandleError(err, "setRaw")
					time.Sleep(10 * time.Millisecond)
				}
			}
		}(docs[i])
	}
	wg.Wait()
}
