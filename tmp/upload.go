package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
)

// 1MB
const (
	MAX_MEMORY = 1 * 1024 * 1024
	STATIC_DIR = "static"
)

func upload(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseMultipartForm(MAX_MEMORY); err != nil {
		log.Println(err)
		http.Error(w, err.Error(), http.StatusForbidden)
	}

	for key, value := range r.MultipartForm.Value {
		fmt.Fprintf(w, "%s:%s ", key, value)
		log.Printf("%s:%s", key, value)
	}

	for _, fileHeaders := range r.MultipartForm.File {
		for _, fileHeader := range fileHeaders {
			file, _ := fileHeader.Open()
			path := fmt.Sprintf("%s/%s", STATIC_DIR, fileHeader.Filename)
			buf, _ := ioutil.ReadAll(file)
			ioutil.WriteFile(path, buf, os.ModePerm)
		}
	}
}

func main() {
	http.HandleFunc("/upload", upload)
	os.MkdirAll(STATIC_DIR, os.ModePerm)
	http.Handle("/", http.FileServer(http.Dir(STATIC_DIR)))
	log.Fatal(http.ListenAndServe(":8080", nil))
}
