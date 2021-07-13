package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"io/ioutil"
	"log"
	"os"
	"strings"
	"sync"

	"cloud.google.com/go/storage"
	"github.com/neelance/firestore-backup-reader/internal/datastore"
	"github.com/syndtr/goleveldb/leveldb/journal"
	"google.golang.org/api/iterator"
	"google.golang.org/protobuf/proto"
)

func main() {
	if len(os.Args) != 2 {
		log.Fatal("usage: firestore-backup-reader [bucket]/[path to kind]")
	}
	path := strings.SplitN(os.Args[1], "/", 2)

	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		log.Fatal(err)
	}

	bucket := client.Bucket(path[0])

	objects := make(chan *storage.ObjectHandle)
	go (func() {
		it := bucket.Objects(ctx, &storage.Query{Prefix: path[1] + "/output-"})
		for {
			attrs, err := it.Next()
			if err == iterator.Done {
				break
			}
			if err != nil {
				log.Fatal(err)
			}
			objects <- bucket.Object((attrs.Name))
		}
		close(objects)
	})()

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go (func() {
			for obj := range objects {
				processOutputFile(ctx, obj)
			}
			wg.Done()
		})()
	}
	wg.Wait()
}

func processOutputFile(ctx context.Context, obj *storage.ObjectHandle) {
	r, err := obj.NewReader(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer r.Close()

	journals := journal.NewReader(r, nil, false, true)
	var w bytes.Buffer
	for {
		j, err := journals.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}
		b, err := ioutil.ReadAll(j)
		if err != nil {
			log.Fatal(err)
		}

		w.Reset()
		writeObject(&w, b, true)
		os.Stdout.Write(w.Bytes())
	}
}

func writeObject(w io.Writer, b []byte, toplevel bool) {
	pb := &datastore.EntityProto{}
	if err := proto.Unmarshal(b, pb); err != nil {
		log.Fatal(err)
	}

	if toplevel {
		w.Write([]byte(`{"key":`))
		writeString(w, pb.GetKey().GetPath().GetElement()[0].GetName())
		w.Write([]byte(`,"value":`))
	}

	w.Write([]byte("{"))
	for i, p := range pb.GetRawProperty() {
		if i > 0 {
			w.Write([]byte(","))
		}

		writeString(w, p.GetName())
		w.Write([]byte(":"))

		if p.GetMeaning() == datastore.Property_ENTITY_PROTO {
			writeObject(w, []byte(p.GetValue().GetStringValue()), false)
		} else {
			writeString(w, p.GetValue().GetStringValue())
		}
	}
	w.Write([]byte("}"))

	if toplevel {
		w.Write([]byte("}\n"))
	}
}

func writeString(w io.Writer, s string) {
	b, err := json.Marshal(s)
	if err != nil {
		log.Fatal(err)
	}
	w.Write(b)
}
