package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"io/ioutil"
	"log"
	"math"
	"os"
	"strings"
	"sync"

	"cloud.google.com/go/storage"
	"github.com/neelance/firestore-export-reader/internal/datastore"
	"github.com/syndtr/goleveldb/leveldb/journal"
	"google.golang.org/api/iterator"
	"google.golang.org/protobuf/proto"
)

func main() {
	if len(os.Args) != 2 {
		log.Fatal("usage: firestore-export-reader [bucket]/[path to kind]")
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
		collection := ""
		id := ""
		for i, e := range pb.GetKey().GetPath().GetElement() {
			if i > 0 {
				collection += "/" + id + "/"
			}
			collection += e.GetType()
			id = e.GetName()
		}

		w.Write([]byte(`{"collection":`))
		writeJSON(w, collection)
		w.Write([]byte(`,"id":`))
		writeJSON(w, id)
		w.Write([]byte(`,"data":`))
	}

	w.Write([]byte("{"))
	currentMultiple := ""
	for i, p := range pb.GetRawProperty() {
		if currentMultiple != "" && p.GetName() != currentMultiple {
			w.Write([]byte("]"))
			currentMultiple = ""
		}

		if i > 0 {
			w.Write([]byte(","))
		}

		if currentMultiple == "" {
			writeJSON(w, p.GetName())
			w.Write([]byte(":"))
		}

		if p.GetMultiple() && p.GetName() != currentMultiple {
			w.Write([]byte("["))
			currentMultiple = p.GetName()
		}

		switch p.GetMeaning() {
		case datastore.Property_ENTITY_PROTO:
			writeObject(w, []byte(p.GetValue().GetStringValue()), false)
		case datastore.Property_GD_WHEN:
			writeJSON(w, p.GetValue().GetInt64Value()/1_000_000)
		default:
			if p.Value.Int64Value != nil {
				writeJSON(w, *p.Value.Int64Value)
			} else if p.Value.BooleanValue != nil {
				writeJSON(w, *p.Value.BooleanValue)
			} else if p.Value.StringValue != nil {
				writeJSON(w, *p.Value.StringValue)
			} else if p.Value.DoubleValue != nil {
				if math.IsNaN(*p.Value.DoubleValue) {
					w.Write([]byte("null"))
				} else {
					writeJSON(w, *p.Value.DoubleValue)
				}
			} else {
				w.Write([]byte("null"))
			}
		}
	}
	if currentMultiple != "" {
		w.Write([]byte("]"))
	}
	w.Write([]byte("}"))

	if toplevel {
		w.Write([]byte("}\n"))
	}
}

func writeJSON(w io.Writer, v interface{}) {
	b, err := json.Marshal(v)
	if err != nil {
		log.Fatal(err)
	}
	w.Write(b)
}
