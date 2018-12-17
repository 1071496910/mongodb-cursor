package main

import (
	"context"
	"flag"
	"log"
	"time"

	"github.com/mongodb/mongo-go-driver/bson"
	"github.com/mongodb/mongo-go-driver/mongo"
)

//{ "_id" : "3506a4d3e87146e88ab97dde57fc1a67z", "bizId" : "testbiz", "storageId" : 1, "size" : 9, "files" : [ { "model" : "W943H476.PNG", "md5" : "39421862af1b71bcea946352886bb98a", "path" : "group5@M01/FE/3A/rBDbFFwSL9iAVO0EAAAnEDUJ9yE547.PNG", "size" : 9 } ], "updateTime" : ISODate("2018-12-13T09:33:04.807Z") }

type Doc struct {
	BIZ   string `bson:"bizId"`
	ID    string `bson:"_id"`
	Files []File `bson:"files"`
}

type File struct {
	Size int `bson:"size"`
}

var (
	addr    = flag.String("addr", "172.16.177.147:21001", "addr to conn")
	biz     = flag.String("biz", "", "biz to summary")
	timeout = flag.Duration("timeout", 5*time.Minute, "deal deadline")
)

func main() {
	flag.Parse()

	connStr := "mongodb://" + *addr

	where := &bson.M{}
	if *biz != "" {
		where = &bson.M{"bizId": *biz}
	}

	var summary = make(map[string]int)
	client, err := mongo.NewClient(connStr)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	log.Printf("Connecting [%v] ...\n", connStr)
	err = client.Connect(ctx)
	defer cancel()

	log.Printf("Using collection [%v/%v]\n", "meizu-image", "image")
	collection := client.Database("meizu-image").Collection("image")

	ctx, cancel = context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	if *biz != "" {
		log.Printf("Figuring biz [%v] ...\n", *biz)
	}

	cur, err := collection.Find(ctx, where)
	if err != nil {
		log.Fatal(err)
	}
	defer cur.Close(ctx)
	for cur.Next(ctx) {
		var result Doc
		//var result bson.M
		err := cur.Decode(&result)
		if err != nil {
			log.Fatal(err)
		}
		//log.Println("Get Doc:", result)
		//id := result["bizId"]
		//size := result["size"]
		//files := result["files"]

		//if err != nil {
		//	log.Fatalln(err)
		//}

		//filesList := files.([]interface{})
		for _, file := range result.Files {
			//log.Println("Get file", file)
			summary[result.BIZ] += file.Size
		}

		//summary[id.(string)] += int(size.(int32))

		// do something with result....
	}
	log.Println("Get result", summary)
	if err := cur.Err(); err != nil {
		log.Fatal(err)
	}
}
