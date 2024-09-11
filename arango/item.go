package arango

import (
	"context"
	"fmt"
	"log"
	"time"

	driver "github.com/arangodb/go-driver"
	"github.com/arangodb/go-driver/http"
)

func Init(yamlPath string) error {
	// # Connect to ArangoDB
	conn, err := http.NewConnection(http.ConnectionConfig{
		Endpoints: []string{"http://localhost:8529"},
	})
	if err != nil {
		log.Fatalf("Failed to create connection: %v", err)
	}

	c, err := driver.NewClient(driver.ClientConfig{
		Connection:     conn,
		Authentication: driver.BasicAuthentication("root", "mypassword"),
	})
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}

	// # Open a database
	ctx := context.Background()
	db, err := c.Database(ctx, "_system")
	if err != nil {
		log.Fatalf("Failed to open database: %v", err)
	}

	fmt.Println(db.Name())

	// # Create a database "mydb" with check
	if exists, err := c.DatabaseExists(ctx, "mydb"); err != nil {
		log.Fatalf("Failed to check for database: %v", err)
	} else {
		if exists {
			fmt.Println("Database exists")
		} else {
			fmt.Println("Database does not exist")
			// create a database
			db, err = c.CreateDatabase(ctx, "mydb", nil)
			if err != nil {
				log.Fatalf("Failed to create database: %v", err)
			}
			fmt.Println(db.Name())
		}
	}

	// # open the database
	db, err = c.Database(ctx, "mydb")
	if err != nil {
		log.Fatalf("Failed to open database: %v", err)
	}

	// # create a collection "mycol" with check
	if exists, err := db.CollectionExists(ctx, "mycol"); err != nil {
		log.Fatalf("Failed to check for collection: %v", err)
	} else {
		if exists {
			fmt.Println("Collection exists")
		} else {
			fmt.Println("Collection does not exist")
			// create a collection
			col, err := db.CreateCollection(ctx, "mycol", nil)
			if err != nil {
				log.Fatalf("Failed to create collection: %v", err)
			}
			fmt.Println(col.Name())
		}
	}

	// # open a collection
	col, err := db.Collection(ctx, "mycol")
	if err != nil {
		log.Fatalf("Failed to open collection: %v", err)
	}

	fmt.Println(col.Name())

	// # create a document
	doc := map[string]interface{}{
		"key": "value",
	}

	meta, err := col.CreateDocument(ctx, doc)
	if err != nil {
		log.Fatalf("Failed to create document: %v", err)
	}

	fmt.Println(meta.Key)

	// # check if a document exists
	exists, err := col.DocumentExists(ctx, meta.Key)
	if err != nil {
		log.Fatalf("Failed to check for document: %v", err)
	}
	if exists {
		fmt.Println("Document exists")
	} else {
		fmt.Println("Document does not exist")
	}

	// # read a document
	var readDoc map[string]interface{}
	meta, err = col.ReadDocument(ctx, meta.Key, &readDoc)
	if err != nil {
		log.Fatalf("Failed to read document: %v", err)
	}

	fmt.Println(meta.Key)

	// # read a document with an explicit revision
	type MyDocument struct {
		Key string `json:"_key"`
		Rev string `json:"_rev"`
	}
	var docc MyDocument

	revCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	meta, err = col.ReadDocument(revCtx, meta.Key, &docc)
	if err != nil {
		// handle error
		fmt.Println(err)
	}

	fmt.Println(meta.Key)

	// remove the document
	meta, err = col.RemoveDocument(ctx, meta.Key)
	if err != nil {
		log.Fatalf("Failed to remove document: %v", err)
	}

	fmt.Println(meta.Key)

	// ctx := context.Background()
	query := "FOR d IN mycol LIMIT 10 RETURN d"
	cursor, err := db.Query(ctx, query, nil)
	if err != nil {
		// handle error
	}
	defer cursor.Close()
	for {
		var doc MyDocument
		meta, err := cursor.ReadDocument(ctx, &doc)
		if driver.IsNoMoreDocuments(err) {
			break
		} else if err != nil {
			// handle other errors
		}
		fmt.Printf("Got doc with key '%s' from query\n", meta.Key)
	}

	ctx = driver.WithQueryCount(context.Background())
	query = "FOR d IN mycol RETURN d"
	cursor, err = db.Query(ctx, query, nil)
	if err != nil {
		// handle error
	}
	defer cursor.Close()
	fmt.Printf("Query yields %d documents\n", cursor.Count())

	ctx = driver.WithQueryCount(context.Background())
	query = "FOR d IN mycol FILTER d._key == @myVar RETURN d"
	bindVars := map[string]interface{}{
		"myVar": "2255",
	}
	cursor, err = db.Query(ctx, query, bindVars)
	if err != nil {
		// handle error
	}
	defer cursor.Close()
	fmt.Printf("Query yields %d documents\n", cursor.Count())

	return nil

}
