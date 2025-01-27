package main
import (
	"context"
	// "errors"

	
	"log"
	"os"


	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)
func NewMongoClient() *mongo.Client {
	clientOptions := options.Client().ApplyURI(os.Getenv("mongo_uri"))
	client, err := mongo.Connect(context.TODO(), clientOptions)
	if err != nil {
		log.Println("Error connecting to MongoDB:", err)
		return nil
	}

	err = client.Ping(context.TODO(), nil)
	if err != nil {
		log.Println("Error pinging MongoDB:", err)
		return nil
	}

	return client
}
