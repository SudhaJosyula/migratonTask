package main

import (
	"context"
	"log"

	"os"

	openfga "github.com/openfga/go-sdk"
	. "github.com/openfga/go-sdk/client"
)

var (
	storeID              = os.Getenv("OPENFGA_STORE_ID")
	authorizationModelID = os.Getenv("OPENFGA_AUTHORIZATION_MODEL_ID")
	baseURL              = os.Getenv("OPENFGA_API_URL")
)

// FgaClient is a wrapper around the OpenFgaClient
type FgaClient struct {
	c *OpenFgaClient
}

// NewAuthorizationClient creates a new FGA client
func NewAuthorizationClient() *FgaClient {
	client, err := NewSdkClient(&ClientConfiguration{
		ApiUrl:               baseURL,
		StoreId:              storeID,
		AuthorizationModelId: authorizationModelID,
	})
	if err != nil {
		// log.Fatalf("Failed to create FGA client: %v", err)
	}
	return &FgaClient{client}
}

func (fga *FgaClient) WriteRelation(writes []openfga.TupleKey) {
	body := ClientWriteRequest{
		Writes: writes,
	}

	options := ClientWriteOptions{
		// You can rely on the model id set in the configuration or override it for this specific request
		AuthorizationModelId: openfga.PtrString(authorizationModelID),
		// You can rely on the store id set in the configuration or override it for this specific request
		StoreId: openfga.PtrString(storeID),
		Transaction: &TransactionOptions{
			Disable:             true,
			MaxParallelRequests: 5,  // Maximum number of requests to issue in parallel
			MaxPerChunk:         20, // Maximum number of requests to be sent in a transaction in a particular chunk
		},
	}
	data, err := fga.c.Write(context.Background()).Body(body).Options(options).Execute()
	if err != nil {
		log.Println("Error writing data: ", err)
		return
	}
	for _, item := range data.Writes {
		if item.Error != nil {
			log.Printf("Write error: %v", item)
		}
	}
}