package main

import (
	"bytes"
	"context"

	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"mime"
	"net/http"
	"os"

	"strings"
	"time"

	
	openfga "github.com/openfga/go-sdk"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var tenantId= os.Getenv("tenant_id")

var token = os.Getenv("TOKEN")


type FindOneResult struct {
	Id primitive.ObjectID `bson:"_id"`
}

func findOne(ctx context.Context, db *mongo.Database, criteria bson.M) (bool, primitive.ObjectID, error) {
	var result FindOneResult
	err := db.Collection("metadata").FindOne(ctx, criteria, options.FindOne().SetProjection(bson.M{"_id": 1})).Decode(&result)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return false, primitive.NilObjectID, nil
		}
		return false, primitive.NilObjectID, err
	}
	return true, result.Id, nil
}

func insertOne(ctx context.Context, db *mongo.Database, document bson.M) (primitive.ObjectID, error) {
	res, err := db.Collection("metadata").InsertOne(ctx, document)
	if err != nil {
		return primitive.NilObjectID, err
	}
	return res.InsertedID.(primitive.ObjectID), nil
}


var fileIDMap = make(map[int64]string);


func MigrateFileMetadata() {
	//connecting to mongo client
	client := NewMongoClient()
	if client == nil {
		log.Fatal("Failed to connect to MongoDB")
	}
	db := client.Database("rolodex")
	loadFileMapFromCSV("RecFile_id_map.csv")
	
	tenantID,err :=getTenantIDfromADGroupID(tenantId, db)
	if(err != nil ){
		log.Fatalf("tenant not found")
	}

	for _, row := range files{
		fmt.Println(row)
		// parentId ,err := getParentID(row.BlobPath.String, db)
		parentId := "674d5865a885fc5c43fd2b31"
		if err != nil {
			log.Fatalf("Error extracting parent folder:", err)
		}
		fileCriteria := bson.M{
			"partitionKey": tenantID.Hex() + "_" + parentId,
			"name":         row.DocumentName.String,
			"type":         "file",
			"isDeleted":    false,
		}

		fileID, err := insertFile(row, fileCriteria, db, parentId)
		if err != nil {
			log.Fatalf("Error inserting document:", err)
		}
		//writing into filemap
		fileIDMap[row.DocID] = fileID
		
		category := GetCategoryAttributes()
		var contentType string
		idx := strings.LastIndex(row.DocumentName.String, ".")
		if idx != -1 {
			contentType = mime.TypeByExtension(row.DocumentName.String[idx:])
		}
		if contentType == "" {
			contentType = "application/octet-stream"
		}
		fileSize := int(row.BlobSize.Int64)
		if err != nil {
			fmt.Println("error while converting filesize to int")
		}

		migrateCoreMetaData(category, fileID, fileSize, row.BlobMD5.String, contentType, "md5")

	}
	writeFileMapToCSV("RecFile_id_map.csv")

}



func getParentID(blobPath string ,db *mongo.Database)(parentid string, err error) {
	path := strings.Split(blobPath, "\\")
	parentid = os.Getenv("parentID")
	for i :=0; i< len(path) -1; i++{
		pKey := "667521e42974a0288e748571"+"_"+parentid
			
			found, fileId, err := findOne(context.TODO(), db, bson.M{"name": path[i] , "partitionKey":pKey})
			if err != nil {
				log.Fatalf("Error finding document:", err)
			
				
			}
			if found {
				fmt.Println("folder already exists", path[i] ,pKey)
				parentid = fileId.Hex()


			} else {
				
				parentid, err = createFolderFromBlob(path[i], parentid)
				folderIDMap[path[i]] = parentid

				
			
			}
	}
	writeFileMapToCSV("folder_id_map.csv")
	return parentid, nil;
	
}

func insertFile(row table_coc, fileCriteria bson.M, db *mongo.Database, parentId string) (fileID string, err error) {

	found, fileId, err := findOne(context.TODO(), db, fileCriteria)
	if err != nil {
		log.Fatalf("Error finding document:", err)
	}
	if found {
		fmt.Println("file already exists")
		return fileId.Hex(), nil
	} else {
		file := fileCriteria
		fileId = primitive.NewObjectID()
		file["_id"] = fileId
		tenantID,err := getTenantIDfromADGroupID(tenantId, db)
		if err != nil {
			fmt.Println("Tenand doesnt exist:", err)
			return "", err
		}
		file["tenantId"] = tenantID

		parentID, err := primitive.ObjectIDFromHex(parentId)
		if err != nil {
			fmt.Println("Invalid parentId:", err)
			return "", nil
		}
		file["parentId"] = parentID
		uri := fmt.Sprintf("%s/%s", row.ExportStorageAccount.String, strings.ReplaceAll(row.BlobPath.String, "\\", "/"))

		if strings.HasSuffix(uri, ".idx") {
			uri, _ = strings.CutSuffix(uri, ".idx")
		}
		file["fileUri"] = uri
		// file["createdDate"] = 
		// createdDate, err := time.Parse("2006-01-02 15:04:05 -0700 MST", row.ExportDate.Time.String())
		// file["createdDate"] = primitive.NewDateTimeFromTime(createdDate)
		// file["modifiedDate"] = primitive.NewDateTimeFromTime(createdDate)

		file["createdDate"] = row.ExportDate.Time
		file["modifiedDate"] = row.ExportDate.Time
		file["createdBy"] = bson.M{
			"oid":   "3918db16-2e82-4f96-9cc4-ccd4e708d100",
			"name":  "rolodex-spn",
			"email": "rolodex-spn",
		}
		file["modifiedBy"] = bson.M{
			"oid":   "3918db16-2e82-4f96-9cc4-ccd4e708d100",
			"name":  "rolodex-spn",
			"email": "rolodex-spn",
		}
		insertedId, err := insertOne(context.TODO(), db, file)
		if err != nil {
			fmt.Println("not inserted properly")
			return "", err
		} else {
			fmt.Println("Inserted document with ID:", insertedId.Hex())
			//write into openfga
			fga := NewAuthorizationClient()
			var writes []openfga.TupleKey = make([]openfga.TupleKey, 0)
			writes = append(writes, openfga.TupleKey{
				User:     fmt.Sprintf("folder:%s", parentId),
				Relation: "parent",
				Object:   fmt.Sprintf("file:%s", insertedId.Hex()),
			})
			fga.WriteRelation(writes)
			fmt.Println("file inserted successfully", insertedId.Hex())
			return insertedId.Hex(), nil

		}

	}

}

func getTenantIDfromADGroupID(adGroupId string, db *mongo.Database)(tenantId primitive.ObjectID , err error){
	var result bson.M
	err = db.Collection("tenants").FindOne(context.TODO(), bson.M{"adGroupId": adGroupId}).Decode(&result)
	if err != nil {
		return primitive.NilObjectID, err
	}
	id, ok := result["_id"].(primitive.ObjectID)
	if !ok {
		return primitive.NilObjectID, fmt.Errorf("failed to cast _id to ObjectID")
	}
	return id, nil

}



func GetCategoryAttributes() map[string]interface{} {

	url := os.Getenv("META_DATA_URL") + "/categories/name/file_core_category"
	request, err := http.NewRequest("GET", url, nil)
	request.Header.Set("Authorization", fmt.Sprintf("Bearer %s", token))
	request.Header.Set("x-tenant-id", tenantId)
	request.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 10 * time.Second}
	response, err := client.Do(request)
	if err != nil {
		fmt.Errorf("error making GET request: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		fmt.Errorf("failed to fetch category attributes: %s", response)
	}
	body, err := ioutil.ReadAll(response.Body)
	if err != nil {
		fmt.Println("Error reading response body:", err)
		return nil
	}

	var result map[string]interface{}
	err = json.Unmarshal(body, &result)
	if err != nil {
		fmt.Println("Error unmarshaling response JSON:", err)
		return nil
	}
	// fmt.Println(result)
	// Check for "data" in the result
	data, exists := result["data"]
	if !exists || data == nil {
		fmt.Println("Failed to fetch category attributes: 'data' field missing")
		return nil
	}

	// Convert "data" to map if possible
	category, ok := data.(map[string]interface{})
	if !ok {
		fmt.Println("Failed to parse 'data' as map[string]interface{}")
		return nil
	}
	return category
}

type FileMetadata struct {
	AttributeID string      `json:"attributeId"`
	Value       interface{} `json:"value"`
	CategoryID  string      `json:"categoryId"`
}

type RequestBody struct {
	ObjectID string         `json:"objectId"`
	Metadata []FileMetadata `json:"metadata"`
}

func migrateCoreMetaData(categories map[string]interface{}, fileId string, fileSize int, hash string, contentType string, hashAlgo string) {
	fmt.Println("inserting metadata of file :", fileId)
	category_id := categories["id"].(string)
	var attributeMap = make(map[string]string)
	if attributes, ok := categories["attributes"].([]interface{}); ok {
		for _, attr := range attributes {
			// Convert each attribute to map[string]interface{}
			if attrMap, ok := attr.(map[string]interface{}); ok {
				// Extract "name" and "id" if they exist
				name, nameOk := attrMap["name"].(string)
				id, idOk := attrMap["id"].(string)
				if nameOk && idOk {
					attributeMap[name] = id
				} else {
					fmt.Println("Error: 'name' or 'id' is not in the expected format")
				}
			}

		}

	} else {
		fmt.Println("Error: 'attributes' is not in the expected format")
	}

	requestBody := RequestBody{
		ObjectID: fileId,
		Metadata: []FileMetadata{
			{
				AttributeID: attributeMap["fileSize"],
				Value:       fileSize,
				CategoryID:  category_id,
			},
			{
				AttributeID: attributeMap["fileContentType"],
				Value:       contentType,
				CategoryID:  category_id,
			},
			{
				AttributeID: attributeMap["fileHash"],
				Value:       hash,
				CategoryID:  category_id,
			},
			{
				AttributeID: attributeMap["fileHashAlgo"],
				Value:       hashAlgo,
				CategoryID:  category_id,
			},
		},
	}

	body, err := json.Marshal(requestBody)
	if err != nil {	}
	fmt.Println(requestBody)

	
	url := os.Getenv("META_DATA_URL") + "/metadata"
	request, err := http.NewRequest("POST", url, bytes.NewBuffer(body))
	request.Header.Set("Authorization", fmt.Sprintf("Bearer %s", token))
	request.Header.Set("x-tenant-id", tenantId)
	request.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 10 * time.Second}
	response, err := client.Do(request)
	if err != nil {
		fmt.Errorf("error making GET request: %w", err)
	}
	if response.StatusCode != http.StatusCreated {
		fmt.Println("statuscode wrong", response.StatusCode, response)
	}else{
		fmt.Println("metadata also migrated successfully", response)
	}

	
	defer response.Body.Close()

}
