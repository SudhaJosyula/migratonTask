package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"strings"

	// "io/ioutil"
	"log"
	"net/http"
	"os"

	"reflect"

	"time"

	_ "github.com/joho/godotenv/autoload"
	_ "github.com/microsoft/go-mssqldb"
	"go.mongodb.org/mongo-driver/bson"

	"strconv"
)

func createFolders() {
	client := NewMongoClient()
	if client == nil {
		log.Fatal("Failed to connect to MongoDB")
	}
	db := client.Database("rolodex")

	for _, folder := range combinedData {
		parentID := os.Getenv("parentID")
		bolbPath := *folder.BlobPath
		p := strings.Split(bolbPath, "\\")
		for i := 0; i < len(p)-1; i++ {

			pKey := "667521e42974a0288e748571" + "_" + parentID

			found, fileId, err := findOne(context.TODO(), db, bson.M{"name": p[i], "partitionKey": pKey})
			if err != nil {
				log.Fatalf("Error finding document:", err)
			}
			if found {
				fmt.Println("folder already exists", p[i], pKey)
				parentID = fileId.Hex()

			} else {

				parentID, err = createFolderFromBlob(p[i], parentID)
				if err != nil {
					fmt.Println(err)
					fmt.Printf("Failed to create folder: %v\n", p[i])
					failedFolders = append(failedFolders, p[i])
				} else {
					successfulFolders = append(successfulFolders, p[i])
				}

			}

		}

	}
	writeFolderMapToCSV("folder_id_map.csv")
	writeStatustoCSV(failedFolders, "failed_folders.csv")
	writeStatustoCSV(successfulFolders, "successful_folders.csv")
}

func createFolderFromBlob(folderName string, rootFolderID string) (fid string, err error) {
	// rootFolderID := os.Getenv("root_folder_id")
	tenantID := os.Getenv("tenant_id")
	requestBodyStruct := struct {
		Name string `json:"name"`
	}{
		Name: folderName,
	}

	body, err := json.Marshal(requestBodyStruct)
	if err != nil {
		return "", fmt.Errorf("error marshalling JSON: %w", err)
	}

	url := os.Getenv("FOLDER_URL") + rootFolderID
	token := os.Getenv("TOKEN")
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(body))
	if err != nil {
		return "", fmt.Errorf("error creating POST request: %w", err)
	}

	req.Header.Set("x-tenant-id", tenantID)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", token))

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("error making POST request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		fmt.Println(resp)
		return "", fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	// var response struct {
	// 	ID string `json:"id"`
	// }
	// if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
	// 	return fmt.Errorf("error decoding response: %w", err)
	// }

	// mu.Lock()
	// folderIDMap[folderName] = response.ID
	// mu.Unlock()

	// return nil
	var response struct {
		Data struct {
			ID string `json:"id"`
		} `json:"data"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		return "", fmt.Errorf("error decoding response: %w", err)
	}

	if response.Data.ID == "" {
		return "", fmt.Errorf("folder ID is missing in response")
	}

	// Store the folder name and ID in the global map
	mu.Lock()
	folderIDMap[folderName] = response.Data.ID

	mu.Unlock()

	return response.Data.ID, nil
}
func writeStatustoCSV(folders []string, fileName string) error {
	file, err := os.Create(fileName)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Write each folder name as a row
	for _, folder := range folders {
		err = writer.Write([]string{folder})
		if err != nil {
			return fmt.Errorf("failed to write folder: %w", err)
		}
	}

	fmt.Println("Failed folders successfully written to", fileName)
	return nil
}



// function to write a map to a CSV file.
func writeFolderMapToCSV(filename string) {
	// Open the file in write mode. Truncate it if it exists or create a new one.
	file, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		log.Fatalf("Error opening file: %v", err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Write each key-value pair from the map to the CSV
	for folder, id := range folderIDMap {
		if err := writer.Write([]string{folder, id}); err != nil {
			log.Fatalf("Error writing to CSV: %v", err)
		}
	}

	fmt.Println("Successfully written folderIDMap to", filename)
}
func writeFileMapToCSV(filename string) {
	// Open the file in write mode. Truncate it if it exists or create a new one.
	file, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		log.Fatalf("Error opening file: %v", err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Write each key-value pair from the map to the CSV
	for folder, id := range fileIDMap {
		if err := writer.Write([]string{strconv.FormatInt(folder, 10), id}); err != nil {
			log.Fatalf("Error writing to CSV: %v", err)
		}
	}

	fmt.Println("Successfully written fileIDMap to", filename)
}
func loadFolderMapFromCSV(filename string) {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("Error opening file: %v", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)

	records, err := reader.ReadAll()
	if err != nil {
		log.Fatalf("Error reading CSV file: %v", err)
	}

	for _, record := range records {
		if len(record) < 2 {
			log.Printf("Skipping invalid record: %v", record)
			continue
		}
		folder := record[0]
		id := record[1]
		folderIDMap[folder] = id
		// fid, err := strconv.ParseInt(folder, 10, 64)
		// if err != nil {
		// 	log.Fatalf("wrong document ID")
		// }
		// fileIDMap[fid] = id // Update the global map
	}
}

func loadFileMapFromCSV(filename string) {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("Error opening file: %v", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)

	records, err := reader.ReadAll()
	if err != nil {
		log.Fatalf("Error reading CSV file: %v", err)
	}

	for _, record := range records {
		if len(record) < 2 {
			log.Printf("Skipping invalid record: %v", record)
			continue
		}
		folder := record[0]
		id := record[1]
		fid, err := strconv.ParseInt(folder, 10, 64)
		if err != nil {
			log.Fatalf("wrong document ID")
		}
		fileIDMap[fid] = id // Update the global map
	}
}

func processFolderMetadata(flag bool) {
	loadFolderMapFromCSV("folder_id_map.csv")

	fmt.Println("folder meadata adding")
	addMetadata(combinedData, flag)

}

func checkFloat(value sql.NullFloat64) *float64 {
	if value.Valid {
		return &value.Float64
	}
	return nil
}

func checkString(value sql.NullString) *string {
	if value.Valid {
		return &value.String
	}
	return nil
}

func checkTime(value sql.NullTime) *string {
	if value.Valid {
		timeStr := value.Time.String() // Retain the default format
		return &timeStr
	}
	return nil
}
func checkInt(value sql.NullInt64) *int64 {
	if value.Valid {
		return &value.Int64
	}
	return nil
}


var attribute_id_map = map[string]string{

	"HYP_INVREFNO": "6675221140985f3c25d4de46",
	"HYP_SFCNAME":  "6675221240985f3c25d4de4c",
	"HYP_SFCNR":    "6675221340985f3c25d4de4e",
	"HYP_VOUNO":    "6675220f40985f3c25d4de40",
	"HYP_REMARK":   "6675221940985f3c25d4de64",
	"HYP_VOUDAT":   "6675221040985f3c25d4de44",
	"HYP_ACCOUNT":  "6675222440985f3c25d4de8c",
	"HYP_YMCODE2":  "6675222540985f3c25d4de90",
	"HYP_VESSEL":   "6675221b40985f3c25d4de6c",
	"HYP_VOYAGE":   "6675221640985f3c25d4de58",
	"HYP_AREA":     "6675221440985f3c25d4de54",
	"HYP_MONTH":    "6675221140985f3c25d4de48",
	"HYP_ARCHDAT":  "6675221a40985f3c25d4de66",
	"HYP_VESCODE":   "6715324f6ad95ec447dadea7",
	"HYP_VOY":       "6715325d6ad95ec447dadea9",
	"HYP_DIR":       "671532706ad95ec447dadeab",
	"HYP_VOUCHERNO": "671532916ad95ec447dadead",
	"HYP_LISTDAT":   "671532b16ad95ec447dadeaf",
}

// Metadata struct represents the metadata format expected by the API.
type Metadata struct {
	AttributeID string      `json:"attributeId"`
	Value       interface{} `json:"value"`
}

// BodyStruct represents the request body structure.
type BodyStruct struct {
	ObjectID string     `json:"objectId"`
	Metadata []Metadata `json:"metadata"`
}

// addMetadata function sends metadata to the specified API.
func addMetadata(folderData []combinedTable, isDosfile bool) {
	fmt.Println("add metadat function clalled")
	tenantID := os.Getenv("tenant_id")
	token := os.Getenv("TOKEN")
	metaDataURL := os.Getenv("META_DATA_URL") + "/metadata"

	client := &http.Client{Timeout: 10 * time.Second}

	for _, record := range folderData {
		metadata := buildMetadata(record)

		// Uncomment for logging metadata
		// fmt.Printf("Processing Record ID: %s, Metadata: %+v\n", folderIDMap[record.FolderName], metadata)
		
		if isDosfile {
			newmetadata := dosMetadata(metadata, folderIDMap[record.FolderName], client)
			body := BodyStruct{
				ObjectID: folderIDMap[record.FolderName],
				Metadata: newmetadata,
			}

			requestBody, err := json.Marshal(body)
			if err != nil {
				fmt.Printf("Error marshalling request body: %v\n", err)
				continue
			}

			fmt.Printf("Request Body for Record %s: %s\n", folderIDMap[*record.DocumentName], string(requestBody)) // Log request body

			if err := sendPostRequest(client, metaDataURL, requestBody, tenantID, token); err != nil {
				fmt.Printf("Error sending POST request: %v\n", err)
			}
		} else {
			body := BodyStruct{
				ObjectID: folderIDMap[*record.DocumentName],
				Metadata: metadata,
			}

			requestBody, err := json.Marshal(body)
			if err != nil {
				fmt.Printf("Error marshalling request body: %v\n", err)
				continue
			}

			fmt.Printf("Request Body for Record %s: %s\n", folderIDMap[*record.DocumentName], string(requestBody)) // Log request body

			if err := sendPostRequest(client, metaDataURL, requestBody, tenantID, token); err != nil {
				fmt.Printf("Error sending POST request: %v\n", err)
			}
		}

	}
}

func buildMetadata(record combinedTable) []Metadata {
	fmt.Println("build metadata function called")
	var metadata []Metadata

	v := reflect.ValueOf(record)
	t := reflect.TypeOf(record)

	for i := 0; i < v.NumField(); i++ {
		fieldName := t.Field(i).Name
		fieldValue := v.Field(i).Interface()

		// Only add non-nil or non-zero fields
		if !isFieldEmpty(fieldValue) {
			if attrID, exists := attribute_id_map[fieldName]; exists {
				metadata = append(metadata, Metadata{
					AttributeID: attrID,
					Value:       fieldValue,
				})
			}
		} else {
			fmt.Println("field is empty", fieldName)
		}
	}
	return metadata
}

// sendPostRequest sends a POST request with the given request body and headers.
func sendPostRequest(client *http.Client, url string, requestBody []byte, tenantID string, token string) error {
	request, err := http.NewRequest("POST", url, bytes.NewBuffer(requestBody))
	if err != nil {
		return fmt.Errorf("Error creating POST request: %v", err)
	}

	request.Header.Set("Authorization", fmt.Sprintf("Bearer %s", token))
	request.Header.Set("x-tenant-id", tenantID)
	request.Header.Set("Content-Type", "application/json")

	response, err := client.Do(request)
	if err != nil {
		return fmt.Errorf("Error making POST request: %v", err)
	}
	defer response.Body.Close()

	bodyBytes, err := io.ReadAll(response.Body) // Read the response body
	if err != nil {
		return fmt.Errorf("Error reading response body: %v", err)
	}

	if response.StatusCode != http.StatusCreated {
		return fmt.Errorf("unexpected status code: %d, response: %s", response.StatusCode, bodyBytes)
	}

	return nil
}


func isFieldEmpty(value interface{}) bool {
	// fmt.Println(reflect.TypeOf(value), value)
	switch v := value.(type) {
	case *float64:
		return v == nil
	case *string:
		return v == nil
	// Extend with cases for other types if necessary
	default:
		return false
	}
}



// func addMetadata1(folderData []combinedTable, isDosfile bool) {
// 	loadFolderMapFromCSV("folder_id_map.csv")
// 	fmt.Println("add metadat function clalled")
// 	tenantID := os.Getenv("tenant_id")
// 	token := os.Getenv("TOKEN")
// 	metaDataURL := os.Getenv("META_DATA_URL") + "/metadata"

// 	client := &http.Client{Timeout: 10 * time.Second}

// 	for _, record := range folderData {
// 		metadata := buildMetadata(record)
// 		oid := folderIDMap[*record.DocumentName]

// 		// Uncomment for logging metadata
// 		fmt.Printf("Processing Record ID: %s, Metadata: %+v\n", folderIDMap[*record.DocumentName], metadata)

// 		// for _, meta := range metadata {
// 		if isDosfile {
// 			for _, meta := range metadata {
// 				dosMetadata(metadata, oid, client)
// 				body := BodyStruct{
// 					ObjectID: record.FolderName,
// 					Metadata: []Metadata{meta},
// 				}

// 				requestBody, err := json.Marshal(body)
// 				if err != nil {
// 					fmt.Printf("Error marshalling request body: %v\n", err)
// 					continue
// 				}

// 				fmt.Printf("Request Body for Record %s: %s\n", record.FolderName, string(requestBody)) // Log request body

// 				if err := sendPostRequest(client, metaDataURL, requestBody, tenantID, token); err != nil {
// 					fmt.Printf("Error sending POST request: %v\n", err)
// 				}
// 			}
// 		} else {
// 			body := BodyStruct{
// 				ObjectID: folderIDMap[*record.DocumentName],
// 				Metadata: metadata,
// 			}

// 			requestBody, err := json.Marshal(body)
// 			if err != nil {
// 				fmt.Printf("Error marshalling request body: %v\n", err)
// 				continue
// 			}

// 			fmt.Printf("Request Body for Record %s: %s\n", folderIDMap[*record.DocumentName], string(requestBody)) // Log request body

// 			if err := sendPostRequest(client, metaDataURL, requestBody, tenantID, token); err != nil {
// 				fmt.Printf("Error sending POST request: %v\n", err)
// 			}
// 		}

// 	}
// }


func dosMetadata(metadata []Metadata, oid string, client *http.Client) []Metadata {
	url := "https://rolodex.dev.maersk-digital.net/api/v1/metadata/metadata?objectId=" + oid + "&nextCursor=&itemsPerPage=100"
	token := os.Getenv("TOKEN")
	tenantID := os.Getenv("tenant_id")
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		log.Fatalf("error creating GET request: %v", err)
	}

	req.Header.Set("x-tenant-id", tenantID)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", token))
	response, err := client.Do(req)
	if err != nil {
		log.Fatalf("error fetching the existing metadata: %v", err)
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		log.Fatalf("failed to fetch metadata: %s", response.Status)
	}

	body, err := ioutil.ReadAll(response.Body)
	if err != nil {
		log.Fatalf("error reading response body: %v", err)
	}

	var result map[string]interface{}
	err = json.Unmarshal(body, &result)
	if err != nil {
		log.Fatalf("error unmarshaling response JSON: %v", err)
	}

	userDefined, ok := result["data"].(map[string]interface{})["userDefined"].([]interface{})
	if !ok {
		log.Fatalf("invalid or missing 'userDefined' data in response")
	}

	// Deduplication logic
	nonDupMap := []Metadata{}
	// Map to track unique AttributeID and Value

	for _, meta := range metadata {
		// Check if the metadata entry exists in the API response
		exists := false
		if meta.Value == nil {
			continue
		}
		for _, attr := range userDefined {
			attribute, ok := attr.(map[string]interface{})
			if !ok {
				continue
			}

			if attribute["attributeId"] == meta.AttributeID {
				dt := attribute["dataType"]

				if dt == "string" || dt == "date" {
					fmt.Println(attribute["attributeId"], attribute["value"], *meta.Value.(*string))
					if attribute["value"] == *meta.Value.(*string) {

						exists = true
						fmt.Println("exits", attribute["attributeId"], attribute["value"], *meta.Value.(*string))
						break
					}
					fmt.Println("not exits", attribute["attributeId"], attribute["value"], *meta.Value.(*string))

				}
				if dt == "number" {
					fmt.Println(attribute["attributeId"], attribute["value"], *meta.Value.(*float64))
					if attribute["value"] == *meta.Value.(*float64) {
						exists = true
						break
					}
				}

			}
		}

		// Add to nonDupMap if not exists and not a duplicate
		if !exists {
			nonDupMap = append(nonDupMap, meta)
		}
	}

	// fmt.Println("BEFORE:", metadata)
	for key, val := range metadata {
		fmt.Println(key, val.Value)
	}
	for key, val := range nonDupMap {
		fmt.Println(key, val.Value)
	}

	return nonDupMap
}
