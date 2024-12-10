package main

import (

	"database/sql"
	"strconv"
	"encoding/json"
	"fmt"

	// "io"
	"strings"

	// "io/ioutil"
	"log"
	"net/http"
	"os"

	"reflect"

	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/joho/godotenv/autoload"
	_ "github.com/microsoft/go-mssqldb"
)


type table_bom_rec struct {
	DOCUMENTID int64 `db:"DOCUMENTID"`
	HYP_VESCODE sql.NullFloat64 `db:"HYP_VESCODE"`
	HYP_VOY 	sql.NullFloat64 `db:"HYP_VOY"`
	HYP_DIR sql.NullString `db:"HYP_DIR"`
	HYP_VOUCHERNO sql.NullString `db:"HYP_VOUCHERNO"`
	HYP_LISTDAT sql.NullTime `db:"HYP_LISTDAT"`
	HYP_ARCHDAT sql.NullTime `db:"HYP_ARCHDAT"`
	MIG_STATUS sql.NullString `db:"MIG_STATUS"`
	MIG_HASH sql.NullString `db:"MIG_HASH"`
}

type combined_rec struct{	
	DocID                     int64 `db:"DocID"`
	FileId 					 int64
	DocumentName              sql.NullString `db:"DocumentName"`
	BlobPath                  sql.NullString `db:"BlobPath"`

	HYP_VESCODE *float64
	HYP_VOY 	*float64
	HYP_DIR *string
	HYP_VOUCHERNO *string
	HYP_LISTDAT *string
	HYP_ARCHDAT *string
}
func RecDBConnections(db1 *sqlx.DB, db2 *sqlx.DB ){
	db1Data, arrdb1, err := fetchFolderDataFromCOC(db1, os.Getenv("TABLE1"))
	if err != nil {
		log.Fatal(err)
	}

	db2Data, err := fetchAndPrintDataDBRec(db2, os.Getenv("TABLE2"), arrdb1)
	if err != nil {
		log.Fatal(err)
	}

	dosData, arrdb2, err := fetchDosFilesFromCOC(db1, os.Getenv("TABLE1"))
	if err != nil {
		log.Fatal(err)
	}
	dosdb2Data,err:= fetchAndPrintDataDBRec(db2, os.Getenv("TABLE2"), arrdb2)
	if err != nil {
		log.Fatal(err)
	}



	joinRecData(db1Data, db2Data) 
	MigrateFileMetadata() //folders
	processFileMetadataRec(false)

	joinDOSRecData(dosData, dosdb2Data)
	processFileMetadataRec(true)

	


	

}

func fetchAndPrintDataDBRec(db *sqlx.DB, tableName string, arrdb1 []int64) ([]table_bom_rec, error) {
	if len(arrdb1) == 0 {
		return nil, fmt.Errorf("no DOCUMENTID values found in db1")
	}

	// Convert arrdb1 to a comma-separated string
	docIDs := strings.Trim(strings.Join(strings.Fields(fmt.Sprint(arrdb1)), ","), "[]")

	query := fmt.Sprintf("SELECT * FROM %s WHERE DOCUMENTID IN (%s)", tableName, docIDs)

	var rows []table_bom_rec
	err := db.Select(&rows, query)
	if err != nil {
		log.Fatalf("Error querying table %s in db2: %v", tableName, err)
	}
	fmt.Println("fetched from bom", len(rows))

	return rows, nil
}

var combinedRecData []combined_rec
var combineRecDOSData []combined_rec
func joinRecData(db1Data []table_coc, db2Data []table_bom_rec ) {
	// Create a map for db2 data to access it faster
	db2Map := make(map[int64]table_bom_rec)
	for _, row := range db2Data {
		db2Map[row.DOCUMENTID] = row
	}

	// Combine the data from both tables
	
	for _, row1 := range db1Data {
		if row2, exists := db2Map[row1.DocID]; exists {
			

			combinedRow := combined_rec{
				DocID: row1.DocID,
				DocumentName: row1.DocumentName,
				BlobPath:                   row1.BlobPath,
				HYP_VESCODE:				checkFloat(row2.HYP_VESCODE),
				HYP_VOY 	: 				checkFloat(row2.HYP_VOY),
				HYP_DIR:					checkString(row2.HYP_DIR),
				HYP_VOUCHERNO :				checkString(row2.HYP_VOUCHERNO),
				HYP_LISTDAT :				checkTime(row2.HYP_LISTDAT),
				HYP_ARCHDAT:               checkTime(row2.HYP_ARCHDAT),
			}
			combinedRecData = append(combinedRecData, combinedRow)
		}
	}
	fmt.Println("all good while joining")

	
}
func extractFileID(documentName string) int64 {
	if len(documentName) < 6 {
		return -1  // Return empty if the string is too short
	}

	start := strings.Index(documentName, "[")+1
	end := strings.Index(documentName, "]")

	if end != -1 && end > start {
		 fid, err := strconv.ParseInt(documentName[start:end], 10, 64)
		if err != nil{
			log.Fatal("no file id exists for dosfile")
		}else{
			return fid
		}
	}
	return -1  // Return empty string if the format is incorrect
}

func joinDOSRecData(db1Data []table_coc, db2Data []table_bom_rec ) {
	// Create a map for db2 data to access it faster
	db2Map := make(map[int64]table_bom_rec)
	for _, row := range db2Data {
		db2Map[row.DOCUMENTID] = row
	}
	for _, row1 := range db1Data {
		if row2, exists := db2Map[row1.DocID]; exists {
			fileID := extractFileID(row1.DocumentName.String)
			

			combinedRow := combined_rec{
				DocID: row1.DocID,
				FileId: fileID,
				//for dos files
				HYP_VESCODE:				checkFloat(row2.HYP_VESCODE),
				HYP_VOY 	: 				checkFloat(row2.HYP_VOY),
				HYP_DIR:					checkString(row2.HYP_DIR),
				HYP_VOUCHERNO :				checkString(row2.HYP_VOUCHERNO),
				HYP_LISTDAT :				checkTime(row2.HYP_LISTDAT),
				HYP_ARCHDAT:               checkTime(row2.HYP_ARCHDAT),
				
				
				
				
			}
			combinedRecData = append(combinedRecData, combinedRow)
		}
	}

	
}

func processFileMetadataRec(flag bool) {
	loadFileMapFromCSV("RecFile_id_map.csv")
	
	
	addMetadataRec(combinedRecData, flag)

}



// addMetadata function sends metadata to the specified API.
func addMetadataRec(folderData []combined_rec, isDosfile bool) {
	fmt.Println("add metadat function clalled", folderData)
	tenantID := os.Getenv("tenant_id")
	token := os.Getenv("TOKEN")
	metaDataURL := os.Getenv("META_DATA_URL") + "/metadata"

	client := &http.Client{Timeout: 10 * time.Second}

	for _, record := range folderData {
		fmt.Println(record.FileId)
		metadata := buildMetadataRec(record)
		

		// Uncomment for logging metadata
		fmt.Printf("Processing Record : %s, Metadata: %+v\n", record.DocumentName, metadata)
		newmetadata := dosMetadata(metadata, fileIDMap[record.DocID], client)
		if isDosfile {
			// for _, meta := range metadata {
				body := BodyStruct{
					ObjectID: fileIDMap[record.FileId],
					Metadata: newmetadata,
				}

				requestBody, err := json.Marshal(body)
				if err != nil {
					fmt.Printf("Error marshalling request body: %v\n", err)
					continue
				}

				fmt.Printf("Request Body for Record %s: %s", record.DocumentName, string(requestBody)) // Log request body

				if err := sendPostRequest(client, metaDataURL, requestBody, tenantID, token); err != nil {
					fmt.Printf("Error sending POST request: %v\n", err)
				}
			// }
		} else {
			body := BodyStruct{
				ObjectID: fileIDMap[record.DocID],
				Metadata: metadata,
			}

			requestBody, err := json.Marshal(body)
			if err != nil {
				fmt.Printf("Error marshalling request body: %v\n", err)
				continue
			}

			fmt.Printf("Request Body for Record %s: %s\n", record.DocID, string(requestBody)) // Log request body

			if err := sendPostRequest(client, metaDataURL, requestBody, tenantID, token); err != nil {
				fmt.Printf("Error sending POST request: %v\n", err)
			}
		}

	}
}




func buildMetadataRec(record combined_rec) []Metadata {
	fmt.Println("build metadata function called")
	var metadata []Metadata

	v := reflect.ValueOf(record)
	t := reflect.TypeOf(record)

	for i := 0; i < v.NumField(); i++ {
		fieldName := t.Field(i).Name
		fieldValue := v.Field(i).Interface()
		if(fieldName != "DocID" || fieldName != "FolderName" || fieldName != "BlobPath" || fieldName != "DocumentName"){
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

		// Only add non-nil or non-zero fields
		
	}
	return metadata
}