package main

import (
	"database/sql"

	"fmt"

	"log"
	"os"
	"strings"
	"sync"

	"github.com/jmoiron/sqlx"
	_ "github.com/joho/godotenv/autoload"
	_ "github.com/microsoft/go-mssqldb"
)

// structure for COClogs database
type table_coc struct {
	DocID int64 `db:"DocID"`
	ExportDate   sql.NullTime   `db:"ExportDate"`
	DocumentType sql.NullString `db:"DocumentType"`
	DocumentName sql.NullString `db:"DocumentName"`
	BlobSize sql.NullInt64 `db:"BlobSize"`
	BlobMD5             sql.NullString `db:"BlobMD5"`
	MD5VerificationDate sql.NullTime   `db:"MD5VerificationDate"`
	BlobPath             sql.NullString `db:"BlobPath"`
	ExportStorageAccount sql.NullString `db:"ExportStorageAccount"`
	IndexBlobMD5         sql.NullString `db:"IndexBlobMD5"`
}

// Structure for BOM database
type table_bom struct {
	DOCUMENTID   int64           `db:"DOCUMENTID"`
	HYP_INVREFNO sql.NullString  `db:"HYP_INVREFNO"`
	HYP_SFCNAME  sql.NullString  `db:"HYP_SFCNAME"`
	HYP_SFCNR    sql.NullFloat64 `db:"HYP_SFCNR"`
	HYP_VOUNO    sql.NullFloat64 `db:"HYP_VOUNO"`
	HYP_REMARK   sql.NullString  `db:"HYP_REMARK"`
	HYP_VOUDAT   sql.NullTime    `db:"HYP_VOUDAT"`
	HYP_ACCOUNT  sql.NullString  `db:"HYP_ACCOUNT"`
	HYP_YMCODE2  sql.NullFloat64 `db:"HYP_YMCODE2"`
	HYP_VESSEL   sql.NullFloat64 `db:"HYP_VESSEL"`
	HYP_VOYAGE   sql.NullFloat64 `db:"HYP_VOYAGE"`
	HYP_AREA     sql.NullFloat64 `db:"HYP_AREA"`
	HYP_MONTH    sql.NullFloat64 `db:"HYP_MONTH"`
	HYP_ARCHDAT  sql.NullTime    `db:"HYP_ARCHDAT"`
}

type combinedTable struct {
	DocID                *int64   `json:"DocID"`
	FolderName           string   `json:"FolderName"` // for dosfiles
	ExportDate           *string  `json:"ExportDate"`
	DocumentType         *string  `json:"DocumentType"`
	DocumentName         *string  `json:"DocumentName"`
	BlobSize             *int64   `json:"BlobSize"`
	BlobMD5              *string  `json:"BlobMD5"`
	MD5VerificationDate  *string  `json:"MD5VerificationDate"`
	BlobPath             *string  `json:"BlobPath"`
	ExportStorageAccount *string  `json:"ExportStorageAccount"`
	IndexBlobMD5         *string  `json:"IndexBlobMD5"`
	HYP_INVREFNO         *string  `json:"HYP_INVREFNO"`
	HYP_SFCNAME          *string  `json:"HYP_SFCNAME"`
	HYP_SFCNR            *float64 `json:"HYP_SFCNR"`
	HYP_VOUNO            *float64 `json:"HYP_VOUNO"`
	HYP_REMARK           *string  `json:"HYP_REMARK"`
	HYP_VOUDAT           *string  `json:"HYP_VOUDAT"`
	HYP_ACCOUNT          *string  `json:"HYP_ACCOUNT"`
	HYP_YMCODE2          *float64 `json:"HYP_YMCODE2"`
	HYP_VESSEL           *float64 `json:"HYP_VESSEL"`
	HYP_VOYAGE           *float64 `json:"HYP_VOYAGE"`
	HYP_AREA             *float64 `json:"HYP_AREA"`
	HYP_MONTH            *float64 `json:"HYP_MONTH"`
	HYP_ARCHDAT          *string  `json:"HYP_ARCHDAT"`
}

var files []table_coc

func fetchFiles1(db *sqlx.DB, tablename string) {
	query := fmt.Sprintf(
		"SELECT TOP(10) * FROM %s WHERE [DocumentType]!='DC_FOLDER' AND [DocumentType] !='DC_DOSFILE' ORDER BY %s", tablename, "DocID")

	err := db.Select(&files, query)
	if err != nil {
		log.Fatalf("Error creating CSV file: %v", err)
	}

}

// fetching data from COC
var rows []table_coc
func fetchFolderDataFromCOC(db *sqlx.DB, tableName string) ([]table_coc, []int64, error) {

	
	query := fmt.Sprintf(`
    SELECT TOP(5) 
        DocID,
        ExportDate,
        DocumentType,
        DocumentName,
        BlobSize,
        BlobMD5,
        MD5VerificationDate,
        BlobPath,
        ExportStorageAccount,
        IndexBlobMD5
    FROM %s 
    WHERE DocumentType = 'DC_FOLDER' 
    ORDER BY %s
`, tableName, "DocID")
	
	err := db.Select(&rows, query)
	if err != nil {
		log.Fatalf("Error querying table %s in db1: %v", tableName, err)
	}

	var arrdB1 []int64

	for _, row := range rows {
		arrdB1 = append(arrdB1, row.DocID)
	}
	fmt.Println("fetched from coc table")
	return rows, arrdB1, nil

}

func fetchDosFilesFromCOC(db *sqlx.DB, tableName string) ([]table_coc, []int64, error){
	// query := fmt.Sprintf("SELECT  TOP(1) * FROM %s WHERE DocumentType = 'DC_DOSFILE' ORDER BY %s", tableName, "DocID")
	query := fmt.Sprintf(`
    SELECT  
        DocID,
        ExportDate,
        DocumentType,
        DocumentName,
        BlobSize,
        BlobMD5,
        MD5VerificationDate,
        BlobPath,
        ExportStorageAccount,
        IndexBlobMD5
    FROM %s 
    WHERE DocumentType = 'DC_DOSFILE' AND DocID = 34
    ORDER BY %s
`, tableName, "DocID")
	err := db.Select(&rows, query)
	if err != nil {
		log.Fatalf("Error querying table %s in db1: %v", tableName, err)
	}

	var arrdB1 []int64

	for _, row := range rows {
		arrdB1 = append(arrdB1, row.DocID)
	}
	fmt.Println("fetched from coc table")
	return rows, arrdB1, nil
}

// fetching data from COC
func fetchDataFromBOM(db *sqlx.DB, tableName string, arrdb1 []int64) ([]table_bom, error) {
	if len(arrdb1) == 0 {
		return nil, fmt.Errorf("no DOCUMENTID values found in db1")
	}

	// Convert arrdb1 to a comma-separated string
	docIDs := strings.Trim(strings.Join(strings.Fields(fmt.Sprint(arrdb1)), ","), "[]")

	query := fmt.Sprintf("SELECT DOCUMENTID,HYP_INVREFNO,HYP_SFCNAME, HYP_SFCNR,HYP_VOUNO,HYP_REMARK,HYP_VOUDAT,HYP_ACCOUNT,HYP_YMCODE2,HYP_VESSEL,HYP_VOYAGE,HYP_AREA, HYP_MONTH, HYP_ARCHDAT FROM %s WHERE DOCUMENTID IN (%s)", tableName, docIDs)
	

	var rows []table_bom
	err := db.Select(&rows, query)
	if err != nil {
		log.Fatalf("Error querying table %s in db2: %v", tableName, err)
	}
	fmt.Println("fetched from bom")
	return rows, nil
}

var combinedData []combinedTable
var combinedDosData []combinedTable

func joinData(db1Data []table_coc, db2Data []table_bom) {
	// Create a map for db2 data to access it faster
	db2Map := make(map[int64]table_bom)
	for _, row := range db2Data {
		db2Map[row.DOCUMENTID] = row
	}

	// Combine the data from both tables

	for _, row1 := range db1Data {
		if row2, exists := db2Map[row1.DocID]; exists {

			combinedRow := combinedTable{
				DocID: &row1.DocID,
				ExportDate:           checkTime(row1.ExportDate),
				DocumentType:         checkString(row1.DocumentType),
				DocumentName:         checkString(row1.DocumentName),
				BlobSize:             checkInt(row1.BlobSize),
				BlobMD5:              checkString(row1.BlobMD5),
				MD5VerificationDate:  checkTime(row1.MD5VerificationDate),
				BlobPath:             checkString(row1.BlobPath),
				ExportStorageAccount: checkString(row1.ExportStorageAccount),

				IndexBlobMD5: checkString(row1.IndexBlobMD5),
				HYP_INVREFNO: checkString(row2.HYP_INVREFNO),
				HYP_SFCNAME:  checkString(row2.HYP_SFCNAME),
				HYP_SFCNR:    checkFloat(row2.HYP_SFCNR),
				HYP_VOUNO:    checkFloat(row2.HYP_VOUNO),
				HYP_REMARK:   checkString(row2.HYP_REMARK),
				HYP_VOUDAT:   checkTime(row2.HYP_VOUDAT),
				HYP_ACCOUNT:  checkString(row2.HYP_ACCOUNT),
				HYP_YMCODE2:  checkFloat(row2.HYP_YMCODE2),
				HYP_VESSEL:   checkFloat(row2.HYP_VESSEL),
				HYP_VOYAGE:   checkFloat(row2.HYP_VOYAGE),
				HYP_AREA:     checkFloat(row2.HYP_AREA),
				HYP_MONTH:    checkFloat(row2.HYP_MONTH),

				HYP_ARCHDAT: checkTime(row2.HYP_ARCHDAT),
			}
			combinedData = append(combinedData, combinedRow)
		}
	}
	fmt.Println("joined data",combinedData[0:2]  )

}

func joinDosFiles(db1Data []table_coc, db2Data []table_bom) {
	// Create a map for db2 data to access it faster
	db2Map := make(map[int64]table_bom)
	for _, row := range db2Data {
		db2Map[row.DOCUMENTID] = row
	}

	// Combine the data from both tables

	for _, row1 := range db1Data {
		if row2, exists := db2Map[row1.DocID]; exists {
			folderName := extractFolderID(row1.DocumentName.String)

			combinedRow := combinedTable{
				DocID:      &row1.DocID,
				FolderName: folderName,
				ExportDate:   checkTime(row1.ExportDate),
				DocumentType: checkString(row1.DocumentType),
				DocumentName: checkString(row1.DocumentName),
				BlobSize: checkInt(row1.BlobSize),
				BlobMD5:             checkString(row1.BlobMD5),
				MD5VerificationDate: checkTime(row1.MD5VerificationDate),
				BlobPath: checkString(row1.BlobPath),
				ExportStorageAccount: checkString(row1.ExportStorageAccount),
				IndexBlobMD5: checkString(row1.IndexBlobMD5),

				HYP_INVREFNO: checkString(row2.HYP_INVREFNO),
				HYP_SFCNAME:  checkString(row2.HYP_SFCNAME),
				HYP_SFCNR:    checkFloat(row2.HYP_SFCNR),
				HYP_VOUNO:    checkFloat(row2.HYP_VOUNO),
				HYP_REMARK:   checkString(row2.HYP_REMARK),
				HYP_VOUDAT:   checkTime(row2.HYP_VOUDAT),
				HYP_ACCOUNT:  checkString(row2.HYP_ACCOUNT),
				HYP_YMCODE2:  checkFloat(row2.HYP_YMCODE2),
				HYP_VESSEL:   checkFloat(row2.HYP_VESSEL),
				HYP_VOYAGE:   checkFloat(row2.HYP_VOYAGE),
				HYP_AREA:     checkFloat(row2.HYP_AREA),
				HYP_MONTH:    checkFloat(row2.HYP_MONTH),
				HYP_ARCHDAT: checkTime(row2.HYP_ARCHDAT),
			}
			combinedData = append(combinedData, combinedRow)
		}
	}

}





func extractFolderID(documentName string) string {
	if len(documentName) < 6 {
		return "" // Return empty if the string is too short
	}

	start := 5 // Start from the sixth character (index 5)
	end := strings.Index(documentName, "]")

	if end != -1 && end > start {
		return documentName[start : end+1] // Extract from sixth character to ]
	}
	return "" // Return empty string if the format is incorrect
}

func getSQLClient(database string) *sqlx.DB {
	server := os.Getenv("server")
	username := os.Getenv("username")
	password := os.Getenv("password")
	connString := fmt.Sprintf("sqlserver://%s:%s@%s?database=%s&connection+timeout=30",
		username, password, server, database)

	db, err := sqlx.Open("sqlserver", connString)
	if err != nil {
		log.Fatalf("Error creating connection pool: %v", err)
	}

	err = db.Ping()
	if err != nil {
		log.Fatalf("Error connecting to database: %v", err)
	}

	return db
}
func assertNotNil(obj interface{}, message string) {
	if obj == nil {
		log.Fatal(message)
	}
}

var database1 = "COCLogs_I_20240428"
var database2 = "BOM_20240428"

func establishConnection()(db1, db2 *sqlx.DB) {
	db1 = getSQLClient(database1)
	assertNotNil(db1, "a1  is nil")
	fmt.Println("Successfully connected to database 1:", database1)

	db2 = getSQLClient(database2)
	assertNotNil(db2, "coc db is  nil")
	fmt.Println("Successfully connected to database 2:", database2)
	return db1,db2
}

func main() {
	db1, db2 := establishConnection()
	
	db1Data, arrdb1, err := fetchFolderDataFromCOC(db1, os.Getenv("TABLE1"))
	if err != nil {
		log.Fatal(err)
	}

	db2Data, err := fetchDataFromBOM(db2, os.Getenv("TABLE2"), arrdb1)
	if err != nil {
		log.Fatal(err)
	}

	
	joinData(db1Data, db2Data)
	createFolders()
	processFolderMetadata(false)


	dosData, arrdb2, err := fetchDosFilesFromCOC(db1, os.Getenv("TABLE1"))
	if err != nil {
		log.Fatal(err)
	}
	dosdb2DataBOM,err:= fetchDataFromBOM(db2, os.Getenv("TABLE2"), arrdb2)
	if err != nil {
		log.Fatal(err)
	}

	joinDosFiles(dosData, dosdb2DataBOM) //dos files
	processFolderMetadata(true)

	fetchFiles1(db1, os.Getenv("TABLE1"))
	MigrateFileMetadata()



}

var folderIDMap = make(map[string]string)
var mu sync.Mutex
var successfulFolders []string
var failedFolders []string
