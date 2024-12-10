package main

// import (
// 	"database/sql"
// 	"encoding/csv"
// 	"fmt"
// 	"os"
// 	"strconv"
// 	"time"
// )

// type table_coc struct {
// 	DocID                 sql.NullInt64
// 	ExportDate            sql.NullTime
// 	DocumentType          sql.NullString
// 	DocumentName          sql.NullString
// 	LocalSize             sql.NullInt64
// 	BlobSize              sql.NullInt64
// 	LocalMD5              sql.NullString
// 	BlobMD5               sql.NullString
// 	MD5VerificationDate   sql.NullTime
// 	BlobContainerName     sql.NullString
// 	BlobPath              sql.NullString
// 	ExportAccount         sql.NullString
// 	ExportConnectivityAccount sql.NullString
// 	ExportStorageAccount  sql.NullString
// 	IndexName             sql.NullString
// 	IndexLocalMD5         sql.NullString
// 	IndexBlobMD5          sql.NullString
// }

// type table_bom struct {
// 	DOCUMENTID    sql.NullInt64
// 	HYP_INVREFNO  sql.NullString
// 	HYP_SFCNAME   sql.NullString
// 	HYP_SFCNR     sql.NullFloat64
// 	HYP_VOUNO     sql.NullString
// 	HYP_REMARK    sql.NullString
// 	HYP_VOUDAT    sql.NullTime
// 	HYP_ACCOUNT   sql.NullFloat64
// 	HYP_YMCODE2   sql.NullFloat64
// 	HYP_VESSEL    sql.NullFloat64
// 	HYP_VOYAGE    sql.NullFloat64
// 	HYP_COSTCENTER sql.NullFloat64
// 	HYP_AREA      sql.NullFloat64
// 	HYP_MONTH     sql.NullFloat64
// 	HYP_AGENT     sql.NullFloat64
// 	HYP_ARCHDAT   sql.NullTime
// 	MIG_STATUS    sql.NullString
// 	MIG_HASH      sql.NullString
// }

// type combinedData struct {
// 	// Fields from table_coc
// 	DocID                 sql.NullInt64
// 	ExportDate            sql.NullTime
// 	DocumentType          sql.NullString
// 	DocumentName          sql.NullString
// 	LocalSize             sql.NullInt64
// 	BlobSize              sql.NullInt64
// 	LocalMD5              sql.NullString
// 	BlobMD5               sql.NullString
// 	MD5VerificationDate   sql.NullTime
// 	BlobContainerName     sql.NullString
// 	BlobPath              sql.NullString
// 	ExportAccount         sql.NullString
// 	ExportConnectivityAccount sql.NullString
// 	ExportStorageAccount  sql.NullString
// 	IndexName             sql.NullString
// 	IndexLocalMD5         sql.NullString
// 	IndexBlobMD5          sql.NullString

// 	// Fields from table_bom
// 	HYP_INVREFNO  sql.NullString
// 	HYP_SFCNAME   sql.NullString
// 	HYP_SFCNR     sql.NullFloat64
// 	HYP_VOUNO     sql.NullString
// 	HYP_REMARK    sql.NullString
// 	HYP_VOUDAT    sql.NullTime
// 	HYP_ACCOUNT   sql.NullFloat64
// 	HYP_YMCODE2   sql.NullFloat64
// 	HYP_VESSEL    sql.NullFloat64
// 	HYP_VOYAGE    sql.NullFloat64
// 	HYP_COSTCENTER sql.NullFloat64
// 	HYP_AREA      sql.NullFloat64
// 	HYP_MONTH     sql.NullFloat64
// 	HYP_AGENT     sql.NullFloat64
// 	HYP_ARCHDAT   sql.NullTime
// 	MIG_STATUS    sql.NullString
// 	MIG_HASH      sql.NullString
// }

// func parseNullInt64(value string) sql.NullInt64 {
// 	v, err := strconv.ParseInt(value, 10, 64)
// 	if err != nil {
// 		return sql.NullInt64{}
// 	}
// 	return sql.NullInt64{Int64: v, Valid: true}
// }

// func parseNullFloat64(value string) sql.NullFloat64 {
// 	v, err := strconv.ParseFloat(value, 64)
// 	if err != nil {
// 		return sql.NullFloat64{}
// 	}
// 	return sql.NullFloat64{Float64: v, Valid: true}
// }

// func parseNullString(value string) sql.NullString {
// 	return sql.NullString{String: value, Valid: value != ""}
// }

// func parseNullTime(value string) sql.NullTime {
// 	layout := "2006-01-02 15:04:05.0000000"
// 	t, err := time.Parse(layout, value)
// 	if err != nil {
// 		return sql.NullTime{}
// 	}
// 	return sql.NullTime{Time: t, Valid: true}
// }

// func readCOC(filePath string) ([]table_coc, error) {
// 	file, err := os.Open(filePath)
// 	if err != nil {
// 		return nil, err
// 	}
// 	defer file.Close()

// 	reader := csv.NewReader(file)
// 	records, err := reader.ReadAll()
// 	if err != nil {
// 		return nil, err
// 	}

// 	var data []table_coc
// 	for _, record := range records[1:] {
// 		row := table_coc{
// 			DocID:                 parseNullInt64(record[0]),
// 			ExportDate:            parseNullTime(record[1]),
// 			DocumentType:          parseNullString(record[2]),
// 			DocumentName:          parseNullString(record[3]),
// 			LocalSize:             parseNullInt64(record[4]),
// 			BlobSize:              parseNullInt64(record[5]),
// 			LocalMD5:              parseNullString(record[6]),
// 			BlobMD5:               parseNullString(record[7]),
// 			MD5VerificationDate:   parseNullTime(record[8]),
// 			BlobContainerName:     parseNullString(record[9]),
// 			BlobPath:              parseNullString(record[10]),
// 			ExportAccount:         parseNullString(record[11]),
// 			ExportConnectivityAccount: parseNullString(record[12]),
// 			ExportStorageAccount:  parseNullString(record[13]),
// 			IndexName:             parseNullString(record[14]),
// 			IndexLocalMD5:         parseNullString(record[15]),
// 			IndexBlobMD5:          parseNullString(record[16]),
// 		}
// 		data = append(data, row)
// 	}
// 	return data, nil
// }

// func readBOM(filePath string) ([]table_bom, error) {
// 	file, err := os.Open(filePath)
// 	if err != nil {
// 		return nil, err
// 	}
// 	defer file.Close()

// 	reader := csv.NewReader(file)
// 	records, err := reader.ReadAll()
// 	if err != nil {
// 		return nil, err
// 	}

// 	var data []table_bom
// 	for _, record := range records[1:] {
// 		row := table_bom{
// 			DOCUMENTID:    parseNullInt64(record[0]),
// 			HYP_INVREFNO:  parseNullString(record[1]),
// 			HYP_SFCNAME:   parseNullString(record[2]),
// 			HYP_SFCNR:     parseNullFloat64(record[3]),
// 			HYP_VOUNO:     parseNullString(record[4]),
// 			HYP_REMARK:    parseNullString(record[5]),
// 			HYP_VOUDAT:    parseNullTime(record[6]),
// 			HYP_ACCOUNT:   parseNullFloat64(record[7]),
// 			HYP_YMCODE2:   parseNullFloat64(record[8]),
// 			HYP_VESSEL:    parseNullFloat64(record[9]),
// 			HYP_VOYAGE:    parseNullFloat64(record[10]),
// 			HYP_COSTCENTER: parseNullFloat64(record[11]),
// 			HYP_AREA:      parseNullFloat64(record[12]),
// 			HYP_MONTH:     parseNullFloat64(record[13]),
// 			HYP_AGENT:     parseNullFloat64(record[14]),
// 			HYP_ARCHDAT:   parseNullTime(record[15]),
// 			MIG_STATUS:    parseNullString(record[16]),
// 			MIG_HASH:      parseNullString(record[17]),
// 		}
// 		data = append(data, row)
// 	}
// 	return data, nil
// }

// func joinData(file1 , file2 string)([]combinedData, error){
// 		coc_tb,err  := readCOC(file1)
// 		if err != nil {
// 			return nil, err
// 		}
// 		bom_tb,err  := readBOM(file2)
// 		if err != nil {
// 			return nil, err
// 		}
// 		var joinedData []combinedData
// 		for _, record := range coc_tb {
// 			for _, record2 := range bom_tb {
// 				if record.DocID == record2.DOCUMENTID {
// 					var row combinedData
// 					row.DocID = record.DocID
// 					row.ExportDate = record.ExportDate
// 					row.DocumentType = record.DocumentType
// 					row.DocumentName = record.DocumentName
// 					row.LocalSize = record.LocalSize
// 					row.BlobSize = record.BlobSize
// 					row.LocalMD5 = record.LocalMD5
// 					row.BlobMD5 = record.BlobMD5
// 					row.MD5VerificationDate = record.MD5VerificationDate
// 					row.BlobContainerName = record.BlobContainerName
// 					row.BlobPath = record.BlobPath
// 					row.ExportAccount = record.ExportAccount
// 					row.ExportConnectivityAccount = record.ExportConnectivityAccount
// 					row.ExportStorageAccount = record.ExportStorageAccount
// 					row.IndexName = record.IndexName
// 					row.IndexLocalMD5 = record.IndexLocalMD5
// 					row.IndexBlobMD5 = record.IndexBlobMD5
// 					row.HYP_INVREFNO = record2.HYP_INVREFNO
// 					row.HYP_SFCNAME = record2.HYP_SFCNAME
// 					row.HYP_SFCNR = record2.HYP_SFCNR
// 					row.HYP_VOUNO = record2.HYP_VOUNO
// 					row.HYP_REMARK = record2.HYP_REMARK
// 					row.HYP_VOUDAT = record2.HYP_VOUDAT
// 					row.HYP_ACCOUNT = record2.HYP_ACCOUNT
// 					row.HYP_YMCODE2 = record2.HYP_YMCODE2
// 					row.HYP_VESSEL = record2.HYP_VESSEL
// 					row.HYP_VOYAGE = record2.HYP_VOYAGE
// 					row.HYP_COSTCENTER = record2.HYP_COSTCENTER
// 					row.HYP_AREA = record2.HYP_AREA
// 					row.HYP_MONTH = record2.HYP_MONTH
// 					row.HYP_AGENT = record2.HYP_AGENT
// 					row.HYP_ARCHDAT = record2.HYP_ARCHDAT
// 					row.MIG_STATUS = record2.MIG_STATUS
// 					row.MIG_HASH = record2.MIG_HASH
// 					joinedData = append(joinedData, row)
	
// 				}
	
// 			}
// 		}
// 		return joinedData, nil
// 	}

// 	func struct_test() {
// 		type S struct {
// 			a int
// 			b int
// 		}
// 		var s S
// 		s.a = 1
// 		s.b = 2
// 		fmt.Println(s)
	
// 	}
	
	
// 	func main(){
// 		file1 := "COC.csv"
// 		file2 := "bom.csv"
	
// 		joinedData, err := joinData(file1, file2)
// 		if err != nil {
// 			fmt.Println("Error:", err)
// 			return
// 		}
// 		fmt.Println(joinedData[0])
// 		struct_test()
// 	}
	



//aother way to join data
//joining data
// func joinAndPrintData(db1Data []table_coc, db2Data []table_bom) {

// 	db2Map := make(map[int64]table_bom)
// 	for _, row := range db2Data {
// 		db2Map[row.DOCUMENTID] = row
// 	}
	

// 	var combinedData []combinedTable
// 	for _, row1 := range db1Data {
// 		if row2, exists := db2Map[row1.DocID]; exists {
// 			combinedRow := combinedTable{
// 				DocID:                 sql.NullInt64{Int64: row1.DocID, Valid: true},
// 				ExportDate:            row1.ExportDate,
// 				DocumentType:          row1.DocumentType,
// 				DocumentName:          row1.DocumentName,
// 				LocalSize:             row1.LocalSize,
// 				BlobSize:              row1.BlobSize,
// 				LocalMD5:              row1.LocalMD5,
// 				BlobMD5:               row1.BlobMD5,
// 				MD5VerificationDate:   row1.MD5VerificationDate,
// 				BlobContainerName:     row1.BlobContainerName,
// 				BlobPath:              row1.BlobPath,
// 				ExportAccount:         row1.ExportAccount,
// 				ExportConnectivityAccount: row1.ExportConnectivityAccount,
// 				ExportStorageAccount:  row1.ExportStorageAccount,
// 				IndexName:             row1.IndexName,
// 				IndexLocalMD5:         row1.IndexLocalMD5,
// 				IndexBlobMD5:          row1.IndexBlobMD5,
// 				HYP_INVREFNO:          row2.HYP_INVREFNO,
// 				HYP_SFCNAME:           row2.HYP_SFCNAME,
// 				HYP_SFCNR:             row2.HYP_SFCNR,
// 				HYP_VOUNO:             row2.HYP_VOUNO,
// 				HYP_REMARK:            row2.HYP_REMARK,
// 				HYP_VOUDAT:            row2.HYP_VOUDAT,
// 				HYP_ACCOUNT:           row2.HYP_ACCOUNT,
// 				HYP_YMCODE2:           row2.HYP_YMCODE2,
// 				HYP_VESSEL:            row2.HYP_VESSEL,
// 				HYP_VOYAGE:            row2.HYP_VOYAGE,
// 				HYP_COSTCENTER:        row2.HYP_COSTCENTER,
// 				HYP_AREA:              row2.HYP_AREA,
// 				HYP_MONTH:             row2.HYP_MONTH,
// 				HYP_AGENT:             row2.HYP_AGENT,
// 				HYP_ARCHDAT:           row2.HYP_ARCHDAT,
// 				MIG_STATUS:            row2.MIG_STATUS,
// 				MIG_HASH:              row2.MIG_HASH,
// 			}
// 			combinedData = append(combinedData, combinedRow)
// 		}
// 	}
	

	
	
	
// fmt.Println("Combined Data:")
// for i, row := range combinedData {
// 	fmt.Printf("Row %d:DocID: %d, ExportDate: %v, DocumentType: %v, DocumentName: %v, LocalSize: %v, BlobSize: %v, LocalMD5: %v, BlobMD5: %v, MD5VerificationDate: %v, BlobContainerName: %v, BlobPath: %v, ExportAccount: %v, ExportConnectivityAccount: %v, ExportStorageAccount: %v, IndexName: %v, IndexLocalMD5: %v, IndexBlobMD5: %v | BOM - HYP_INVREFNO: %v, HYP_SFCNAME: %v, HYP_SFCNR: %v, HYP_VOUNO: %v, HYP_REMARK: %v, HYP_VOUDAT: %v, HYP_ACCOUNT: %v, HYP_YMCODE2: %v, HYP_VESSEL: %v, HYP_VOYAGE: %v, HYP_COSTCENTER: %v, HYP_AREA: %v, HYP_MONTH: %v, HYP_AGENT: %v, HYP_ARCHDAT: %v, MIG_STATUS: %v, MIG_HASH: %v\n",
// 		i+1, row.DocID.Int64, row.ExportDate.Time, row.DocumentType.String, row.DocumentName.String, row.LocalSize.Int64, row.BlobSize.Int64, row.LocalMD5.String, row.BlobMD5.String, row.MD5VerificationDate.Time, row.BlobContainerName.String, row.BlobPath.String, row.ExportAccount.String, row.ExportConnectivityAccount.String, row.ExportStorageAccount.String, row.IndexName.String, row.IndexLocalMD5.String, row.IndexBlobMD5.String,
// 		row.HYP_INVREFNO.String, row.HYP_SFCNAME.String, row.HYP_SFCNR.Float64, row.HYP_VOUNO.String, row.HYP_REMARK.String, row.HYP_VOUDAT.Time, row.HYP_ACCOUNT.Float64, row.HYP_YMCODE2.Float64, row.HYP_VESSEL.Float64, row.HYP_VOYAGE.Float64, row.HYP_COSTCENTER.Float64, row.HYP_AREA.Float64, row.HYP_MONTH.Float64, row.HYP_AGENT.Float64, row.HYP_ARCHDAT.Time, row.MIG_STATUS.String, row.MIG_HASH.String)
// }


// }







// func addMetadata(){
// 	file, err := os.Open("folder_id_map.csv")
// 	if err != nil {
// 		log.Fatalf("Error opening file: %v", err)
// 	}
// 	defer file.Close()

// 	reader := csv.NewReader(file)
// 	data := make(map[string]string)
// 	docs, err := reader.ReadAll()
// 	if err != nil {
// 		log.Fatalf("Error reading CSV file: %v", err)
// 	}

// 	if len(docs) == 0 {
// 		log.Fatal("CSV file is empty")
// 	}
// 	for _, record := range docs {
// 		data[record[0]] = record[1] // DocID -> RefID
// 	}

// 	file2, err := os.Open("folder_id_map.csv")
// 	if err != nil {
// 		log.Fatalf("Error opening file: %v", err)
// 	}
// 	defer file2.Close()
	


// 	reader2 := csv.NewReader(file2)
// 	records, err := reader2.Read()
// 	if err != nil {
// 		log.Fatalf("Error reading CSV file: %v", err)
// 	}

// 	if len(records) == 0 {
// 		log.Fatal("CSV file is empty")
// 	}

// 	for _,record := range(records[1:]){
		
// 		docID  := record[0]
// 		refID, exists := data[docID]
// 		if !exists {
// 			refID = "N/A" // Handle missing RefID
// 		}

		
// 	}


// }

// func processCSVData() error {
// 	// Open and read folder_id_map.csv
// 	folderFile, err := os.Open("folder_id_map.csv")
// 	if err != nil {
// 		return fmt.Errorf("error opening %s: %v", "folderIDMapFile", err)
// 	}
// 	defer folderFile.Close()

// 	folderReader := csv.NewReader(folderFile)
// 	folderRecords, err := folderReader.ReadAll()
// 	if err != nil {
// 		return fmt.Errorf("error reading %s: %v", "folderIDMapFile", err)
// 	}

// 	if len(folderRecords) == 0 {
// 		return fmt.Errorf("%s is empty", "folderIDMapFile")
// 	}

// 	// Populate map[DocID] -> RefID from folder_id_map.csv
// 	folderIDMap := make(map[string]string)
// 	for _, record := range folderRecords {
// 		folderIDMap[record[0]] = record[1]
// 	}

// 	// Open and read combined_data.csv
// 	dataFile, err := os.Open("combined_data.csv")
// 	if err != nil {
// 		return fmt.Errorf("error opening %s: %v", "combinedDataFile", err)
// 	}
// 	defer dataFile.Close()

// 	dataReader := csv.NewReader(dataFile)
// 	// header, err := dataReader.Read() // Read and store header
// 	// if err != nil {
// 	// 	return fmt.Errorf("error reading header from %s: %v", "combinedDataFile", err)
// 	// }

// 	records, err := dataReader.ReadAll() // Read all data rows
// 	if err != nil {
// 		return fmt.Errorf("error reading %s: %v", "combinedDataFile", err)
// 	}

// 	if len(records) == 0 {
// 		return fmt.Errorf("%s is empty", "combinedDataFile")
// 	}

// 	// Print header with DocID and RefID included
// 	// fmt.Println("DocID,RefID," + header[1] + "," + header[3])

// 	// Iterate through the records and print the relevant data
// 	for _, record := range records[1:10] {
// 		docID := record[0]
// 		refID, exists := folderIDMap[docID]
// 		if !exists {
// 			refID = "N/A"
// 		}
// 		fmt.Printf("%s,%s,%s,%s\n", docID, refID, record[1], record[3]) // Print selected columns
// 	}
// 	return nil
// }










// func addMetadata(folderData []folderdata){
// 	tenantID := os.Getenv("tenant_id")
// 	token := os.Getenv("TOKEN")
// 	type Metadata struct {
// 		AttributeID string `json:"attributeId"`
// 		Value       string `json:"value"`
// 	}

// 	type BodyStruct struct {
// 		ObjectID string     `json:"objectId"`
// 		Metadata []Metadata `json:"metadata"`
// 	}

// 	for _, record := range folderData{
// 		refId := record.ReferenceID
// 		exportdata := BodyStruct{
// 			ObjectID: refId,
// 			Metadata: []Metadata{
// 				{
// 					AttributeID: attribute_id_map["ExportDate"],
// 					Value:       record.ExportDate,

// 				},
// 			},

// 		}
// 		requestBody, err := json.Marshal(exportdata)
// 	if err != nil {
// 		// http.Error(w, "Error marshalling request body", http.StatusInternalServerError)
// 		fmt.Errorf("Error marshalling request body: %v", err)
		
// 	}

// 	//  POST request
// 	URL := os.Getenv("META_DATA_URL") + "/metadata"
// 	request, err := http.NewRequest("POST", URL, bytes.NewBuffer(requestBody))
// 	if err != nil {
// 		// http.Error(w, err.Error(), http.StatusInternalServerError)
// 		fmt.Errorf("Error creating POST request: %v", err)
// 	}
// 	request.Header.Set("Authorization", fmt.Sprintf("Bearer %s", token))
// 	request.Header.Set("x-tenant-id", tenantID)
// 	request.Header.Set("Content-Type", "application/json")

// 	client1 := &http.Client{
// 		Timeout: 10 * time.Second,
// 	}
// 	response, err := client1.Do(request)
// 	if err != nil {
// 		fmt.Errorf("Error making POST request: %v", err)
// 	}
// 	defer response.Body.Close()

// 	// return the response from the POST request
// 	if err != nil {
// 		fmt.Errorf("Error reading response body: %v", err)
// 	}
// 	if response.StatusCode != http.StatusCreated {
// 		fmt.Println(response)
// 		 fmt.Errorf("unexpected status code: %d", response.StatusCode)
// 	}

	

// 	}
// }