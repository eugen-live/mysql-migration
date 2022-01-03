package mysql_migration

import (
	"database/sql"
	_ "database/sql"
	"fmt"
	_ "github.com/denisenkom/go-mssqldb"
	"github.com/denisenkom/go-mssqldb/msdsn"
	_ "github.com/denisenkom/go-mssqldb/msdsn"
	"github.com/go-sql-driver/mysql"
	_ "github.com/go-sql-driver/mysql"
	"log"
	"os"
	"strings"
	"testing"
)

func TestGetAndValidateSchemasSuccess(t *testing.T) {
	mssqlDsn, mysqlDsn, err := prepareDatabases()
	if err != nil {
		t.Error(err.Error())
		return
	}

	mssqlDb, mysqlDb, _ := tryGetConnections(mssqlDsn, mysqlDsn)
	defer mssqlDb.Close()
	defer mysqlDb.Close()

	_, err = mssqlDb.Exec(`CREATE TABLE Documents
								(
									CompanyID   INT  NOT NULL,
									DocumentID  INT  NOT NULL,
									DocDate     DATE NOT NULL,
									Description NCHAR(255),
									PRIMARY KEY (CompanyID, DocumentID)
								);`)
	if err != nil {
		t.Error(err.Error())
		return
	}

	_, err = mysqlDb.Exec(`CREATE TABLE Documents
								(
									CompanyID   INT  NOT NULL,
									DocumentID  INT  NOT NULL,
									DocDate     DATE NOT NULL,
									Description NCHAR(255),
									PRIMARY KEY (CompanyID, DocumentID)
								);`)
	if err != nil {
		t.Error(err.Error())
		return
	}

	mssqlTables, mysqlTables, _ := getAndValidateSchemas(mssqlDb, mysqlDb)

	if len(mssqlTables) != 1 {
		t.Error("Validation got an incorrect count of tables from MsSQL")
		return
	}

	if len(mysqlTables) != 1 {
		t.Error("Validation got an incorrect count of tables from MySQL")
		return
	}

	if len(mssqlTables[0].columns) != 4 {
		t.Error("Validation got an incorrect count of columns from MsSQL")
		return
	}

	if len(mysqlTables[0].columns) != 4 {
		t.Error("Validation got an incorrect count of columns from MySQL")
		return
	}

	if strings.ToLower(mssqlTables[0].Name) != "documents" {
		t.Error("Validation got an incorrect name of table from MsSQL")
		return
	}

	if strings.ToLower(mysqlTables[0].Name) != "documents" {
		t.Error("Validation got an incorrect name of table from MySQL")
		return
	}

	if mssqlTables[0].columns[0].Name != "CompanyID" ||
		mssqlTables[0].columns[1].Name != "DocumentID" ||
		mssqlTables[0].columns[2].Name != "DocDate" ||
		mssqlTables[0].columns[3].Name != "Description" {
		t.Error("Incorrect columns in MsSQL")
		return
	}

	if mysqlTables[0].columns[0].Name != "CompanyID" ||
		mysqlTables[0].columns[1].Name != "DocumentID" ||
		mysqlTables[0].columns[2].Name != "DocDate" ||
		mysqlTables[0].columns[3].Name != "Description" {
		t.Error("Incorrect columns in MySQL")
		return
	}
}

func TestGetAndValidateSchemasDifferentColumnsFail(t *testing.T) {
	mssqlDsn, mysqlDsn, err := prepareDatabases()
	if err != nil {
		t.Error(err.Error())
		return
	}

	mssqlDb, mysqlDb, _ := tryGetConnections(mssqlDsn, mysqlDsn)
	defer mssqlDb.Close()
	defer mysqlDb.Close()

	_, err = mssqlDb.Exec(`CREATE TABLE Documents
								(
									CompanyID   INT  NOT NULL,
									DocumentID  INT  NOT NULL,
									DocDate     DATE NOT NULL,
									Description NCHAR(255),
									PRIMARY KEY (CompanyID, DocumentID)
								);`)
	if err != nil {
		t.Error(err.Error())
		return
	}

	_, err = mysqlDb.Exec(`CREATE TABLE Documents
								(
									CompanyID   INT  NOT NULL,
									DocumentID  INT  NOT NULL,
									DocDate     DATE NOT NULL,
									PRIMARY KEY (CompanyID, DocumentID)
								);`)
	if err != nil {
		t.Error(err.Error())
		return
	}

	_, _, err = getAndValidateSchemas(mssqlDb, mysqlDb)
	if err == nil || err.Error() != "databases' count of columns are different" {
		t.Error("Validation shouldn't pass due to different count of columns")
		return
	}
}

func TestGetAndValidateSchemasDifferentTablesCountFail(t *testing.T) {
	mssqlDsn, mysqlDsn, err := prepareDatabases()
	if err != nil {
		t.Error(err.Error())
		return
	}

	mssqlDb, mysqlDb, _ := tryGetConnections(mssqlDsn, mysqlDsn)
	defer mssqlDb.Close()
	defer mysqlDb.Close()

	_, err = mssqlDb.Exec(`CREATE TABLE Documents
								(
									CompanyID   INT  NOT NULL,
									DocumentID  INT  NOT NULL,
									DocDate     DATE NOT NULL,
									Description NCHAR(255),
									PRIMARY KEY (CompanyID, DocumentID)
								);`)
	if err != nil {
		t.Error(err.Error())
		return
	}

	_, err = mssqlDb.Exec(`CREATE TABLE Documents1
								(
									CompanyID   INT  NOT NULL,
									DocumentID  INT  NOT NULL,
									DocDate     DATE NOT NULL,
									Description NCHAR(255),
									PRIMARY KEY (CompanyID, DocumentID)
								);`)
	if err != nil {
		t.Error(err.Error())
		return
	}

	_, err = mysqlDb.Exec(`CREATE TABLE Documents
								(
									CompanyID   INT  NOT NULL,
									DocumentID  INT  NOT NULL,
									DocDate     DATE NOT NULL,
									Description NCHAR(255),
									PRIMARY KEY (CompanyID, DocumentID)
								);`)
	if err != nil {
		t.Error(err.Error())
		return
	}

	_, _, err = getAndValidateSchemas(mssqlDb, mysqlDb)
	if err == nil || err.Error() != "databases' count of tables are different" {
		t.Error("Validation shouldn't pass due to different count of tables")
		return
	}
}

func TestGetAndValidateSchemasDifferentTablesNameFail(t *testing.T) {
	mssqlDsn, mysqlDsn, err := prepareDatabases()
	if err != nil {
		t.Error(err.Error())
		return
	}

	mssqlDb, mysqlDb, _ := tryGetConnections(mssqlDsn, mysqlDsn)
	defer mssqlDb.Close()
	defer mysqlDb.Close()

	_, err = mssqlDb.Exec(`CREATE TABLE Documents
								(
									CompanyID   INT  NOT NULL,
									DocumentID  INT  NOT NULL,
									DocDate     DATE NOT NULL,
									Description NCHAR(255),
									PRIMARY KEY (CompanyID, DocumentID)
								);`)
	if err != nil {
		t.Error(err.Error())
		return
	}

	_, err = mysqlDb.Exec(`CREATE TABLE Documents1
								(
									CompanyID   INT  NOT NULL,
									DocumentID  INT  NOT NULL,
									DocDate     DATE NOT NULL,
									Description NCHAR(255),
									PRIMARY KEY (CompanyID, DocumentID)
								);`)
	if err != nil {
		t.Error(err.Error())
		return
	}

	_, _, err = getAndValidateSchemas(mssqlDb, mysqlDb)
	if err == nil || err.Error() != "database's tables are different" {
		t.Error("Validation shouldn't pass due to different names of tables")
		return
	}
}

type Document struct {
	CompanyID   int
	DocumentID  int
	DocDate     string
	Description string
}

func TestMigrateDatabase(t *testing.T) {
	mssqlDsn, mysqlDsn, err := prepareDatabases()
	if err != nil {
		t.Error(err.Error())
		return
	}
	TestGetAndValidateSchemasSuccess(t)
	mssqlDb, mysqlDb, _ := tryGetConnections(mssqlDsn, mysqlDsn)
	defer mssqlDb.Close()
	defer mysqlDb.Close()

	documents := []Document{{1, 0, "1970-02-04", "Description 1"},
		{1, 1, "1993-01-27", "Description 2"},
		{1, 2, "2016-06-04", "Description 3"}}
	for _, doc := range documents {
		_, err = mssqlDb.Exec(`INSERT INTO Documents (CompanyID, DocumentID, DocDate, Description) 
												VALUES (?, ?, ?, ?);`,
			doc.CompanyID, doc.DocumentID, doc.DocDate, doc.Description)

		if err != nil {
			t.Error(err.Error())
			return
		}
	}

	migrateDatabase(mssqlDsn, mysqlDsn)

	rows, err := mysqlDb.Query(`SELECT CompanyID, DocumentID, DocDate, Description FROM Documents`)

	if err != nil {
		t.Error(err.Error())
		return
	}

	var migratedDocuments []Document
	for rows.Next() {
		var doc Document
		rows.Scan(&doc.CompanyID, &doc.DocumentID, &doc.DocDate, &doc.Description)
		migratedDocuments = append(migratedDocuments, doc)
	}

	if len(documents) != len(migratedDocuments) {
		t.Error("Not all documents migrated")
		return
	}

	for i, doc := range documents {
		migratedDoc := migratedDocuments[i]
		if doc != migratedDoc {
			t.Error("Document migrated incorrect")
		}
	}
}

func TestMigrationEscapedCharacters(t *testing.T) {
	mssqlDsn, mysqlDsn, err := prepareDatabases()
	if err != nil {
		t.Error(err.Error())
		return
	}
	TestGetAndValidateSchemasSuccess(t)
	mssqlDb, mysqlDb, _ := tryGetConnections(mssqlDsn, mysqlDsn)
	defer mssqlDb.Close()
	defer mysqlDb.Close()

	documents := []Document{{1, 0, "1970-02-04", `Description with 'quotes' 1`},
		{1, 1, "1993-01-27", `Description \ 2`},
		{1, 2, "2016-06-04", `Description '/', '\', '.' 3`}}
	for _, doc := range documents {
		_, err = mssqlDb.Exec(`INSERT INTO Documents (CompanyID, DocumentID, DocDate, Description) 
												VALUES (?, ?, ?, ?);`,
			doc.CompanyID, doc.DocumentID, doc.DocDate, doc.Description)

		if err != nil {
			t.Error(err.Error())
			return
		}
	}

	migrateDatabase(mssqlDsn, mysqlDsn)

	rows, err := mysqlDb.Query(`SELECT CompanyID, DocumentID, DocDate, Description FROM Documents`)

	if err != nil {
		t.Error(err.Error())
		return
	}

	var migratedDocuments []Document
	for rows.Next() {
		var doc Document
		rows.Scan(&doc.CompanyID, &doc.DocumentID, &doc.DocDate, &doc.Description)
		migratedDocuments = append(migratedDocuments, doc)
	}

	if len(documents) != len(migratedDocuments) {
		t.Error("Not all documents migrated")
		return
	}

	for i, doc := range documents {
		migratedDoc := migratedDocuments[i]
		if doc != migratedDoc {
			t.Error("Document migrated incorrect")
		}
	}
}

type DocumentWithAmount struct {
	CompanyID     int
	DocumentID    int
	DecimalAmount string
	FloatAmount   float64
}

func TestMigrationPointTypes(t *testing.T) {
	mssqlDsn, mysqlDsn, err := prepareDatabases()
	if err != nil {
		t.Error(err.Error())
		return
	}

	mssqlDb, mysqlDb, _ := tryGetConnections(mssqlDsn, mysqlDsn)
	defer mssqlDb.Close()
	defer mysqlDb.Close()

	_, err = mssqlDb.Exec(`CREATE TABLE DocumentsWithAmount
								(
									CompanyID   	INT  NOT NULL,
									DocumentID		INT  NOT NULL,
									DecimalAmount 	DECIMAL(6,3) NULL,
									FloatAmount 	FLOAT NULL,
									PRIMARY KEY (CompanyID, DocumentID)
								);`)
	if err != nil {
		t.Error(err.Error())
		return
	}

	_, err = mysqlDb.Exec(`CREATE TABLE DocumentsWithAmount
								(
									CompanyID   	INT  NOT NULL,
									DocumentID		INT  NOT NULL,
									DecimalAmount 	DECIMAL(6,3) NULL,
									FloatAmount 	FLOAT NULL,
									PRIMARY KEY (CompanyID, DocumentID)
								);`)
	if err != nil {
		t.Error(err.Error())
		return
	}

	documents := []DocumentWithAmount{{1, 0, "100.356", 100.356},
		{1, 1, "-100.356", -100.356}}
	for _, doc := range documents {
		_, err = mssqlDb.Exec(`INSERT INTO DocumentsWithAmount (CompanyID, DocumentID, DecimalAmount, FloatAmount) 
												VALUES (?, ?, ?, ?);`,
			doc.CompanyID, doc.DocumentID, doc.DecimalAmount, doc.FloatAmount)

		if err != nil {
			t.Error(err.Error())
			return
		}
	}

	migrateDatabase(mssqlDsn, mysqlDsn)

	rows, err := mysqlDb.Query(`SELECT CompanyID, DocumentID, DecimalAmount, FloatAmount FROM DocumentsWithAmount`)

	if err != nil {
		t.Error(err.Error())
		return
	}

	var migratedDocuments []DocumentWithAmount
	for rows.Next() {
		var doc DocumentWithAmount
		rows.Scan(&doc.CompanyID, &doc.DocumentID, &doc.DecimalAmount, &doc.FloatAmount)
		migratedDocuments = append(migratedDocuments, doc)
	}

	if len(documents) != len(migratedDocuments) {
		t.Error("Not all documents migrated")
		return
	}

	for i, doc := range documents {
		migratedDoc := migratedDocuments[i]
		if doc != migratedDoc {
			t.Error("Document migrated incorrect")
		}
	}
}
func getMsSqlDsnParams(mssqlDsn string) msdsn.Config {
	if len(mssqlDsn) > 0 {
		params, _, err := msdsn.Parse(mssqlDsn)
		if err != nil {
			log.Fatal("unable to parse MSSQLSERVER_DSN", err)
		}
		return params
	}
	return msdsn.Config{}
}

func getMySqlDsnParams(mysqlDsn string) *mysql.Config {
	if len(mysqlDsn) > 0 {
		params, err := mysql.ParseDSN(mysqlDsn)
		if err != nil {
			log.Fatal("unable to parse MYSQLSERVER_DSN", err)
		}
		return params
	}
	return &mysql.Config{}
}

func dropDatabaseIfExists(dB *sql.DB, dbName string) error {
	_, err := dB.Exec(fmt.Sprintf(`DROP DATABASE IF EXISTS %s;`, dbName))

	return err
}

func createDatabase(dB *sql.DB, dbName string) error {
	_, err := dB.Exec(fmt.Sprintf(`CREATE DATABASE %s;`, dbName))

	return err
}

func prepareDatabases() (string, string, error) {
	mssqlDsn := os.Getenv("MSSQLSERVER_DSN")
	mysqlDsn := os.Getenv("MYSQLSERVER_DSN")

	msSqlParams := getMsSqlDsnParams(mssqlDsn)
	mySqlParams := getMySqlDsnParams(mysqlDsn)

	mssqlDbName := msSqlParams.Database
	mysqlDbName := mySqlParams.DBName
	msSqlParams.Database = "master"
	mySqlParams.DBName = "mysql"

	mssqlDb, mysqlDb, _ := tryGetConnections(msSqlParams.URL().String(), mySqlParams.FormatDSN())
	defer mssqlDb.Close()
	defer mysqlDb.Close()

	err := dropDatabaseIfExists(mssqlDb, mssqlDbName)
	if err != nil {
		return "", "", err
	}
	err = dropDatabaseIfExists(mysqlDb, mysqlDbName)
	if err != nil {
		return "", "", err
	}

	err = createDatabase(mssqlDb, mssqlDbName)
	if err != nil {
		return "", "", err
	}
	err = createDatabase(mysqlDb, mysqlDbName)
	if err != nil {
		return "", "", err
	}

	msSqlParams.Database = mssqlDbName
	mySqlParams.DBName = mysqlDbName

	mssqlDb, mysqlDb, _ = tryGetConnections(msSqlParams.URL().String(), mySqlParams.FormatDSN())
	defer mssqlDb.Close()
	defer mysqlDb.Close()
	return mssqlDsn, mysqlDsn, nil
}
