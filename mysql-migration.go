package mysql_migration

import (
	"database/sql"
	"errors"
	_ "errors"
	"fmt"
	_ "github.com/denisenkom/go-mssqldb"
	_ "github.com/go-sql-driver/mysql"
	"os"
	"sort"
	"strings"
	"time"
	_ "time"
)

type TableDefinition struct {
	Name    string
	Type    string
	columns []ColumnDefinition
}

type ColumnDefinition struct {
	Name string
	Type string
}

func main() {
	sourceUrl := os.Args[0]
	destinationUrl := os.Args[1]
	err := migrateDatabase(sourceUrl, destinationUrl)
	if err != nil {
		fmt.Println(err)
		return
	}
}

func migrateDatabase(sourceUrl string, destinationUrl string) error {
	mssqlDb, mysqlDb, err := tryGetConnections(sourceUrl, destinationUrl)
	if err != nil {
		return err
	}
	defer mssqlDb.Close()
	defer mysqlDb.Close()

	mssqlTables, mysqlTables, err := getAndValidateSchemas(mssqlDb, mysqlDb)
	if err != nil {
		return err
	}

	err = migrateData(mssqlDb, mysqlDb, mssqlTables, mysqlTables)
	if err != nil {
		return err
	}

	return nil
}

func migrateData(mssqlDb *sql.DB, mysqlDb *sql.DB, mssqlTables []TableDefinition, mysqlTables []TableDefinition) error {
	_, err := mysqlDb.Exec(`SET SESSION FOREIGN_KEY_CHECKS=0;`)

	if err != nil {
		return err
	}

	for tableIndex, mssqlTable := range mssqlTables {
		mysqlTable := mysqlTables[tableIndex]

		res, err := mssqlDb.Query(fmt.Sprintf(`SELECT * FROM %s;`, mssqlTable.Name))

		if err != nil {
			return err
		}

		fields, _ := res.Columns()
		scans := make([]interface{}, len(fields))

		rowsCount := 0
		for res.Next() {
			for i := range scans {
				scans[i] = &scans[i]
			}
			res.Scan(scans...)

			columnsValue := make([]string, len(mysqlTable.columns))
			for i, v := range scans {
				if v != nil {
					switch t := v.(type) {
					case int, int64:
						columnsValue[i] = fmt.Sprint(t)
					case float32, float64:
						columnsValue[i] = fmt.Sprint(t)
					case time.Time:
						columnsValue[i] = fmt.Sprintf("'%s'", t.Format("2006-01-02 15:04:05.000000"))
					case string:
						escapedString := strings.TrimRight(t, " ")
						escapedString = strings.Replace(escapedString, "\\", "\\\\", -1)
						escapedString = strings.Replace(escapedString, "'", "\\'", -1)
						columnsValue[i] = fmt.Sprintf("'%s'", escapedString)
					case []uint8:
						byteArray := v.([]uint8)
						switch mssqlTable.columns[i].Type {
						case "uniqueidentifier":
							//mssql returns the GUID with the first half flipped
							//https://stackoverflow.com/questions/38160945/ms-sql-uniqueidentifier-with-golang-sql-driver-and-uuid
							columnsValue[i] = fmt.Sprintf("'%02x%02x%02x%02x-%02x%02x-%02x%02x-%02x%02x-%02x%02x%02x%02x%02x%02x'",
								byteArray[3], byteArray[2], byteArray[1], byteArray[0],
								byteArray[5], byteArray[4],
								byteArray[7], byteArray[6],
								byteArray[8], byteArray[9],
								byteArray[10], byteArray[11], byteArray[12], byteArray[13], byteArray[14], byteArray[15])
						case "varbinary":
							if len(byteArray) > 0 {
								columnsValue[i] = fmt.Sprintf("0x%x", byteArray)
							} else {
								columnsValue[i] = "0x0"
							}
						case "decimal":
							columnsValue[i] = string(t)
						}
					default:
						columnsValue[i] = "NULL"
					}
				}
			}

			columnsName := make([]string, len(mysqlTable.columns))
			for i, column := range mysqlTable.columns {
				columnsName[i] = "`" + column.Name + "`"
			}

			sqlInsertText := fmt.Sprintf(`INSERT INTO %s (%s) VALUES(%s);`,
				"`"+mysqlTable.Name+"`",
				strings.Join(columnsName, ", "),
				strings.Join(columnsValue, ", "))

			_, err = mysqlDb.Exec(sqlInsertText)

			if err != nil {
				return err
			}

			rowsCount++
		}
		fmt.Printf("Table %s migrated (%d/%d); Rows: %d;\n", mssqlTable.Name, tableIndex+1, len(mysqlTables), rowsCount)
	}
	return nil
}

func tryGetConnections(sourceUrl string, destinationUrl string) (*sql.DB, *sql.DB, error) {
	mssqlDb, err := sql.Open("mssql", sourceUrl)
	if err != nil {
		return nil, nil, err
	}

	mysqlDb, err := sql.Open("mysql", destinationUrl)
	if err != nil {
		return nil, nil, err
	}

	return mssqlDb, mysqlDb, nil
}

func getAndValidateSchemas(mssqlDb *sql.DB, mysqlDb *sql.DB) ([]TableDefinition, []TableDefinition, error) {
	mssqlTables, err := getTablesName(mssqlDb,
		`SELECT TABLE_NAME, TABLE_TYPE
						FROM INFORMATION_SCHEMA.TABLES
						WHERE TABLE_TYPE = 'BASE TABLE';`,

		`SELECT COLUMN_NAME, DATA_TYPE
						FROM information_schema.columns
						WHERE TABLE_CATALOG = DB_NAME() AND
								table_name = '%s'
						ORDER BY ORDINAL_POSITION;`)
	if err != nil {
		return nil, nil, err
	}

	mysqlTables, err := getTablesName(mysqlDb,
		"SHOW FULL TABLES WHERE table_type = 'BASE TABLE';",

		`SELECT COLUMN_NAME, DATA_TYPE
						FROM information_schema.columns
						WHERE TABLE_SCHEMA = DATABASE() AND
								table_name = '%s'
						ORDER BY ORDINAL_POSITION;`)
	if err != nil {
		return nil, nil, err
	}

	if len(mysqlTables) != len(mssqlTables) {
		return nil, nil, errors.New("databases' count of tables are different")
	}

	for i := 0; i < len(mysqlTables); i++ {
		if strings.ToLower(mysqlTables[i].Name) != strings.ToLower(mssqlTables[i].Name) {
			return nil, nil, errors.New("database's tables are different")
		}
		if len(mysqlTables[i].columns) != len(mssqlTables[i].columns) {
			return nil, nil, errors.New("databases' count of columns are different")
		}
	}

	return mssqlTables, mysqlTables, nil
}

func getTablesName(dB *sql.DB, sqlGetTablesList string, sqlGetColumnsList string) ([]TableDefinition, error) {
	res, err := dB.Query(sqlGetTablesList)
	defer res.Close()

	if err != nil {
		return nil, err
	}

	var tables []TableDefinition
	for res.Next() {
		var table TableDefinition
		err := res.Scan(&table.Name, &table.Type)

		if err != nil {
			return nil, err
		}
		columns, err := getColumns(dB, fmt.Sprintf(sqlGetColumnsList, table.Name))
		if err != nil {
			return nil, err
		}
		table.columns = columns

		tables = append(tables, table)
	}

	sort.Slice(tables, func(i, j int) bool { return strings.ToLower(tables[i].Name) < strings.ToLower(tables[j].Name) })

	return tables, nil
}

func getColumns(dB *sql.DB, sqlGetColumnsList string) ([]ColumnDefinition, error) {
	res, err := dB.Query(sqlGetColumnsList)
	defer res.Close()

	if err != nil {
		return nil, err
	}

	var columns []ColumnDefinition
	for res.Next() {
		var column ColumnDefinition
		err := res.Scan(&column.Name, &column.Type)

		if err != nil {
			return nil, err
		}
		columns = append(columns, column)
	}

	return columns, nil
}
