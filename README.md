# A tool for migration the databases to MySQL

It's a Go console utility for migration from MSSQL to MySQL engine. The databases should have prepopulated schemas. The tool only copies data. The first parameter is a connection string for a source MSSQL database, and the second is MySQL destination.

## Supported types:
| MSSQL Type       | MySQL Type |
|------------------|------------|
| INT              | INT        |
| DATE             | DATE       |
| NCHAR            | NCHAR      |
| DECIMAL          | DECIMAL    |
| FLOAT            | FLOAT      |
| UNIQUEIDENTIFIER | CHAR(36)   |
| VARBINARY        | VARBINARY  |

## Run examples:
```bash
mysql-migration "sqlserver://user:pass@hostname/instance?database=dbname" "username:password@protocol(address)/dbname?param=value"
```

*See driver's pages for more information about the connection strings:*
* https://github.com/denisenkom/go-mssqldb
* https://github.com/go-sql-driver/mysql

## Tests

`go test` is used for testing. A running instances of MSSQL and MySQL servers is required. Environment variables are used to pass login information. The databases are being dropped and recreated before the tests if they exist and left alive after.

Example:

```bash
    env MSSQLSERVER_DSN="server=localhost;user id=sa;password={foo;bar};database=dbname" MYSQLSERVER_DSN="username:password@protocol(address)/dbname?param=value" go test
```