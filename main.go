package main

import (
	"database/sql"
	"fmt"
	"os"
	"strings"
	"text/template"

	go_ora "github.com/sijms/go-ora/v2"
	"github.com/spf13/pflag"
)

const (
	versionStr = "v1.3, update data: 2023-09-13, author: Sun Quan"
)

var (
	oracleUrl   *string   = pflag.String("url", "", "oracle url, e.g. oracle://user:pass@server/service_name")
	username    *string   = pflag.StringP("username", "u", "dbsel", "username to login")
	password    *string   = pflag.StringP("password", "p", "", "password")
	host        *string   = pflag.StringP("host", "h", "localhost", "host")
	port        *int      = pflag.IntP("port", "P", 1521, "port")
	service     *string   = pflag.StringP("service", "s", "", "service")
	selectquery *string   = pflag.StringP("sql", "q", "", "select sql")
	expColNames *[]string = pflag.StringSlice("expCols", nil, "output column names, optional, columus count must be same as select query, default is same as select query. e.g. --expColNames=\"col1,col2,col3\"")
	expTabName  *string   = pflag.String("expTab", "", "output table name, required")
	expUserName *string   = pflag.String("expUser", "", "output user name, required")

	outFile *string = pflag.StringP("outFile", "o", "insert.sql", "output file name")

	version *bool = pflag.BoolP("version", "v", false, "show version")
)

func init() {
	pflag.Parse()
}

func main() {
	if *version {
		fmt.Println(versionStr)
		return
	}

	// check required parameters
	switch {
	case *oracleUrl != "":
		break
	case *password == "":
		panic("password is required")
	case *service == "":
		panic("service is required")
	}
	switch {
	case *selectquery == "":
		panic("select query is required")
	case *expTabName == "":
		panic("expTabName is required")
	}

	// connect to oracle
	var connStr string
	if *oracleUrl != "" {
		connStr = *oracleUrl
	} else {
		connStr = go_ora.BuildUrl(*host, *port, *service, *username, *password, nil)
	}
	fmt.Println("connStr:", connStr)
	conn, err := sql.Open("oracle", connStr)
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	err = conn.Ping()
	if err != nil {
		panic(err)
	}
	fmt.Println("Successfully connected.")

	// execute select query
	*selectquery = strings.TrimSpace(*selectquery)
	if strings.HasSuffix(*selectquery, ";") {
		*selectquery = (*selectquery)[:len(*selectquery)-1]
	}
	rows, err := conn.Query(*selectquery)
	if err != nil {
		panic(err)
	}
	fmt.Println("Successfully execut query:", *selectquery)
	defer rows.Close()

	// generate insert statement
	fmt.Println("Generating insert statement...")
	insertStmts, err := generateInsertStatement(rows, *expColNames, *expTabName, *expUserName)
	if err != nil {
		panic(fmt.Errorf("generateInsertStatement error: %w", err))
	}
	fmt.Println("Successfully generated insert statement.")

	file, err := os.OpenFile(*outFile, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	fmt.Println("writing to", *outFile)
	file.WriteString(insertStmts)
}

func generateInsertStatement(rows *sql.Rows, expColNames []string, expTabName, expUserName string) (string, error) {
	columnNames, err := rows.Columns()
	if err != nil {
		return "", err
	}
	columnCount := len(columnNames)
	if expColNames != nil {
		if len(expColNames) != columnCount {
			return "", fmt.Errorf("expColNames count must be same as select query, expColNames count: %d, select query column count: %d", len(expColNames), columnCount)
		}
	} else {
		expColNames = columnNames
	}

	values := make([]any, columnCount)
	valuePointers := make([]any, columnCount)
	var b strings.Builder

	for rows.Next() {
		for i := 0; i < columnCount; i++ {
			valuePointers[i] = &values[i]
		}

		err := rows.Scan(valuePointers...)
		if err != nil {
			return "", err
		}

		// 构建 INSERT 语句
		tmplstr := `INSERT INTO {{if .User}}{{.User}}.{{end}}{{.Table}} ({{range $index, $col := .Cols}}{{if $index}},{{end}}{{$col}}{{end}}) VALUES ({{range $index, $val := .Vals}}{{if $index}},{{end}}'{{$val}}'{{end}});`
		data := struct {
			Cols  []string
			Vals  []any
			User  string
			Table string
		}{
			Cols:  expColNames,
			Vals:  values,
			Table: expTabName,
			User:  expUserName,
		}
		tmpl, err := template.New("InsertStmt").Parse(tmplstr)
		if err != nil {
			return "", err
		}
		err = tmpl.Execute(&b, data)
		if err != nil {
			return "", err
		}
		b.WriteString("\n")
	}
	b.WriteString("commit;")
	return b.String(), nil
}
