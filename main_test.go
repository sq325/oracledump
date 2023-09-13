package main

import (
	"html/template"
	"strings"
	"testing"
)

func Test_generateInsertStatement(t *testing.T) {
	tmplstr := `INSERT INTO {{.Table}} ({{range $index, $col := .Cols}}{{if $index}},{{end}}{{$col}}{{end}}) VALUES ({{range $index, $val := .Vals}}{{if $index}},{{end}}{{$val}}{{end}});`
	data := []struct {
		Cols  []string
		Vals  []string
		Table string
	}{
		{
			Cols:  []string{"col1", "col2", "col3"},
			Vals:  []string{"val1", "val2", "val3"},
			Table: "table1",
		},
		{
			Cols:  []string{"col1", "col2", "col3"},
			Vals:  []string{"val1", "val2", "val3"},
			Table: "table2",
		},
	}

	tmpl, err := template.New("test").Parse(tmplstr)
	if err != nil {
		t.Fatal(err)
	}
	var b strings.Builder
	for _, d := range data {
		err = tmpl.Execute(&b, d)
		if err != nil {
			t.Fatal(err)
		}
		b.WriteString("\n")
	}
	t.Log(b.String())
}
