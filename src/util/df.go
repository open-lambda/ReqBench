package util

import (
	"reflect"
	"sync"
	"strings"
	"os"
	"io"
)

//helper to easily store list of struct as dataframe-like object and export it to csv
type RecordInterface interface {
	ToSlice() []string
}

type DataFrame struct {
	recordType		RecordInterface
	list			[]RecordInterface
	sync.Mutex
}

func NewDataFrame(record RecordInterface) *DataFrame {
	return &DataFrame{
		recordType: record,
		list: make([]RecordInterface, 0),
	}
}

func (df *DataFrame) Append(record RecordInterface) {
	df.Lock()
	defer df.Unlock()

	df.list = append(df.list, record)
}

func (df *DataFrame) Index(i int) RecordInterface {
	df.Lock()
	defer df.Unlock()

	return df.list[i]
}

func (df *DataFrame) Columns() []string { //column name should be specified with csv tag
	df.Lock()
	defer df.Unlock()

	t := reflect.TypeOf(df.recordType)
	cols := make([]string, 0, t.NumField())

	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		colName := field.Tag.Get("csv")
		cols = append(cols, colName)
	}

	return cols
}

func (df *DataFrame) String() string {
	df.Lock()
	defer df.Unlock()

	output := make([]string, 0)

	df.Unlock()
	output = append(output, ", "+ strings.Join(df.Columns(), ", "))
	df.Lock()

	for i := 0; i < len(df.list); i++ {

		output = append(output, strings.Join(df.list[i].ToSlice(), ", "))
	}

	return strings.Join(output, "\n")
}

func (df *DataFrame) ToCSV(path string) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = io.WriteString(file, df.String())
	if err != nil {
		return err
	}
	return nil
}