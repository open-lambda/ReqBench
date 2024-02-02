package util

import (
	"reflect"
	"sync"
	"strings"
	"fmt"
	"os"
	"io"
)

//helper to easily store list of struct as dataframe-like object and export it to csv
type DataFrame struct {
	recordType		reflect.Type
	sliceValue		reflect.Value
	typeFormatMap	map[string]string //format string for variable types. use %v if not specified.
	sync.Mutex
}

func NewDataFrame(record interface{}) *DataFrame {
	recordType := reflect.TypeOf(record)
	
	sliceType := reflect.SliceOf(recordType)
	sliceValue := reflect.MakeSlice(sliceType, 0, 100) //TODO: capacity=100?

	return &DataFrame{
		recordType: recordType,
		sliceValue: sliceValue,
	}
}

func (df *DataFrame) SetTypeFormatMap(formatMap map[string]string) {
	df.typeFormatMap = formatMap
}

func (df *DataFrame) Append(record interface{}) {
	df.Lock()
	defer df.Unlock()

	r := reflect.ValueOf(record)
	df.sliceValue = reflect.Append(df.sliceValue, r)
}

func (df *DataFrame) Index(i int) interface{} {
	df.Lock()
	defer df.Unlock()

	return df.sliceValue.Index(i).Interface()
}

func (df *DataFrame) Columns() []string { //column name should be specified with csv tag
	t := df.recordType
	cols := make([]string, 0, t.NumField())

	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		colName := field.Tag.Get("csv")
		cols = append(cols, colName)
	}

	return cols
}

func (df *DataFrame) String() string {
	output := make([]string, 0)
	output = append(output, strings.Join(df.Columns(), ", "))

	for i := 0; i < df.sliceValue.Len(); i++ {
		row := df.sliceValue.Index(i)
		
		row_str := []string{}

		for i := 0; i < row.NumField(); i++ {
			field := row.Field(i)
			fieldType := field.Type().Kind().String()

			var str string
			_, exists := df.typeFormatMap[fieldType]
			if exists {
				str = fmt.Sprintf(df.typeFormatMap[fieldType], field)
			} else {
				str = fmt.Sprintf("%v", field)
			}

			row_str = append(row_str, str)
		}

		output = append(output, strings.Join(row_str, ", "))
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