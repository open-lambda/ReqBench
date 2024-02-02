package main

import (		   
	"fmt"
	"rb/util"
)

type Record struct {
	fn_name		string		`csv:"fn_name"`
	latency		float64		`csv:"latency(ms)"`
	memory		float64		`csv:"memory(mb)"`
}

func (r Record) ToSlice() []string {
	return []string {
		r.fn_name,
		fmt.Sprintf("%f", r.latency),
		fmt.Sprintf("%f", r.memory),
	}
}

func main() {
	var r Record
	df := util.NewDataFrame(r)

	df.Append(Record {"fn1", 100, 100.1})
	df.Append(Record {"fn2", 100, 100.1})

	fmt.Println(df)
	df.ToCSV("test_df.csv")
}