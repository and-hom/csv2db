package main

import (
	"os"
	"strconv"
	"github.com/Sirupsen/logrus"
	"encoding/csv"
	"fmt"
	"math/rand"
)

func main() {
	_rows, err := strconv.ParseInt(os.Args[2], 10, 64)
	if err != nil {
		logrus.Fatal(err)
	}
	rows := int(_rows)

	_cols, err := strconv.ParseInt(os.Args[3], 10, 64)
	if err != nil {
		logrus.Fatal(err)
	}
	cols := int(_cols)

	file, err := os.Create(os.Args[1])
	if err != nil {
		logrus.Fatal(err)
	}
	defer file.Close()

	w := csv.NewWriter(file)

	row := make([]string, 0, cols)
	for j := 0; j < cols; j++ {
		row = append(row, fmt.Sprintf("c-%d", j))
	}
	w.Write(row)

	for i := 0; i < rows; i++ {
		for j := 0; j < cols; j++ {
			row[j] = RandStringBytesRmndr(64)
		}
		w.Write(row)
	}
	w.Flush()

}

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func RandStringBytesRmndr(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Int63() % int64(len(letterBytes))]
	}
	return string(b)
}
