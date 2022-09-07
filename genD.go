package main

import (
	"database/sql"
	"flag"
	"fmt"
	"math"
	"math/rand"
	"strconv"
	"strings"

	_ "github.com/go-sql-driver/mysql"
)

// Port port
var Port = flag.String("P", "4000", "naaj connecting column nums")

var colNum = flag.Int("colNum", 1, "naaj connecting column nums")

// low colNum bits presentation, 0 means int, 1 means varchar
var colTypes = flag.Uint64("typeBits", 0, "type bits representation, default all int")

// exactly two value for left and right. default 200.000 for each.
var rows = flag.String("rows", "", "the size of each table")

// if multi cols, the percentage should be "1 2 3 4" to specify.
var nulls = flag.String("nullP", "", "the percentage of null values, default 1%")

// if multi cols, the percentage should be "100 90 80 70" to specify.
var distinct = flag.String("distinctP", "", "the percentage of distinct value/rows, default 100%")

func main() {
	flag.Parse()
	conn, err := Conn()
	if err != nil {
		fmt.Println("数据库连接错误", err)
		return
	}
	Run(conn)
}

const TEMP = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

type Tp int8

const (
	int64Tp   Tp     = 1
	varcharTp Tp     = 2
	mask      uint64 = 0x0000000000000001
)

func Conn() (*sql.DB, error) {
	res := "root:@tcp(127.0.0.1:%s)/test?charset=utf8&parseTime=True"
	db, err := sql.Open("mysql", fmt.Sprintf(res, *Port))
	if err != nil {
		return nil, err
	}
	return db, nil
}

type naColumn struct {
	tp          Tp
	distinct    int
	null        int
	rows        int
	distinctStr []string
}

func Run(db *sql.DB) {
	LeftNACols := make([]*naColumn, 0, *colNum)
	rightNACols := make([]*naColumn, 0, *colNum)
	// analyze the type bits.
	typeBits := make([]bool, 0, *colNum)
	for *colTypes != 0 {
		v := *colTypes & mask
		if v != 0 {
			typeBits = append(typeBits, true)
		} else {
			typeBits = append(typeBits, false)
		}
		*colTypes = *colTypes >> 1
	}
	// analyze the nulls
	nullInts := make([]int, 0, *colNum)
	nullStrValues := strings.Split(*nulls, " ")
	for i := 0; i < *colNum*2; i++ {
		if i < len(nullStrValues) {
			v, err := strconv.Atoi(nullStrValues[i])
			if err != nil {
				panic(err)
			}
			nullInts = append(nullInts, v)
		} else {
			// default 1
			nullInts = append(nullInts, 1)
		}
	}
	// analyze the distinct
	distinctInts := make([]int, 0, *colNum)
	distinctStrValues := strings.Split(*distinct, " ")
	for i := 0; i < *colNum*2; i++ {
		if i < len(distinctStrValues) {
			v, err := strconv.Atoi(distinctStrValues[i])
			if err != nil {
				panic(err)
			}
			distinctInts = append(distinctInts, v)
		} else {
			// default 100
			distinctInts = append(distinctInts, 100)
		}
	}
	// analyze the rows
	rowsInts := make([]int, 0, 2)
	rowsStrValues := strings.Split(*rows, " ")
	for i := 0; i < 2; i++ {
		if i < len(rowsStrValues) {
			v, err := strconv.Atoi(rowsStrValues[i])
			if err != nil {
				panic(err)
			}
			rowsInts = append(rowsInts, v)
		} else {
			// default 200 thousand.
			rowsInts = append(rowsInts, 200000)
		}
	}
	leftNullInts := nullInts[:*colNum]
	rightNullInts := nullInts[*colNum:]
	leftDistinctInts := distinctInts[:*colNum]
	rightDistinctInts := distinctInts[*colNum:]
	leftRowsInts := rowsInts[0]
	rightRowsInts := rowsInts[1]
	// construct na-column
	for i := *colNum - 1; i >= 0; i-- {
		tp := int64Tp
		if typeBits[i] {
			// varchar
			tp = varcharTp
		}
		nulls := (float64(leftNullInts[len(LeftNACols)]) / 100.0) * float64(leftRowsInts)
		distincts := (float64(leftDistinctInts[len(LeftNACols)]) / 100.0) * float64(leftRowsInts)
		LeftNACols = append(LeftNACols, &naColumn{tp, int(math.Floor(distincts + 0.5)), int(math.Floor(nulls + 0.5)), leftRowsInts, nil})
		nulls = (float64(rightNullInts[len(rightNACols)]) / 100.0) * float64(rightRowsInts)
		distincts = (float64(rightDistinctInts[len(rightNACols)]) / 100.0) * float64(rightRowsInts)
		rightNACols = append(rightNACols, &naColumn{tp, int(math.Floor(distincts + 0.5)), int(math.Floor(nulls + 0.5)), rightRowsInts, nil})
	}
	leftNAData := make([][]string, 0, len(LeftNACols))
	for i, _ := range LeftNACols {
		leftNAData = append(leftNAData, LeftNACols[i].genData())
	}
	rightNAData := make([][]string, 0, len(rightNACols))
	for i, _ := range rightNACols {
		rightNAData = append(rightNAData, rightNACols[i].genData())
	}
	lSql := "insert into t1 values("
	rSql := "insert into t2 values("
	for i := range LeftNACols {
		if i == 0 {
			lSql += "%s"
			rSql += "%s"
		} else {
			lSql += ",%s"
			rSql += ",%s"
		}
	}
	lSql += ")"
	rSql += ")"
	for i := 0; i < leftRowsInts; i++ {
		// for every row
		var values []interface{}
		for colIdx, _ := range leftNAData {
			values = append(values, leftNAData[colIdx][i])
		}
		ll := fmt.Sprintf(lSql, values...)
		_, err := db.Exec(ll)
		if err != nil {
			fmt.Println("insert error", err)
		}
	}
	for i := 0; i < rightRowsInts; i++ {
		// for every row
		var values []interface{}
		for colIdx, _ := range rightNAData {
			values = append(values, rightNAData[colIdx][i])
		}
		rr := fmt.Sprintf(rSql, values...)
		_, err := db.Exec(rr)
		if err != nil {
			fmt.Println("insert error", err)
		}
	}
}

func (n *naColumn) genData() []string {
	// occupy pos for null values.
	res := make([]string, 0, n.rows)
	nullPos := make(map[int]bool, n.rows)
	for i := 0; i < n.null; i++ {
		pos := rand.Intn(n.rows)
		for _, ok := nullPos[pos]; ok; {
			if pos < n.rows-1 {
				pos++
			} else {
				pos = rand.Intn(n.rows)
			}
			_, ok = nullPos[pos]
		}
		nullPos[pos] = true
	}
	// enumerate the rest.
	for i := 0; i < n.rows; i++ {
		if _, ok := nullPos[i]; ok {
			res = append(res, n.genNull())
			continue
		}
		if n.tp == int64Tp {
			oneOfDistinct := rand.Intn(n.distinct)
			res = append(res, strconv.FormatInt(int64(oneOfDistinct), 10))
		} else {
			oneOfDistinct := n.genStr8()
			res = append(res, oneOfDistinct)
		}
	}
	return res
}

func (n *naColumn) genStr8() string {
	if n.distinctStr == nil {
		distinctMap := make(map[string]bool, n.distinct)
		for i := 0; i < n.distinct; i++ {
			str := n.genOneStr()
			for _, ok := distinctMap[str]; ok; {
				str = n.genOneStr()
				_, ok = distinctMap[str]
			}
			distinctMap[str] = true
		}
		for k, _ := range distinctMap {
			n.distinctStr = append(n.distinctStr, k)
		}
	}
	return "'" + n.distinctStr[rand.Intn(n.distinct)] + "'"
}

func (n *naColumn) genOneStr() string {
	var res string
	for i := 0; i < 8; i++ {
		pos := rand.Intn(62)
		res = res + TEMP[pos:pos+1]
	}
	return res
}

func (n *naColumn) genNull() string {
	return "null"
}
