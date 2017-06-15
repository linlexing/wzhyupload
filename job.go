package main

import (
	"dbweb/lib/ddb"
	"io/ioutil"
	"strings"
	"sync"
	"time"

	"fmt"

	"os"
	"path/filepath"

	"archive/zip"

	"bufio"

	"github.com/linlexing/dbx/data"
	"github.com/linlexing/dbx/schema"
	"github.com/robfig/cron"
)

const batchNum = 500

var (
	jobs   = cron.New()
	jobRun = &sync.Mutex{}
)

func init() {
	jobs.Start()
}
func taskRun() {
	jobRun.Lock()
	defer jobRun.Unlock()
	dlog.Println("run at ", time.Now())
	err := buildDataFile()
	if err != nil {
		dlog.Error(err)
		return
	}
	//然后开始上传
	files, err := ioutil.ReadDir(filepath.Join(workDir, "out"))
	if err != nil {
		dlog.Error(err)
		return
	}
	for _, one := range files {
		filename := filepath.Join(workDir, "out", one.Name())
		if err = doUpload(vconfig.URL, filename,
			filepath.Join(workDir, vconfig.FinishOut), vconfig.UserName,
			vconfig.Password); err != nil {
			dlog.Error(err)
		}
		dlog.Println("file:", filename, "uploaded")
	}
	dlog.Println("job finished")
}
func createNewZipFile() (*os.File, *zip.Writer, *bufio.Writer, error) {
	outPath := filepath.Join(workDir, "out")
	if err := os.MkdirAll(outPath, os.ModePerm); err != nil {
		return nil, nil, nil, err
	}
	file, err := os.Create(filepath.Join(outPath, fmt.Sprintf("gsdata_%s_%s_000001.zip",
		time.Now().Format("20060102"), vconfig.AreaCode)))
	if err != nil {
		return nil, nil, nil, err
	}

	zipw := zip.NewWriter(file)
	//先复制模板文件
	files, err := ioutil.ReadDir(filepath.Join(workDir, "template"))
	if err != nil {
		return nil, nil, nil, err
	}
	for _, f := range files {
		w, err := zipw.Create(f.Name())
		if err != nil {
			return nil, nil, nil, err
		}
		bys, err := ioutil.ReadFile(filepath.Join(workDir, "template", f.Name()))
		if err != nil {
			return nil, nil, nil, err
		}
		if _, err = w.Write(bys); err != nil {
			return nil, nil, nil, err
		}
	}
	w, err := zipw.Create("ent_info.dat")

	return file, zipw, bufio.NewWriter(w), err
}
func openDB() (ddb.DB, *data.Table, *data.Table, error) {
	db, err := ddb.Openx(vconfig.Driver, vconfig.DBURL)
	if err != nil {
		return nil, nil, nil, err
	}
	tab, err := data.OpenTable(db.DriverName(), db, vconfig.Table)
	if err != nil {
		return nil, nil, nil, err
	}
	//必须全部是string类型
	for _, col := range tab.Columns {
		if col.Type != schema.TypeString {
			return nil, nil, nil, fmt.Errorf("column %s type not is string", col.Name)
		}
	}
	if len(vconfig.FieldSize) != len(tab.Columns) {
		return nil, nil, nil, fmt.Errorf("field size %d <> column length %d", len(vconfig.FieldSize), len(tab.Columns))
	}
	tab.Name = vconfig.ShadowTable
	tab.PrimaryKeys = []string{vconfig.PrimaryKey}
	//自动更新影子表的结构
	if err = tab.Table.Update(db.DriverName(), db); err != nil {
		return nil, nil, nil, err
	}
	tab.Name = vconfig.Table
	sttab, err := data.OpenTable(db.DriverName(), db, vconfig.ShadowTable)
	if err != nil {
		return nil, nil, nil, err
	}
	return db, tab, sttab, nil
}

//searchTable 遍历表，找出所有的差异数据
func searchTable(db ddb.DB, tab, sttab *data.Table, cb func(icount int,
	diffrows [][]interface{}) error) error {
	saveToShadowTable := func(diffRows [][]interface{}) error {
		//保存到影子表中
		for _, line := range diffRows {
			row := map[string]interface{}{}
			for i, col := range sttab.Columns {
				row[col.Name] = line[i]
			}
			if err := sttab.Save(row); err != nil {
				return err
			}
		}
		return nil
	}
	rows, err := db.Query(fmt.Sprintf("select %s from %s", vconfig.PrimaryKey, vconfig.Table))
	if err != nil {
		return err
	}
	defer rows.Close()
	icount := 1
	pks := []interface{}{}
	for rows.Next() {
		var pk interface{}
		if err = rows.Scan(&pk); err != nil {
			return err
		}
		pks = append(pks, pk)

		if icount%batchNum == 0 {
			diffRows, err := queryDiff(db, tab, vconfig.ShadowTable, pks)
			if err != nil {
				return err
			}
			pks = nil
			if err := cb(icount, diffRows); err != nil {
				return err
			}
			if err := saveToShadowTable(diffRows); err != nil {
				return err
			}

		}
		icount++
	}
	if len(pks) > 0 {
		diffRows, err := queryDiff(db, tab, vconfig.ShadowTable, pks)
		if err != nil {
			return err
		}
		pks = nil
		if err := cb(icount, diffRows); err != nil {
			return err
		}
		if err := saveToShadowTable(diffRows); err != nil {
			return err
		}
	}
	return nil
}

func writeLine(w *bufio.Writer, data []interface{}) error {
	for i, v := range data {
		var str string
		switch tv := v.(type) {
		case string:
			str = tv
		case []byte:
			str = string(tv)
		case nil:
		default:
			return fmt.Errorf("%T not in string", v)
		}
		str = strings.Replace(
			strings.Replace(str, "\r", " ", -1),
			"\n", " ", -1)
		rstr := []rune(str)
		if len(rstr) < vconfig.FieldSize[i] {
			str = str + strings.Repeat(" ", vconfig.FieldSize[i]-len(rstr))
		} else if len(rstr) > vconfig.FieldSize[i] {
			str = string(rstr[:vconfig.FieldSize[i]])
		}
		if _, err := w.WriteString(str); err != nil {
			return err
		}
	}
	if _, err := fmt.Fprintln(w); err != nil {
		return err
	}
	return nil
}
func buildDataFile() error {
	file, zipw, datw, err := createNewZipFile()
	if err != nil {
		return err
	}
	defer file.Close()
	defer zipw.Close()
	defer datw.Flush()
	db, tab, sttab, err := openDB()
	if err != nil {
		return err
	}
	dateTimeFields := []int{}

	for i, col := range tab.Columns {
		switch col.Name {
		case "数据修改时间", "数据上传时间":
			dateTimeFields = append(dateTimeFields, i)
		}
	}
	if err := searchTable(db, tab, sttab, func(i int, rows [][]interface{}) error {
		if len(rows) > 0 {
			dlog.Println("rownum:", i, "write", len(rows), "rows")
		}
		for _, line := range rows {
			//不复制，会影响回写影子表
			newLine := make([]interface{}, len(line))
			copy(newLine, line)
			//设置时间字段
			for _, idx := range dateTimeFields {
				newLine[idx] = time.Now().Format("20060102150405")
			}
			if err := writeLine(datw, newLine); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return err
	}
	return nil
}

//queryDiff 返回一个新增、变更的记录内容
func queryDiff(db ddb.DB, table *data.Table, shadowtable string,
	pkvalues []interface{}) ([][]interface{}, error) {
	str := fmt.Sprintf("%s in(?)", table.PrimaryKeys[0])
	where, params, err := data.In(str, pkvalues)
	if err != nil {
		return nil, err
	}
	//两个表的where
	params = append(params, params...)
	strSQL := data.Find(db.DriverName()).Minus(db, table.FullName(), where,
		shadowtable, where, table.PrimaryKeys, table.ColumnNames)

	rows, err := db.Query(strSQL, params...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	rev := [][]interface{}{}
	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	colCount := len(cols)
	for rows.Next() {
		row := make([]interface{}, colCount)
		for i := range row {
			row[i] = new(interface{})
		}
		if err = rows.Scan(row...); err != nil {
			return nil, err
		}
		line := make([]interface{}, colCount)
		for i := range row {
			line[i] = *(row[i].(*interface{}))
		}

		rev = append(rev, line)
	}
	return rev, nil
}
