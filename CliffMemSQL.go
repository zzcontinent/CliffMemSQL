package CliffMemSQL

import (
	"errors"
	"reflect"
	"strings"
	"sort"
	"strconv"
)

//连表查询导致数据库资源被占用，其他服务可能变慢，需要将查询语句根据索引拆分，把数据计算放到本地，
//需求：一个查表数据内存映射

type ST_MemTable struct {
	memTable     []st_MemTable_Row
	colNameType  map[string]string
	rowCnt       int //行数
	colCnt       int //列数
	colNameOrder string
}
type st_MemTable_Row map[string]interface{}

//对表格数据提取相应类型
func (this st_MemTable_Row) GetInt(inParam string) (int) {
	if this != nil {
		switch this[inParam].(type) {
		case int:
			return this[inParam].(int)
		case float64:
			return int(this[inParam].(float64))
		default:
			return 0
		}
	} else {
		return 0
	}
}
func (this st_MemTable_Row) GetInt64(inParam string) (int64) {
	if this != nil {
		switch this[inParam].(type) {
		case int64:
			return this[inParam].(int64)
		case float64:
			return int64(this[inParam].(float64))
		default:
			return 0
		}
	} else {
		return 0
	}
}
func (this st_MemTable_Row) GetString(inParam string) (string) {
	if this != nil {
		switch this[inParam].(type) {
		case string:
			return this[inParam].(string)
		default:
			return ""
		}
	} else {
		return ""
	}
}
func (this st_MemTable_Row) GetVal(inParam string) (interface{}) {
	if this != nil {
		return this[inParam]
	} else {
		return nil
	}
}
func (this *st_MemTable_Row) SetVal(inKey string, inVal interface{}) {
	if this != nil {
		(*this)[inKey] = inVal
	}
}

func (this *ST_MemTable) getColType(colName string) string {

	for key, val := range this.colNameType {
		if key == colName {
			return val
		}
	}
	return ""
}
func (this *ST_MemTable) CheckColNameExist(colName string) bool {

	if str := this.getColType(colName); str == "" {
		return false
	} else {
		return true
	}
}

func NewMemTable(colNameType map[string]string) *ST_MemTable {
	pMemTable := new(ST_MemTable)
	pMemTable.memTable = make([]st_MemTable_Row, 0)
	pMemTable.colNameType = colNameType
	pMemTable.colNameType["m_ValidStatus"] = "int" //用于判断该行是否有效，内部维护
	pMemTable.rowCnt = 0
	pMemTable.colCnt = len(colNameType) - 1
	if pMemTable.colCnt < 0 {
		pMemTable.colCnt = 0
	}
	return pMemTable
}

func (this *ST_MemTable) GetColType(colName string) (string, error) {
	if this == nil {
		return "", errors.New("pT is null")
	}
	return this.getColType(colName), nil
}
func (this *ST_MemTable) GetRowCount() (int, error) {
	if this == nil {
		return 0, errors.New("pT is null")
	}
	return this.rowCnt, nil
}
func (this *ST_MemTable) GetColCount() (int, error) {
	if this == nil {
		return 0, errors.New("pT is null")
	}
	return this.colCnt, nil
}
func (this *ST_MemTable) GetColNames() ([]string, error) {
	if this == nil {
		return nil, errors.New("pT is null")
	}
	retStr := make([]string, 0)
	for key, _ := range this.colNameType {
		retStr = append(retStr, key)
	}
	return retStr, nil
}
func (this *ST_MemTable) UpdateRow(setRow map[string]interface{}, whereRow map[string]interface{}) (tf bool, effectRows int, err error) {
	if this == nil {
		return false, 0, errors.New("pT is null")
	}
	posRowQ, _, _, _ := this.QueryRows(whereRow)
	for key, val := range setRow {
		//列名判断
		if this.CheckColNameExist(key) == false {
			return false, 0, errors.New("colType not match:colName=" + key)
		}
		//类型判断
		if this.getColType(key) != reflect.TypeOf(val).String() {
			return false, 0, errors.New("Type:colName=" + key + " colType=" + this.getColType(key) + " NOT=" + reflect.TypeOf(val).String())
		}
	}
	retEffectRows := 0
	for key, valSet := range setRow {
		//更新
		for _, val := range posRowQ {
			this.memTable[val][key] = valSet
			retEffectRows ++
		}
	}
	return true, retEffectRows, nil
}
func (this *ST_MemTable) InsertRow(mapRow map[string]interface{}) (bool, error) {
	if this == nil {
		return false, errors.New("pT is null")
	}

	for key, val := range mapRow {
		//列名判断
		if this.CheckColNameExist(key) == false {
			return false, errors.New("colType not match:colName=" + key)
		}
		//类型判断
		if this.getColType(key) != reflect.TypeOf(val).String() {
			return false, errors.New("Type:colName=" + key + " colType=" + this.getColType(key) + " NOT=" + reflect.TypeOf(val).String())
		}
	}
	//更新
	mapRowTmp := st_MemTable_Row{}

	for key, val := range mapRow {
		if this.colNameType[key] != "" {
			mapRowTmp.SetVal(key, val)
		}
	}
	mapRowTmp.SetVal("m_ValidStatus", 1)
	this.memTable = append(this.memTable, mapRowTmp)
	this.rowCnt++
	return true, nil
}
func (this *ST_MemTable) DeleteRow(whereMap map[string]interface{}) (bool, error) {
	if this == nil {
		return false, errors.New("pT is null")
	}

	for key, val := range whereMap {
		//列名判断
		if this.CheckColNameExist(key) == false {
			return false, errors.New("colType not match:colName=" + key)
		}
		//类型判断
		if this.getColType(key) != reflect.TypeOf(val).String() {
			return false, errors.New("Type:colName=" + key + " colType=" + this.getColType(key) + " NOT=" + reflect.TypeOf(val).String())
		}
	}
	//更新m_ValidStatus
	setMapTmp := make(map[string]interface{})
	setMapTmp["m_ValidStatus"] = -1
	tf, cnt, err := this.UpdateRow(setMapTmp, whereMap)
	if tf != true {
		return false, err
	}
	this.rowCnt -= cnt
	return true, nil
}

//inCnt:-1 获取全部行数据
func (this *ST_MemTable) GetRows(inStart int, inCnt int) (tf bool, effectRows int, outmap []st_MemTable_Row, err error) {
	if this == nil {
		return false, 0, nil, errors.New("pT is null")
	}
	if inStart < 0 || (inStart >= this.rowCnt && this.rowCnt > 0) {
		return false, 0, nil, errors.New("inStart out of range")
	}
	if inCnt < 0 && inCnt != -1 {
		return false, 0, nil, errors.New("inCnt < 0 && inCnt != -1")
	} else if inCnt == -1 {
		retEffectCnt := 0
		retList := make([]st_MemTable_Row, 0)
		for i, val := range this.memTable {
			if i >= inStart && val.GetInt("m_ValidStatus") == 1 {
				retList = append(retList, val)
				retEffectCnt++
			}
		}
		return true, retEffectCnt, retList, nil
	}
	//获取
	retEffectCnt := 0
	retList := make([]st_MemTable_Row, 0)
	for i, val := range this.memTable {
		if i >= inStart && i < inStart+inCnt && val.GetInt("m_ValidStatus") == 1 {
			retList = append(retList, val)
			retEffectCnt++
		}
	}
	return true, retEffectCnt, retList, nil
}

func (this *ST_MemTable) GetCols(inColName []string) (tf bool, outmap []map[string]interface{}, err error) {
	if this == nil {
		return false, nil, errors.New("pT is null")
	}
	for _, val := range inColName {
		if !this.CheckColNameExist(val) {
			return false, nil, errors.New("没有列名:" + val)
		}
	}
	//获取
	retList := make([]map[string]interface{}, 0)
	retListOne := make(map[string]interface{})
	for _, valRowMap := range this.memTable {
		for _, inVal := range inColName {
			if valRowMap.GetInt("m_ValidStatus") == 1 {
				retListOne[inVal] = valRowMap[inVal]
			}
		}
		retList = append(retList, retListOne)
	}
	return true, retList, nil
}
func (this *ST_MemTable) GetColsOne(inColName string) ([]map[string]interface{}, error) {
	if this == nil {
		return nil, errors.New("pT is null")
	}
	if !this.CheckColNameExist(inColName) {
		return nil, errors.New("没有列名:" + inColName)
	}

	//获取
	retList := make([]map[string]interface{}, 0)
	retListOne := make(map[string]interface{})
	for _, valRowMap := range this.memTable {
		if valRowMap.GetInt("m_ValidStatus") == 1 {
			retListOne[inColName] = valRowMap[inColName]
		}
		retList = append(retList, retListOne)
	}
	return retList, nil
}

func (this *ST_MemTable) QueryRows(whereMap map[string]interface{}) (posRow []int, total int, outMap []map[string]interface{}, err error) {
	if this == nil {
		return nil, 0, nil, errors.New("pT is null")
	}
	//获取
	pos := make([]int, 0)
	retList := make([]map[string]interface{}, 0)
	cnt := 0
	gotIt := 0
	for i, valMapRow := range this.memTable {
		gotIt = 0
		for key, val := range whereMap { //要匹配的key和val
			if valMapRow[key] == val && valMapRow.GetInt("m_ValidStatus") == 1 {
				gotIt ++
			}
		}
		if gotIt == len(whereMap) {
			pos = append(pos, i)
			retList = append(retList, this.memTable[i])
			cnt++
		}
	}
	return pos, cnt, retList, nil
}
func (this *ST_MemTable) QueryRowsLike(whereMap map[string]interface{}) (posRow []int, total int, outMap []map[string]interface{}, err error) {
	if this == nil {
		return nil, 0, nil, errors.New("pT is null")
	}
	//获取
	pos := make([]int, 0)
	retList := make([]map[string]interface{}, 0)
	cnt := 0
	gotIt := 0
	for i, valMapRow := range this.memTable {
		if valMapRow.GetInt("m_ValidStatus") == 1 {
			gotIt = 0
			for key, val := range whereMap { //要匹配的key和val
				if this.colNameType[key] == "string" {
					if strings.Contains(valMapRow[key].(string), val.(string)) {
						gotIt ++
					}
				}
			}
			if gotIt == len(whereMap) {
				pos = append(pos, i)
				retList = append(retList, this.memTable[i])
				cnt++
			}
		}
	}
	return pos, cnt, retList, nil
}
func (this *ST_MemTable) AddColName(colNameType map[string]string) (bool, error) {
	if this == nil {
		return false, errors.New("pT is null")
	}
	for key, val := range colNameType {
		this.colNameType[key] = val
		this.colCnt ++
	}
	return true, nil
}

//pT1 join pT2
func (this *ST_MemTable) Join(pT2 *ST_MemTable, whereColNameEqual map[string]string) (outPT *ST_MemTable, effectRows int) {
	joinMapNameType := make(map[string]string)
	for key, val := range this.colNameType {
		joinMapNameType[key] = val
	}
	for key, val := range pT2.colNameType {
		joinMapNameType[key] = val
	}
	retPT := NewMemTable(joinMapNameType)
	//n^2匹配
	for _, valMap1 := range this.memTable {
		joinMapRow := make(map[string]interface{})
		for _, valMap2 := range pT2.memTable {
			mathCnt := 0
			for WhereStr1, WhereStr2 := range whereColNameEqual {
				if valMap1[WhereStr1] == valMap2[WhereStr2] {
					mathCnt++
				}
			}
			if mathCnt == len(whereColNameEqual) {
				for key1, val1 := range valMap1 {
					joinMapRow[key1] = val1
				}
				for key2, val2 := range valMap2 {
					joinMapRow[key2] = val2
				}
				retPT.InsertRow(joinMapRow)
			}
		}
	}
	return retPT, retPT.rowCnt
}
func (this *ST_MemTable) LeftJoin(pT2 *ST_MemTable, whereColNameEqual map[string]string) (outPT *ST_MemTable, effectRows int) {
	joinMapNameType := make(map[string]string)
	for key, val := range this.colNameType {
		joinMapNameType[key] = val
	}
	for key, val := range pT2.colNameType {
		joinMapNameType[key] = val
	}
	retPT := NewMemTable(joinMapNameType)
	//n^2匹配
	for _, valMap1 := range this.memTable {
		joinMapRow := make(map[string]interface{})
		for key1, val1 := range valMap1 {
			joinMapRow[key1] = val1
		}
		rowMatchCnt := 0
		for _, valMap2 := range pT2.memTable {
			oneRowMathCnt := 0
			for WhereStr1, WhereStr2 := range whereColNameEqual {
				if valMap1[WhereStr1] == valMap2[WhereStr2] {
					oneRowMathCnt++
				}
			}
			if oneRowMathCnt == len(whereColNameEqual) {
				for key2, val2 := range valMap2 {
					joinMapRow[key2] = val2
				}
				rowMatchCnt++
				retPT.InsertRow(joinMapRow)
			}
		}
		if rowMatchCnt == 0 {
			retPT.InsertRow(joinMapRow)
		}
	}
	return retPT, retPT.rowCnt
}

//对表行 关键字去重
func (this *ST_MemTable) GroupBy_Limit1st(colName string) (error) {
	if !this.CheckColNameExist(colName) {
		return errors.New("GroupBy_Limit1:" + "找不到对应列(" + colName + ")")
	}
	cnt, _ := this.GetRowCount()
	j := cnt - 1
	//从后向前 删除重复数据
	for j >= 0 {
		for i, TableRow := range this.memTable {
			if TableRow["m_ValidStatus"] == 1 {
				if i < j {
					if TableRow.GetVal(colName) == this.memTable[j].GetVal(colName) {
						this.memTable[j].SetVal("m_ValidStatus", -1)
					}
				}
			}
		}
		j--
	}
	return nil
}

//对表合并操作，需要重新创建表格，表的列属性全部变成string
func (this *ST_MemTable) GroupBy(colName string) (error) {
	if !this.CheckColNameExist(colName) {
		return errors.New("GroupBy:" + "找不到对应列(" + colName + ")")
	}
	cnt, _ := this.GetRowCount()
	i := 0
	//从前往后，合并组数据
	for i < cnt {
		if this.memTable[i].GetInt("m_ValidStatus") == 1 {
			foundRow := make([]st_MemTable_Row, 0)
			foundRow = append(foundRow, this.memTable[i])
			for j, _ := range this.memTable {
				if this.memTable[j]["m_ValidStatus"] == 1 {
					if j > i {
						if this.memTable[i].GetVal(colName) == this.memTable[j].GetVal(colName) {
							foundRow = append(foundRow, this.memTable[j])
							this.memTable[j].SetVal("m_ValidStatus", -1)
							this.rowCnt--
						}
					}
				}
			}
			//对i的行进行合并，以字符串+，方式拼接
			mapSlice := make(map[string][]interface{})
			for _,rowMap := range foundRow{
				for key,val := range rowMap{
					mapSlice[key] = append(mapSlice[key],val)
				}
			}
			for key,slice := range mapSlice{
				this.memTable[i].SetVal(key,SliceToString(slice,","))
			}
			this.memTable[i].SetVal("m_ValidStatus",1)
		}
		i++
	}
	//对列格式作调整
	for key, _ := range this.colNameType {
		this.colNameType[key] = "string"
	}
	this.colNameType["m_ValidStatus"] = "int"
	return nil
}

//对表进行关键列排序，目前只支持int类型，后续加入时间排序
func (this *ST_MemTable) Sort_ASC(ColName string) {
	this.colNameOrder = ColName
	if !sort.IsSorted(this) {
		sort.Sort(this)
	}
}
func (this *ST_MemTable) Sort_DESC(ColName string) {
	this.colNameOrder = ColName
	if !sort.IsSorted(this) {
		sort.Sort(this)
	}
	i := 0
	j := this.rowCnt - 1
	for i < j {
		this.memTable[i], this.memTable[j] = this.memTable[j], this.memTable[i]
		i++
		j--
	}

}
func (this *ST_MemTable) Len() int {
	return this.rowCnt
}
func (this *ST_MemTable) Less(i, j int) bool {
	_, _, outmap1, err := this.GetRows(i, 1)
	if err != nil {
		return false
	}
	_, _, outmap2, err := this.GetRows(j, 1)
	if err != nil {
		return false
	}
	return outmap1[0].GetInt(this.colNameOrder) < outmap2[0].GetInt(this.colNameOrder)
}
func (this *ST_MemTable) Swap(i, j int) {
	this.memTable[i], this.memTable[j] = this.memTable[j], this.memTable[i]
}

//数组去重算法
func Rm_duplicate(list []interface{}) []interface{} {
	x := make([]interface{}, 0)
	for _, i := range list {
		if len(x) == 0 {
			x = append(x, i)
		} else {
			for k, v := range x {
				if i == v {
					break
				}
				if k == len(x)-1 {
					x = append(x, i)
				}
			}
		}
	}
	return x
}
func ReplacedBySlice(in1 string, in2 []string) string {
	for _, val := range in2 {
		in1 = strings.Replace(in1, "?", val, 1)
	}
	return in1
}

func StringToSlice_Int(in1 string, interval string) ([]int, error) {
	outSliceStr := strings.Split(in1, interval)
	outSliceInt := make([]int, len(outSliceStr))
	var err error
	for i, val := range outSliceStr {
		outSliceInt[i], err = strconv.Atoi(val)
		if err != nil {
			return nil, err
		}
	}
	return outSliceInt, nil
}
func StringToSlice_String(in1 string, interval string) ([]string) {
	return strings.Split(in1, interval)
}
func SliceToString(inParam []interface{}, interval string) string {
	outStr := ""
	for i, val := range inParam {
		switch val.(type) {
		case int:
			outStr += strconv.Itoa(val.(int))
			if i < len(inParam)-1 {
				outStr += interval
			}
		case int64:
			outStr += strconv.FormatInt(val.(int64), 10)
			if i < len(inParam)-1 {
				outStr += interval
			}
		case string:
			outStr += val.(string)
			if i < len(inParam)-1 {
				outStr += interval
			}
		}
	}
	return outStr
}

//数组一致内容
func SliceSame(in1 []interface{}, in2 []interface{}) ([]interface{}) {
	outSame := make([]interface{}, 0)
	flagFound := 0
	for _, val1 := range in1 {
		flagFound = 0
		for _, val2 := range in2 {
			if val1 == val2 {
				flagFound ++
			}
		}
		if flagFound > 0 {
			outSame = append(outSame, val1)
		}
	}
	return Rm_duplicate(outSame)
}

//数组不同内容A+B
func SliceDiff(in1 []interface{}, in2 []interface{}) ([]interface{}) {
	outDiff := make([]interface{}, 0)
	flagFound := 0
	for _, val1 := range in1 {
		flagFound = 0
		for _, val2 := range in2 {
			if val1 == val2 {
				flagFound++
			}
		}
		if flagFound == 0 {
			outDiff = append(outDiff, val1)
		}
	}
	for _, val2 := range in2 {
		flagFound = 0
		for _, val1 := range in1 {
			if val1 == val2 {
				flagFound++

			}
		}
		if flagFound == 0 {
			outDiff = append(outDiff, val2)
		}
	}
	return Rm_duplicate(outDiff)
}

//数组不同内容A-B
func SliceDiffFromA(in1 []interface{}, in2 []interface{}) ([]interface{}) {
	outDiff := make([]interface{}, 0)
	flagFound := 0
	for _, val1 := range in1 {
		flagFound = 0
		for _, val2 := range in2 {
			if val1 == val2 {
				flagFound++
			}
		}
		if flagFound == 0 {
			outDiff = append(outDiff, val1)
		}
	}
	return Rm_duplicate(outDiff)
}

//对interface{}提取相应数据类型
type stMyInterfaceConv struct{}

var CGetInterface stMyInterfaceConv
//从interface{}进行类型转换；因为强制类型转换会导致panic，转换前要加类型判断
func (stMyInterfaceConv) GetInt(in interface{}) int {
	switch in.(type) {
	case int:
		return in.(int)
	case int64:
		return int(in.(int64))
	case float32:
		return int(in.(float32))
	case float64:
		return int(in.(float64))
	case string:
		return 0
	default:
		return 0
	}
}
func (stMyInterfaceConv) GetInt64(in interface{}) int64 {
	switch in.(type) {
	case int:
		return int64(in.(int))
	case int64:
		return in.(int64)
	case float32:
		return int64(in.(float32))
	case float64:
		return int64(in.(float64))
	case string:
		return 0
	default:
		return 0
	}
}
func (stMyInterfaceConv) GetString(in interface{}) string {
	switch in.(type) {
	case int:
		return ""
	case int64:
		return ""
	case float32:
		return ""
	case float64:
		return ""
	case string:
		return in.(string)
	default:
		return ""
	}
}

//加入Sort函数,比较数字大小
type SortSliceInt []int
func (this SortSliceInt) Sort_ASC() {
	if !sort.IsSorted(this) {
		sort.Sort(this)
	}
}
func (this SortSliceInt) Sort_DESC() {
	this.Sort_ASC()
	i := 0
	j := len(this) - 1
	for i < j {
		this.Swap(i, j)
		i++
		j--
	}
}
func (this SortSliceInt) Len() int {
	return len(this)
}
func (this SortSliceInt) Swap(i, j int) {
	this[i], this[j] = this[j], this[i]
}
func (this SortSliceInt) Less(i, j int) bool {
	return this[i] < this[j]
}

type SortSliceInt64 []int64
func (this SortSliceInt64) Sort_ASC() {
	if !sort.IsSorted(this) {
		sort.Sort(this)
	}
}
func (this SortSliceInt64) Sort_DESC() {
	this.Sort_ASC()
	i := 0
	j := len(this) - 1
	for i < j {
		this.Swap(i, j)
		i++
		j--
	}
}
func (this SortSliceInt64) Len() int {
	return len(this)
}
func (this SortSliceInt64) Swap(i, j int) {
	this[i], this[j] = this[j], this[i]
}
func (this SortSliceInt64) Less(i, j int) bool {
	return this[i] < this[j]
}

type SortSlicefloat32 []float32
func (this SortSlicefloat32) Sort_ASC() {
	if !sort.IsSorted(this) {
		sort.Sort(this)
	}
}
func (this SortSlicefloat32) Sort_DESC() {
	this.Sort_ASC()
	i := 0
	j := len(this) - 1
	for i < j {
		this.Swap(i, j)
		i++
		j--
	}
}
func (this SortSlicefloat32) Len() int {
	return len(this)
}
func (this SortSlicefloat32) Swap(i, j int) {
	this[i], this[j] = this[j], this[i]
}
func (this SortSlicefloat32) Less(i, j int) bool {
	return this[i] < this[j]
}