package CliffMemSQL

import (
	"errors"
	"reflect"
	"strings"
	"sort"
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

func (this st_MemTable_Row) GetInt(inParam string) (int) {
	if this != nil {
		switch this[inParam].(type) {
		case int:
			return this[inParam].(int)
		case float64:
			return int(this[inParam].(float64))
		default:
			return -9999
		}
	} else {
		return -9999
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
			return -9999
		}
	} else {
		return -9999
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
	joinMapRow := make(map[string]interface{})
	for key, val := range this.colNameType {
		joinMapNameType[key] = val
	}
	for key, val := range pT2.colNameType {
		joinMapNameType[key] = val
	}
	retPT := NewMemTable(joinMapNameType)
	//n^2匹配
	for _, valMap1 := range this.memTable {
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
	joinMapRow := make(map[string]interface{})
	for key, val := range this.colNameType {
		joinMapNameType[key] = val
	}
	for key, val := range pT2.colNameType {
		joinMapNameType[key] = val
	}
	retPT := NewMemTable(joinMapNameType)
	//n^2匹配
	for _, valMap1 := range this.memTable {
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
	j := this.rowCnt-1
	for i<j{
		this.memTable[i],this.memTable[j] = this.memTable[j],this.memTable[i]
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
