package CliffMemSQL

import (
	"errors"
	"reflect"
	"strings"
	"sort"
	"strconv"
	"sync"
)

//连表查询导致数据库资源被占用，其他服务可能变慢，需要将查询语句根据索引拆分，把数据计算放到本地，
//需求：一个查表数据内存映射

const printMaxLen = 100

type ST_MemTable struct {
	memTable      []st_MemTable_Row
	colNameType   map[string]string
	colNameRemark map[string]string
	colNameOrder  string
	m_RWLock      *sync.RWMutex //支持多线程读写操作
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
func (this st_MemTable_Row) GetValToString(inParam string) (string) {
	if this != nil {
		switch this[inParam].(type) {
		case string:
			return this[inParam].(string)
		case int:
			return strconv.Itoa(this[inParam].(int))
		case int64:
			return strconv.FormatInt(this[inParam].(int64), 10)
		case float64:
			return strconv.FormatFloat(this[inParam].(float64), 'f', 0, 0)
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

func (this st_MemTable_Row) GetStringToInt(inParam string) (int) {
	if this != nil {
		out, err := strconv.ParseInt(this.GetString(inParam), 10, 0)
		if err != nil {
			out = 0
		}
		return int(out)
	} else {
		return 0
	}
}
func (this st_MemTable_Row) GetStringToInt64(inParam string) (int64) {
	if this != nil {
		out, err := strconv.ParseInt(this.GetString(inParam), 10, 64)
		if err != nil {
			out = 0
		}
		return out
	} else {
		return 0
	}
}
func (this st_MemTable_Row) GetStringToFloat64(inParam string) (float64) {
	if this != nil {
		out, err := strconv.ParseFloat(this.GetString(inParam), 64)
		if err != nil {
			out = 0
		}
		return out
	} else {
		return 0
	}
}
func (this st_MemTable_Row) GetStringToFloat32(inParam string) (float32) {
	if this != nil {
		out, err := strconv.ParseFloat(this.GetString(inParam), 32)
		if err != nil {
			out = 0
		}
		return float32(out)
	} else {
		return 0
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
	pMemTable.colNameType = make(map[string]string)
	for k, v := range colNameType {
		pMemTable.colNameType[k] = v
	}
	pMemTable.colNameType["m_ValidStatus"] = "int" //用于判断该行是否有效，内部维护
	pMemTable.colNameRemark = make(map[string]string)
	pMemTable.m_RWLock = &sync.RWMutex{}
	return pMemTable
}

func (this *ST_MemTable) GetColType(colName string) (string, error) {
	if this == nil {
		return "", errors.New("pT is null")
	}
	return this.getColType(colName), nil
}

func (this *ST_MemTable) AddRemark(colName string, remark string) {
	if this == nil {
		return
	}
	if _, ok := this.colNameType[colName]; ok {
		tmpRemark := strings.Join(strings.Split(remark, "\n"), " ")
		tmpRemark2 := strings.Join(strings.Split(tmpRemark, "\t"), "")
		this.colNameRemark[colName] = tmpRemark2
	}
	return
}

func (this *ST_MemTable) GetRemark(colName string) string {
	if this == nil {
		return ""
	}
	return this.colNameRemark[colName]
}

//获取表格有效行数
func (this *ST_MemTable) GetRowCount() (int) {
	if this == nil {
		return 0
	}
	cnt := 0
	this.m_RWLock.RLock()
	defer this.m_RWLock.RUnlock()
	for _, val := range this.memTable {
		if val.GetVal("m_ValidStatus") == 1 {
			cnt ++
		}
	}
	return cnt
}

//获取表格总共行数
//加锁
func (this *ST_MemTable) GetRowCount_Total() (int) {
	if this == nil {
		return 0
	}
	return len(this.memTable)
}

func (this *ST_MemTable) GetColCount() (int) {
	if this == nil {
		return 0
	}
	return len(this.colNameType)
}

func (this *ST_MemTable) GetColNames() ([]string) {
	if this == nil {
		return nil
	}
	retStr := make([]string, 0)
	for key, _ := range this.colNameType {
		retStr = append(retStr, key)
	}
	return retStr
}

func (this *ST_MemTable) InsertRow(mapRow map[string]interface{}) (bool, error) {
	if this == nil {
		return false, errors.New("pT is null")
	}
	this.m_RWLock.RLock()
	for key, val := range mapRow {
		//列名判断
		if this.CheckColNameExist(key) == false {
			return false, errors.New("colType not match:colName=" + key)
		}
		//类型判断
		if val != nil {
			if this.getColType(key) != reflect.TypeOf(val).String() {
				return false, errors.New("Type:colName=" + key + " colType=" + this.getColType(key) + " NOT=" + reflect.TypeOf(val).String())
			}
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
	this.m_RWLock.RUnlock()
	this.m_RWLock.Lock()
	defer this.m_RWLock.Unlock()
	this.memTable = append(this.memTable, mapRowTmp)
	return true, nil
}

//inCnt:-1 获取全部行数据
//inStart 为有效行目录的下标

func (this *ST_MemTable) GetRows_IndexOK(inStart int, inCnt int) (tf bool, effectRows int, outmap []st_MemTable_Row, err error) {
	validStart := 0
	this.m_RWLock.RLock()
	defer this.m_RWLock.RUnlock()
	if this == nil {
		return false, 0, nil, errors.New("pT is null")
	}
	if inStart < 0 || (inStart >= this.GetRowCount() && this.GetRowCount() > 0) {
		return false, 0, nil, errors.New("inStart out of range")
	}
	if inCnt < 0 && inCnt != -1 {
		return false, 0, nil, errors.New("inCnt < 0 && inCnt != -1")
	}
	//找到有效行的开始
	validCnt := 0
	for i, val := range this.memTable {
		if val.GetInt("m_ValidStatus") == 1 {
			validCnt ++
			if validCnt >= inStart+1 {
				validStart = i
				break
			}
		}
	}
	if inCnt == -1 {
		retEffectCnt := 0
		retList := make([]st_MemTable_Row, 0)
		for i, val := range this.memTable {
			if i >= validStart && val.GetInt("m_ValidStatus") == 1 {
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
		if i >= validStart && val.GetInt("m_ValidStatus") == 1 {
			if retEffectCnt < inCnt {
				retList = append(retList, val)
				retEffectCnt ++
			}
		}
	}
	return true, retEffectCnt, retList, nil
}

//inStart 为总表的下标

func (this *ST_MemTable) GetRows(inStart int, inCnt int) (tf bool, effectRows int, outmap []st_MemTable_Row, err error) {
	if this == nil {
		return false, 0, nil, errors.New("pT is null")
	}
	if inStart < 0 || (inStart >= this.GetRowCount_Total() && this.GetRowCount_Total() > 0) {
		return false, 0, nil, errors.New("inStart out of range")
	}
	if inCnt < 0 && inCnt != -1 {
		return false, 0, nil, errors.New("inCnt < 0 && inCnt != -1")
	}
	this.m_RWLock.RLock()
	defer this.m_RWLock.RUnlock()
	if inCnt == -1 {
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
		if i >= inStart && val.GetInt("m_ValidStatus") == 1 {
			if retEffectCnt < inCnt {
				retList = append(retList, val)
				retEffectCnt ++
			}
		}
	}
	return true, retEffectCnt, retList, nil
}

func (this *ST_MemTable) GetCols(inColName []string) (tf bool, outmap []map[string]interface{}, err error) {
	if this == nil {
		return false, nil, errors.New("pT is null")
	}
	this.m_RWLock.RLock()
	defer this.m_RWLock.RUnlock()
	for _, val := range inColName {
		if !this.CheckColNameExist(val) {
			return false, nil, errors.New("没有列名:" + val)
		}
	}
	//获取
	retList := make([]map[string]interface{}, 0)
	for _, valRowMap := range this.memTable {
		retListOne := make(map[string]interface{})
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
	this.m_RWLock.RLock()
	defer this.m_RWLock.RUnlock()
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
func (this *ST_MemTable) Subset(inColName []string) (*ST_MemTable, error) {
	if this == nil {
		return nil, errors.New("pT is null")
	}
	this.m_RWLock.RLock()
	defer this.m_RWLock.RUnlock()
	for _, val := range inColName {
		if !this.CheckColNameExist(val) {
			return nil, errors.New("没有列名:" + val)
		}
	}
	colNameType := make(map[string]string)
	for _, val := range inColName {
		colNameType[val] = this.getColType(val)
	}
	pTOut := NewMemTable(colNameType)

	//获取
	_, _, thisRows, err := this.GetRows(0, -1)
	if err != nil {
		return nil, err
	}
	for _, val := range thisRows {
		rowOne := make(map[string]interface{})
		for colName, colType := range colNameType {
			switch colType {
			case "string":
				rowOne[colName] = val.GetString(colName)
				break

			case "int":
				rowOne[colName] = val.GetInt(colName)
				break

			case "int64":
				rowOne[colName] = val.GetInt64(colName)
				break
			default:
				break
			}
		}
		_, err := pTOut.InsertRow(rowOne)
		if err != nil {
			return pTOut, err
		}
	}

	for key, val := range this.colNameRemark {
		pTOut.AddRemark(key, val)
	}
	return pTOut, nil
}

func (this *ST_MemTable) QueryRows(whereMap map[string]interface{}) (posRow []int, total int, outMap []map[string]interface{}, err error) {
	if this == nil {
		return nil, 0, nil, errors.New("pT is null")
	}
	this.m_RWLock.RLock()
	defer this.m_RWLock.RUnlock()
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
	this.m_RWLock.RLock()
	defer this.m_RWLock.RUnlock()
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

func (this *ST_MemTable) QueryTable(whereMap map[string]interface{}) (*ST_MemTable, error) {
	if this == nil {
		return nil, errors.New("pT is null")
	}
	this.m_RWLock.RLock()
	defer this.m_RWLock.RUnlock()
	//获取
	gotIt := 0
	pTOut := NewMemTable(this.colNameType)
	for _, valMapRow := range this.memTable {
		gotIt = 0
		for key, val := range whereMap { //要匹配的key和val
			if valMapRow[key] == val && valMapRow.GetInt("m_ValidStatus") == 1 {
				gotIt ++
			}
		}
		if gotIt == len(whereMap) {
			pTOut.InsertRow(valMapRow)
		}
	}
	return pTOut, nil
}
func (this *ST_MemTable) QueryTableInAnd(whereMapIn map[string][]interface{}) (*ST_MemTable, error) {
	if this == nil {
		return nil, errors.New("pT is null")
	}
	this.m_RWLock.RLock()
	defer this.m_RWLock.RUnlock()
	//获取
	pTOut := NewMemTable(this.colNameType)
	for _, valMapRow := range this.memTable {
		gotIt := make(map[string]int)
		for key, valList := range whereMapIn { //要匹配的key和val
			for _, val := range valList {
				if valMapRow[key] == val && valMapRow["m_ValidStatus"] == 1 {
					gotIt[key] ++
				}
			}
		}
		gotItAnd := 0
		for key, _ := range whereMapIn {
			if gotIt[key] > 0 {
				gotItAnd ++
			}
		}
		if gotItAnd == len(whereMapIn) {
			pTOut.InsertRow(valMapRow)
		}
	}
	return pTOut, nil
}

func (this *ST_MemTable) AddColName(colNameType map[string]string) (bool, error) {
	if this == nil {
		return false, errors.New("pT is null")
	}
	this.m_RWLock.Lock()
	defer this.m_RWLock.Unlock()
	for key, val := range colNameType {
		this.colNameType[key] = val
	}
	return true, nil
}

func (this *ST_MemTable) CloneTable() (*ST_MemTable, error) {
	if this == nil {
		return nil, errors.New("pT is null")
	}
	this.m_RWLock.RLock()
	defer this.m_RWLock.RUnlock()
	cloneTable := NewMemTable(this.colNameType)
	_, _, inRows, _ := this.GetRows(0, -1)
	for _, InsertRowOne := range inRows {
		_, err := cloneTable.InsertRow(InsertRowOne)
		if err != nil {
			return nil, err
		}
	}
	return cloneTable, nil
}
func (this *ST_MemTable) InserTable(inPT *ST_MemTable) (*ST_MemTable, error) {
	if this == nil || inPT == nil {
		return nil, errors.New("pT is null")
	}

	outTable, err := this.CloneTable()
	if err != nil {
		return nil, err
	}
	inPT.m_RWLock.RLock()
	defer inPT.m_RWLock.RUnlock()
	_, _, inRows, _ := inPT.GetRows(0, -1)
	for _, valRowsOne := range inRows {
		insertRowOne := make(map[string]interface{})
		for _, val := range outTable.GetColNames() {
			insertRowOne[val] = valRowsOne.GetVal(val)
		}
		_, err := outTable.InsertRow(insertRowOne)
		if err != nil {
			return nil, err
		}
	}
	return outTable, nil
}

//pT1 join pT2
//Join--如果联合主键相同，则以pT2相同字段覆盖pT1相同字段内容
func (this *ST_MemTable) Join(pT2 *ST_MemTable, whereColNameEqual map[string]string) (outPT *ST_MemTable, effectRows int) {
	if this == nil || pT2 == nil {
		return nil, -1
	}
	joinMapNameType := make(map[string]string)
	joinRemark := make(map[string]string)
	for key, val := range this.colNameType {
		joinMapNameType[key] = val
		joinRemark[key] = this.colNameRemark[key]
	}
	for key, val := range pT2.colNameType {
		joinMapNameType[key] = val
		joinRemark[key] = pT2.colNameRemark[key]
	}
	retPT := NewMemTable(joinMapNameType)
	//备注加入
	for key, val := range joinRemark {
		retPT.AddRemark(key, val)
	}
	//n^2匹配
	this.m_RWLock.RLock()
	defer this.m_RWLock.RUnlock()
	for _, valMap1 := range this.memTable {
		if valMap1.GetVal("m_ValidStatus") == 1 {
			joinMapRow := make(map[string]interface{})
			pT2.m_RWLock.RLock()
			for _, valMap2 := range pT2.memTable {
				if valMap2.GetVal("m_ValidStatus") == 1 {
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
			pT2.m_RWLock.RUnlock()
		}
	}
	return retPT, retPT.GetRowCount()
}
func (this *ST_MemTable) LeftJoin(pT2 *ST_MemTable, whereColNameEqual map[string]string) (outPT *ST_MemTable, effectRows int) {
	if this == nil || pT2 == nil {
		return nil, -1
	}
	joinMapNameType := make(map[string]string)
	joinRemark := make(map[string]string)
	for key, val := range this.colNameType {
		joinMapNameType[key] = val
		joinRemark[key] = this.colNameRemark[key]
	}
	for key, val := range pT2.colNameType {
		joinMapNameType[key] = val
		joinRemark[key] = pT2.colNameRemark[key]
	}
	retPT := NewMemTable(joinMapNameType)
	for key, val := range joinRemark {
		retPT.AddRemark(key, val)
	}
	//n^2匹配
	this.m_RWLock.RLock()
	defer this.m_RWLock.RUnlock()
	for _, valMap1 := range this.memTable {
		if valMap1.GetVal("m_ValidStatus") == 1 {
			joinMapRow := make(map[string]interface{})
			for key1, val1 := range valMap1 {
				joinMapRow[key1] = val1
			}
			rowMatchCnt := 0
			pT2.m_RWLock.RLock()
			for _, valMap2 := range pT2.memTable {
				if valMap2.GetVal("m_ValidStatus") == 1 {
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
			}
			pT2.m_RWLock.RUnlock()
			if rowMatchCnt == 0 {
				retPT.InsertRow(joinMapRow)
			}
		}
	}
	return retPT, retPT.GetRowCount()
}

//Join_1Cover2--如果联合主键相同，则以pT1相同字段覆盖pT2相同字段内容
//Join_2Cover1--如果联合主键相同，则以pT2相同字段覆盖pT1相同字段内容
func (this *ST_MemTable) Join_1Cover2(pT2 *ST_MemTable, whereColNameEqual map[string]string) (outPT *ST_MemTable, effectRows int) {
	if this == nil {
		return nil, -1
	}
	joinMapNameType := make(map[string]string)
	joinRemark := make(map[string]string)
	for key, val := range pT2.colNameType {
		joinMapNameType[key] = val
		joinRemark[key] = pT2.colNameRemark[key]
	}
	for key, val := range this.colNameType {
		joinMapNameType[key] = val
		joinRemark[key] = this.colNameRemark[key]
	}
	retPT := NewMemTable(joinMapNameType)
	//备注加入
	for key, val := range joinRemark {
		retPT.AddRemark(key, val)
	}
	//n^2匹配
	this.m_RWLock.RLock()
	defer this.m_RWLock.RUnlock()
	for _, valMap1 := range this.memTable {
		if valMap1.GetVal("m_ValidStatus") == 1 {
			joinMapRow := make(map[string]interface{})
			pT2.m_RWLock.RLock()
			for _, valMap2 := range pT2.memTable {
				if valMap2.GetVal("m_ValidStatus") == 1 {
					mathCnt := 0
					for WhereStr1, WhereStr2 := range whereColNameEqual {
						if valMap1[WhereStr1] == valMap2[WhereStr2] {
							mathCnt++
						}
					}
					if mathCnt == len(whereColNameEqual) {
						for key2, val2 := range valMap2 {
							joinMapRow[key2] = val2
						}
						for key1, val1 := range valMap1 {
							joinMapRow[key1] = val1
						}
						retPT.InsertRow(joinMapRow)
					}
				}
			}
			pT2.m_RWLock.RUnlock()
		}
	}
	return retPT, retPT.GetRowCount()
}
func (this *ST_MemTable) Join_2Cover1(pT2 *ST_MemTable, whereColNameEqual map[string]string) (outPT *ST_MemTable, effectRows int) {
	if this == nil || pT2 == nil {
		return nil, -1
	}
	joinMapNameType := make(map[string]string)
	joinRemark := make(map[string]string)
	for key, val := range this.colNameType {
		joinMapNameType[key] = val
		joinRemark[key] = this.colNameRemark[key]
	}
	for key, val := range pT2.colNameType {
		joinMapNameType[key] = val
		joinRemark[key] = pT2.colNameRemark[key]
	}
	retPT := NewMemTable(joinMapNameType)
	//备注加入
	for key, val := range joinRemark {
		retPT.AddRemark(key, val)
	}
	//n^2匹配
	this.m_RWLock.RLock()
	defer this.m_RWLock.RUnlock()
	for _, valMap1 := range this.memTable {
		if valMap1.GetVal("m_ValidStatus") == 1 {
			joinMapRow := make(map[string]interface{})
			pT2.m_RWLock.RLock()
			for _, valMap2 := range pT2.memTable {
				if valMap2.GetVal("m_ValidStatus") == 1 {
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
			pT2.m_RWLock.RUnlock()
		}
	}
	return retPT, retPT.GetRowCount()
}
func (this *ST_MemTable) LeftJoin_1Cover2(pT2 *ST_MemTable, whereColNameEqual map[string]string) (outPT *ST_MemTable, effectRows int) {
	if this == nil || pT2 == nil {
		return nil, -1
	}
	joinMapNameType := make(map[string]string)
	joinRemark := make(map[string]string)
	for key, val := range pT2.colNameType {
		joinMapNameType[key] = val
		joinRemark[key] = pT2.colNameRemark[key]
	}
	for key, val := range this.colNameType {
		joinMapNameType[key] = val
		joinRemark[key] = this.colNameRemark[key]
	}
	retPT := NewMemTable(joinMapNameType)
	for key, val := range joinRemark {
		retPT.AddRemark(key, val)
	}
	//n^2匹配
	this.m_RWLock.RLock()
	defer this.m_RWLock.RUnlock()
	for _, valMap1 := range this.memTable {
		if valMap1.GetVal("m_ValidStatus") == 1 {
			joinMapRow := make(map[string]interface{})
			for key1, val1 := range valMap1 {
				joinMapRow[key1] = val1
			}
			rowMatchCnt := 0
			pT2.m_RWLock.RLock()
			for _, valMap2 := range pT2.memTable {
				if valMap2.GetVal("m_ValidStatus") == 1 {
					oneRowMathCnt := 0
					for WhereStr1, WhereStr2 := range whereColNameEqual {
						if valMap1[WhereStr1] == valMap2[WhereStr2] {
							oneRowMathCnt++
						}
					}
					if oneRowMathCnt == len(whereColNameEqual) {
						for key2, val2 := range valMap2 {
							if _, ok := joinMapRow[key2]; !ok {
								joinMapRow[key2] = val2
							}
						}
						rowMatchCnt++
						retPT.InsertRow(joinMapRow)
					}
				}
			}
			pT2.m_RWLock.RUnlock()
			if rowMatchCnt == 0 {
				retPT.InsertRow(joinMapRow)
			}
		}
	}
	return retPT, retPT.GetRowCount()
}
func (this *ST_MemTable) LeftJoin_2Cover1(pT2 *ST_MemTable, whereColNameEqual map[string]string) (outPT *ST_MemTable, effectRows int) {
	if this == nil || pT2 == nil {
		return nil, -1
	}
	joinMapNameType := make(map[string]string)
	joinRemark := make(map[string]string)
	for key, val := range this.colNameType {
		joinMapNameType[key] = val
		joinRemark[key] = this.colNameRemark[key]
	}
	for key, val := range pT2.colNameType {
		joinMapNameType[key] = val
		joinRemark[key] = pT2.colNameRemark[key]
	}
	retPT := NewMemTable(joinMapNameType)
	for key, val := range joinRemark {
		retPT.AddRemark(key, val)
	}
	//n^2匹配
	this.m_RWLock.RLock()
	defer this.m_RWLock.RUnlock()
	for _, valMap1 := range this.memTable {
		if valMap1.GetVal("m_ValidStatus") == 1 {
			joinMapRow := make(map[string]interface{})
			for key1, val1 := range valMap1 {
				joinMapRow[key1] = val1
			}
			rowMatchCnt := 0
			pT2.m_RWLock.RLock()
			for _, valMap2 := range pT2.memTable {
				if valMap2.GetVal("m_ValidStatus") == 1 {
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
			}
			pT2.m_RWLock.RUnlock()
			if rowMatchCnt == 0 {
				retPT.InsertRow(joinMapRow)
			}
		}
	}
	return retPT, retPT.GetRowCount()
}

//对表行 关键字去重
func (this *ST_MemTable) GroupBy_Limit1st(colName string) (error) {
	if this == nil {
		return errors.New("pT 空")
	}
	if !this.CheckColNameExist(colName) {
		return errors.New("GroupBy_Limit1:" + "找不到对应列(" + colName + ")")
	}
	this.m_RWLock.Lock()
	defer this.m_RWLock.Unlock()
	cnt := this.GetRowCount_Total()
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

//对表行 关键字去重
func (this *ST_MemTable) GroupBy_Limit1(colNameList []string) (error) {
	if this == nil {
		return errors.New("pT 空")
	}
	for _, val := range colNameList {
		if !this.CheckColNameExist(val) {
			return errors.New("GroupBy_Limit1:" + "找不到对应列(" + val + ")")
		}
	}
	this.m_RWLock.Lock()
	defer this.m_RWLock.Unlock()
	cnt := this.GetRowCount_Total()
	j := cnt - 1
	//从后向前 删除重复数据
	for j >= 0 {
		for i, TableRow := range this.memTable {
			if TableRow["m_ValidStatus"] == 1 {
				if i < j {
					isFoundCnt := 0
					for _, colNameTmp := range colNameList {
						if TableRow.GetVal(colNameTmp) == this.memTable[j].GetVal(colNameTmp) {
							isFoundCnt++
						}
					}
					if isFoundCnt == len(colNameList) && isFoundCnt != 0 {
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
	if this == nil {
		return errors.New("pT 空")
	}
	if !this.CheckColNameExist(colName) {
		return errors.New("GroupBy:" + "找不到对应列(" + colName + ")")
	}
	this.m_RWLock.Lock()
	defer this.m_RWLock.Unlock()
	cnt := this.GetRowCount_Total()
	i := 0
	//增加一列用于统计合并个数
	this.colNameType["m_Count"] = "int"
	//从前往后，合并组数据
	for i < cnt {
		if this.memTable[i].GetInt("m_ValidStatus") == 1 {
			colValue := this.memTable[i].GetVal(colName) //用于填补group by ，保留一个值
			foundRow := make([]st_MemTable_Row, 0)
			foundRow = append(foundRow, this.memTable[i])
			for j, _ := range this.memTable {
				if this.memTable[j].GetInt("m_ValidStatus") == 1 {
					if j > i {
						if this.memTable[i].GetVal(colName) == this.memTable[j].GetVal(colName) {
							foundRow = append(foundRow, this.memTable[j])
							this.memTable[j].SetVal("m_ValidStatus", -1)
						}
					}
				}
			}
			//对i的行进行合并，以字符串+，方式拼接
			mapSlice := make(map[string][]interface{})
			for _, rowMap := range foundRow {
				for key, val := range rowMap {
					mapSlice[key] = append(mapSlice[key], val)
				}
			}
			for key, slice := range mapSlice {
				this.memTable[i].SetVal(key, SliceToString(slice, ","))
			}
			this.memTable[i].SetVal("m_ValidStatus", 1)
			this.memTable[i].SetVal("m_Count", len(foundRow))
			this.memTable[i].SetVal(colName, colValue)
		}
		i++
	}
	//对列格式作调整
	for key, _ := range this.colNameType {
		this.colNameType[key] = "string"
	}
	this.colNameType["m_ValidStatus"] = "int"
	this.colNameType["m_Count"] = "int"
	return nil
}

//获取非等宽字体 打印宽度
func StringPrintWidth(in string) int {
	outLen := 0
	for _, val := range (in) {
		//占用1个字节宽度
		//if unicode.Is(unicode.Scripts["Han"], val) {
		if val > 255 {
			outLen += 2
		} else {
			outLen += 1
		}
	}
	return outLen
}

//Cliff 打印表结构到log中
func FormatColString(inString string, lenMax8 int) string {
	right := lenMax8 - StringPrintWidth(inString)
	if right > 0 { //对齐显示
		for right > 0 {
			inString += " "
			right--
		}
	} else if right < 0 { //截断超出显示部分
		for right < 0 {
			len2 := len(inString)
			if len2 > 0 {
				inString = inString[:len2-1]
			} else {
				break
			}
			right = lenMax8 - StringPrintWidth(inString)
		}
		if right > 0 {
			for ; right > 0; right-- {
				inString += " "
			}
		}
	}
	return inString
}
func (this *ST_MemTable) PrintTable() ([]string) {
	if this == nil {
		return []string{}
	}
	this.m_RWLock.RLock()
	defer this.m_RWLock.RUnlock()

	outStringList := make([]string, 0)
	outStringListOne := "" //头
	colNameOrder := SortSliceString{}
	colLen8 := make(map[string]int)
	for key, _ := range this.colNameType {
		colNameOrder = append(colNameOrder, key)
	}
	colNameOrder.Sort_ASC()
	//获取每列最大长度
	for _, colName := range colNameOrder {
		oneColString := (" | " + colName + "(" + this.colNameType[colName] + ")")
		lenTmp1 := StringPrintWidth(oneColString)
		if lenTmp1 > colLen8[colName] {
			colLen8[colName] = lenTmp1
		}
		for _, val := range this.memTable {
			lenTmp := StringPrintWidth(" | " + val.GetValToString(colName))
			if lenTmp > colLen8[colName] {
				colLen8[colName] = lenTmp
			}
		}
		if colLen8[colName] > printMaxLen {
			colLen8[colName] = printMaxLen
		}
	}

	//打印头
	outStringListOne += "字段"
	for _, colNameVal := range colNameOrder {
		oneColString2 := (" | " + colNameVal + "(" + this.colNameType[colNameVal] + ")")
		outStringListOne += FormatColString(oneColString2, colLen8[colNameVal])
	}
	outStringList = append(outStringList, outStringListOne)

	//打印主体
	for _, val := range this.memTable {
		outStringListOne_tmp := "内容"
		for _, colNameVal := range colNameOrder {
			oneColString2 := (" | " + val.GetValToString(colNameVal))
			outStringListOne_tmp += FormatColString(oneColString2, colLen8[colNameVal])
		}
		outStringList = append(outStringList, outStringListOne_tmp)
	}
	return outStringList
}
func (this *ST_MemTable) PrintTable_Remark() ([]string) {
	if this==nil{
		return []string{}
	}
	this.m_RWLock.RLock()
	defer this.m_RWLock.RUnlock()
	outStringList := make([]string, 0)
	outStringListOne := ""  //头
	outStringListOne2 := "" //备注

	colNameOrder := SortSliceString{}
	colLen8 := make(map[string]int)
	for key, _ := range this.colNameType {
		colNameOrder = append(colNameOrder, key)
	}
	colNameOrder.Sort_ASC()
	//获取每列最大长度
	for key, val := range this.colNameRemark {
		lenTmp := StringPrintWidth(" | " + val)
		colLen8[key] = lenTmp
	}

	for _, colName := range colNameOrder {
		oneColString := (" | " + colName + "(" + this.colNameType[colName] + ")")
		lenTmp1 := StringPrintWidth(oneColString)
		if lenTmp1 > colLen8[colName] {
			colLen8[colName] = lenTmp1
		}
		for _, val := range this.memTable {
			lenTmp := StringPrintWidth(" | " + val.GetValToString(colName))
			if lenTmp > colLen8[colName] {
				colLen8[colName] = lenTmp
			}
		}
		//截止最长
		if colLen8[colName] > printMaxLen {
			colLen8[colName] = printMaxLen
		}
	}

	//打印头
	outStringListOne += "字段"
	for _, colNameVal := range colNameOrder {
		oneColString2 := (" | " + colNameVal + "(" + this.colNameType[colNameVal] + ")")
		outStringListOne += FormatColString(oneColString2, colLen8[colNameVal])
	}
	outStringList = append(outStringList, outStringListOne)

	//打印备注
	outStringListOne2 += "备注"
	for _, colNameVal := range colNameOrder {
		oneColString2 := (" | " + this.colNameRemark[colNameVal])
		outStringListOne2 += FormatColString(oneColString2, colLen8[colNameVal])
	}
	outStringList = append(outStringList, outStringListOne2)

	//打印主体
	for _, val := range this.memTable {
		outStringListOne_tmp := "内容"
		for _, colNameVal := range colNameOrder {
			oneColString2 := (" | " + val.GetValToString(colNameVal))
			outStringListOne_tmp += FormatColString(oneColString2, colLen8[colNameVal])
		}
		outStringList = append(outStringList, outStringListOne_tmp)
	}
	return outStringList
}

//对表进行关键列排序，目前只支持int类型，后续加入时间排序
func (this *ST_MemTable) Sort_ASC(ColName string) {
	if this == nil{
		return
	}
	this.m_RWLock.Lock()
	defer this.m_RWLock.Unlock()
	this.colNameOrder = ColName
	if !sort.IsSorted(this) {
		sort.Sort(this)
	}
}
func (this *ST_MemTable) Sort_DESC(ColName string) {
	if this == nil{
		return
	}
	this.m_RWLock.Lock()
	defer this.m_RWLock.Unlock()
	this.colNameOrder = ColName
	if !sort.IsSorted(this) {
		sort.Sort(this)
	}
	i := 0
	j := this.GetRowCount_Total() - 1
	for i < j {
		this.memTable[i], this.memTable[j] = this.memTable[j], this.memTable[i]
		i++
		j--
	}
}
func (this *ST_MemTable) Len() int {
	if this == nil{
		return 0
	}
	return this.GetRowCount_Total()
}
func (this *ST_MemTable) Less(i, j int) bool {
	if this==nil{
		return false
	}
	outmap1 := this.memTable[i]
	outmap2 := this.memTable[j]
	return outmap1.GetInt(this.colNameOrder) < outmap2.GetInt(this.colNameOrder)
}
func (this *ST_MemTable) Swap(i, j int) {
	if this==nil{
		return
	}
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
func (stMyInterfaceConv) GetValToString(in interface{}) string {
	if in != nil {
		switch in.(type) {
		case string:
			return in.(string)
		case int:
			return strconv.Itoa(in.(int))
		case int64:
			return strconv.FormatInt(in.(int64), 10)
		case float64:
			return strconv.FormatFloat(in.(float64), 'f', 0, 0)
		default:
			return ""
		}
	} else {
		return ""
	}
}
func (stMyInterfaceConv) GetValToSlice(in interface{}) []interface{} {
	if inList, ok := in.([]interface{}); ok {
		return inList
	} else {
		return nil
	}
}
func (stMyInterfaceConv) GetValToSliceInt(in interface{}) []int {
	outList := make([]int, 0)
	if inList, ok := in.([]interface{}); ok {
		for _, val := range inList {
			if x, ok := val.(int); ok {
				outList = append(outList, x)
			}
		}
	}
	return outList
}
func (stMyInterfaceConv) GetValToSliceInt64(in interface{}) []int64 {
	outList := make([]int64, 0)
	if inList, ok := in.([]interface{}); ok {
		for _, val := range inList {
			if x, ok := val.(int64); ok {
				outList = append(outList, x)
			}
		}
	}
	return outList
}
func (stMyInterfaceConv) GetValToSliceString(in interface{}) []string {
	outList := make([]string, 0)
	if inList, ok := in.([]interface{}); ok {
		for _, val := range inList {
			if x, ok := val.(string); ok {
				outList = append(outList, x)
			}
		}
	}
	return outList
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

type SortSliceString []string

func (this SortSliceString) Sort_ASC() {
	if !sort.IsSorted(this) {
		sort.Sort(this)
	}
}
func (this SortSliceString) Sort_DESC() {
	this.Sort_ASC()
	i := 0
	j := len(this) - 1
	for i < j {
		this.Swap(i, j)
		i++
		j--
	}
}
func (this SortSliceString) Len() int {
	return len(this)
}
func (this SortSliceString) Swap(i, j int) {
	this[i], this[j] = this[j], this[i]
}

//字符串对比首字母asc码大小
func (this SortSliceString) Less(i, j int) bool {
	if len(this[i]) != 0 && len(this[j]) != 0 {
		leni := len(this[i])
		lenj := len(this[j])
		lenMin := leni
		if lenMin > lenj {
			lenMin = lenj
		}
		for iTmp := 0; iTmp < lenMin; iTmp++ {
			if this[i][iTmp] < this[j][iTmp] {
				return true
			} else if this[i][iTmp] > this[j][iTmp] {
				return false
			}
		}
	}
	return len(this[i]) < len(this[j])
}
