package main

import (
	"fmt"
	"git.woda.ink/woda/common/utils/CliffMemSQL"
)

//微服务接口错误码枚举值
func TestMyMemSQL(){
	colNameType := make(map[string]string)
	colNameType["hello"] = "string"
	colNameType["nihao"] = "string"
	colNameType["dajiahao"] = "string"
	colNameType["countx"] = "int"
	pT := CliffMemSQL.NewMemTable(colNameType)

	insertRowTmp := make(map[string]interface{})
	insertRowTmp["hello"]="a"
	insertRowTmp["nihao"]= "b"
	insertRowTmp["dajiahao"]="c"
	insertRowTmp["countx"]=3
	pT.InsertRow(insertRowTmp)
	insertRowTmp["hello"]="aa"
	insertRowTmp["nihao"]="bb"
	insertRowTmp["dajiahao"]="cc"
	insertRowTmp["countx"]=1
	pT.InsertRow(insertRowTmp)
	insertRowTmp["hello"]="aaa"
	insertRowTmp["nihao"]="bbb"
	insertRowTmp["dajiahao"]="ccc"
	insertRowTmp["countx"]=10
	pT.InsertRow(insertRowTmp)

	pT.Sort_DESC("countx")
	fmt.Println(pT)
	pT.Sort_ASC("countx")
	fmt.Println(pT)
	//setRowTmp := make(map[string]interface{})
	//setRowTmp["hello"]="ab"
	//whereRowTmp := make(map[string]interface{})
	//whereRowTmp["nihao"]="b"
	//pT.UpdateRow(setRowTmp,whereRowTmp)
	//fmt.Println(pT)
	//pT.DeleteRow(whereRowTmp)
	//fmt.Println(pT)
	//
	//
	//pT.DeleteRow(whereRowTmp)
	//fmt.Println(pT)
	//
	//fmt.Println(pT.GetRows(0,2))
	//fmt.Println(pT.QueryRowsLike(whereRowTmp))
}

func main() {
	TestMyMemSQL()



}

