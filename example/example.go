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
	pT := CliffMemSQL.NewMemTable(colNameType)
	fmt.Println(pT)
	insertRowTmp := make(map[string]interface{})
	insertRowTmp["hello"]="a"
	insertRowTmp["nihao"]= "b"
	insertRowTmp["dajiahao"]="c"
	pT.InsertRow(insertRowTmp)
	fmt.Println(pT)
	insertRowTmp["hello"]="aa"
	insertRowTmp["nihao"]="bb"
	insertRowTmp["dajiahao"]="cc"
	pT.InsertRow(insertRowTmp)
	fmt.Println(pT)


	setRowTmp := make(map[string]interface{})
	setRowTmp["hello"]="ab"
	whereRowTmp := make(map[string]interface{})
	whereRowTmp["nihao"]="b"
	pT.UpdateRow(setRowTmp,whereRowTmp)
	fmt.Println(pT)
	pT.DeleteRow(whereRowTmp)
	fmt.Println(pT)


	pT.DeleteRow(whereRowTmp)
	fmt.Println(pT)

	fmt.Println(pT.GetRows(0,2))
	fmt.Println(pT.QueryRowsLike(whereRowTmp))
}

func main() {
	TestMyMemSQL()



}

