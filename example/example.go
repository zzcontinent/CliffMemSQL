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

	colNameType2 := make(map[string]string)
	colNameType2["hello2"] = "string"
	colNameType2["nihao2"] = "string"
	colNameType2["dajiahao2"] = "string"
	colNameType2["countx"] = "int"
	pT2 := CliffMemSQL.NewMemTable(colNameType2)

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

	insertRowTmp2 := make(map[string]interface{})
	insertRowTmp2["hello2"]="a2"
	insertRowTmp2["nihao2"]= "b2"
	insertRowTmp2["dajiahao2"]="c2"
	insertRowTmp2["countx"]=3
	pT2.InsertRow(insertRowTmp2)

	where :=make(map[string]string)
	where["countx"]="countx"
	pT3,_ := pT.LeftJoin(pT2,where)
	_,_,x,_ :=pT3.GetRows(0,-1)
	fmt.Println(x)

}

func main() {
	TestMyMemSQL()



}

