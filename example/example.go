package main

import (
	"fmt"
	"git.woda.ink/woda/common/utils/CliffMemSQL"
)

//微服务接口错误码枚举值
func TestMyMemSQL() {
	colNameType := make(map[string]string)
	colNameType["hello"] = "string"
	colNameType["nihao"] = "string"
	colNameType["dajiahao"] = "string"
	colNameType["countx"] = "int"
	pT := CliffMemSQL.NewMemTable(colNameType)

	colNameType2 := make(map[string]string)
	colNameType2["hello"] = "string"
	colNameType2["nihao2"] = "string"
	colNameType2["dajiahao2"] = "string"
	colNameType2["countx"] = "int"
	pT2 := CliffMemSQL.NewMemTable(colNameType2)

	insertRowTmp := make(map[string]interface{})
	insertRowTmp2 := make(map[string]interface{})

	insertRowTmp2["hello"] = "a"
	insertRowTmp2["nihao2"] = "b"
	insertRowTmp2["dajiahao2"] = "c"
	insertRowTmp2["countx"] = 3
	pT2.InsertRow(insertRowTmp2)
	insertRowTmp2["hello"] = "aa2"
	insertRowTmp2["nihao2"] = "bbbbb2"
	insertRowTmp2["dajiahao2"] = "ccccc2"
	insertRowTmp2["countx"] = 3
	pT2.InsertRow(insertRowTmp2)
	insertRowTmp2["hello"] = "aaa4"
	insertRowTmp2["nihao2"] = "bbb4"
	insertRowTmp2["dajiahao2"] = "ccc4"
	insertRowTmp2["countx"] = 4
	pT2.InsertRow(insertRowTmp2)

	insertRowTmp["hello"] = "aa"
	insertRowTmp["nihao"] = "bbbbb"
	insertRowTmp["dajiahao"] = "ccccc"
	insertRowTmp["countx"] = 12
	pT.InsertRow(insertRowTmp)

	insertRowTmp["hello"] = "a"
	insertRowTmp["nihao"] = "b"
	insertRowTmp["dajiahao"] = "c"
	insertRowTmp["countx"] = 3
	pT.InsertRow(insertRowTmp)

	insertRowTmp["hello"] = "aa"
	insertRowTmp["nihao"] = "bb"
	insertRowTmp["dajiahao"] = "cc"
	insertRowTmp["countx"] = 1
	pT.InsertRow(insertRowTmp)
	insertRowTmp["hello"] = "aaa"
	insertRowTmp["nihao"] = "bbb"
	insertRowTmp["dajiahao"] = "ccc"
	insertRowTmp["countx"] = 14
	pT.InsertRow(insertRowTmp)
	insertRowTmp = make(map[string]interface{})
	insertRowTmp["hello"] = "aaa"
	insertRowTmp["nihao"] = "bbb"
	insertRowTmp["dajiahao"] = "ccc"
	pT.InsertRow(insertRowTmp)
	insertRowTmp = make(map[string]interface{})
	insertRowTmp["hello"] = "aaaa"
	insertRowTmp["nihao"] = "bbba"
	insertRowTmp["dajiahao"] = "ccca"
	pT.InsertRow(insertRowTmp)
	insertRowTmp["hello"] = "aaaa"
	insertRowTmp["nihao"] = "bbba长度测试12313124dafadfasd"
	insertRowTmp["dajiahao"] = "bbba长度测试12313124dafadfasddfasdfasd"
	insertRowTmp["countx"] = 1
	pT.InsertRow(insertRowTmp)

	pT.AddRemark("hello", "记返费给的状态\n0\t未处理\n1\t未面试\n2\t通过\n3\t未通过\n4\t放弃")
	pT.AddRemark("nihao", "你好2")
	pT.AddRemark("dajiahao", "大家好")
	pT.AddRemark("countx", "总计1111大家好大家好大家好大家好11111111家好1111$$$$家好$家好$$$$$$$、$$$家好$$$$$$$$$$#、家好######家好#####、##、#家好大家####家好##家好#家好###家好123、##########@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!$$$$$$$@@@@@@@@@@@@@@@@@@@@@@@@@!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!$$$$$$$33333333333333333")
	pT2.AddRemark("countx", "总计1111")

	whereMapJoin := make(map[string]string)
	whereMapJoin["countx"] = "countx"
	p1_2, _ := pT.Join_1Cover2(pT2, whereMapJoin)
	p2_1, _ := pT.Join_2Cover1(pT2, whereMapJoin)

	pT.GroupBy("hello")
	for _, val := range pT.PrintTable() {
		fmt.Println(val)
	}
	for i := 0; i < pT.GetRowCount_Total(); i++ {
		fmt.Println(pT.GetRows(i, 1))
	}
	for i := 0; i < pT.GetRowCount(); i++ {
		fmt.Println(pT.GetRows_IndexOK(i, 1))
	}
	for i := 0; i < pT.GetRowCount(); i++ {
		fmt.Println(pT.GetRows(i, -1))
	}

	for _, val := range p1_2.PrintTable() {
		fmt.Println(val)
	}
	for _, val := range p2_1.PrintTable() {
		fmt.Println(val)
	}

	//p1_2L,_ :=pT.LeftJoin_1Cover2(pT2,whereMapJoin)
	//p2_1L,_ :=pT.LeftJoin_2Cover1(pT2,whereMapJoin)
	//for _,val := range p1_2L.PrintTable_Remark(){
	//	fmt.Println(val)
	//}
	//for _,val := range p2_1L.PrintTable_Remark(){
	//	fmt.Println(val)
	//}

	//pT2.AddRemark("hello","你好")
	//pT2.AddRemark("nihao","你好2")
	//pT2.AddRemark("dajiahao","大家好")
	//pT2.AddRemark("countx","总计222222222222222222222222222222222222222222")

	//whereMap := make(map[string]interface{})
	//whereMap["countx"] = 1
	//pT.DeleteRow(whereMap)

	//pT.GroupBy_Limit1st("hello")
	//fmt.Println(pT.GetRows(0,-1))
	//
	//pT.Sort_ASC("countx")
	//fmt.Println(pT.GetRows(0,-1))
	//pT.Sort_DESC("countx")
	//fmt.Println(pT.GetRows(0,-1))
	//pT
	//fmt.Println(pT.GetRows(0,-1))

	////pT
	//pT.GroupBy("hello")
	//fmt.Println(pT.PrintTable())
	//fmt.Println(pT.GetRows(0,-1))
	//tmpOut, _ :=pT.Join(pT2,whereMapJoin)
	//fmt.Println(tmpOut.GetRows(0,-1))
	//fmt.Println(pT2.GetRows(0,-1))

	//tmpOut, _ :=pT.LeftJoin(pT2,whereMapJoin)
	//fmt.Println(tmpOut.GetRows(0,-1))
	//tmpOut, _ =pT.Join(pT2,whereMapJoin)
	//fmt.Println(tmpOut.GetRows(0,-1))
	//fmt.Println(pT.GetRowCount_Total())
	//fmt.Println(pT.GetRowCount())

	//insertRowTmp2 := make(map[string]interface{})
	//insertRowTmp2["hello2"]="a2"
	//insertRowTmp2["nihao2"]= "b2"
	//insertRowTmp2["dajiahao2"]="c2"
	//insertRowTmp2["countx"]=3
	//pT2.InsertRow(insertRowTmp2)

	where := make(map[string]string)
	where["countx"] = "countx"
	//pT3, _ := pT.LeftJoin(pT2, where)
	//_, _, _, _ := pT3.GetRows(0, -1)

	for _, val := range pT.PrintTable_Remark() {
		fmt.Println(val)
	}
	for _, val := range pT.PrintTable() {
		fmt.Println(val)
	}
	outPt, _ := pT.Subset([]string{"hello", "nihao"})
	for _, val := range outPt.PrintTable_Remark() {
		fmt.Println(val)
	}

	//var tst1 []interface{}
	//var tst2 []interface{}
	//tst3 := make(map[int64][]interface{})
	//tst3[1] = append(tst3[1],1,2,3,4,5)
	//tst3[2] = append(tst3[2],4,5,6,2,3,4,5)
	//tst1 = tst3[1]
	//tst2 = tst3[2]
	//fmt.Println(tst1,tst2)
	//var x1 int
	//var x2 int
	//var y1 float32
	//x1 = 1
	//x2 = 2
	//y1 = float32(x1)/float32(x2)
	//fmt.Println(y1)
	//testx()

}

func main() {
	//a0 := "01"
	//a1 := "Hello"
	//a2 := "你好"
	//
	//fmt.Println(reflect.TypeOf(a0[0]))//uint8
	//fmt.Println(reflect.TypeOf(a1[0]))
	//fmt.Println(reflect.TypeOf(a2[0]))
	var test interface{}
	testSlice := make([]interface{}, 0)
	testSlice = append(testSlice, 0)
	testSlice = append(testSlice, 1)
	testSlice = append(testSlice, 5)
	testSlice = append(testSlice, "123")
	testSlice = append(testSlice, "321")
	testSlice = append(testSlice, int64(12))
	testSlice = append(testSlice, int64(13))
	test = testSlice
	fmt.Println(CliffMemSQL.CGetInterface.GetValToSliceInt(test))
	fmt.Println(CliffMemSQL.CGetInterface.GetValToSliceString(test))
	fmt.Println(CliffMemSQL.CGetInterface.GetValToSliceInt64(test))

	TestMyMemSQL()
	A := make([]interface{}, 0)
	B := make([]interface{}, 0)
	A = append(A, "1", "2", "3", "4", "5", "6", "7", "8", "9", "4", )
	B = append(B, "1", "2", "3", "11", "12", "13", "14", "15", "1", )
	fmt.Println(CliffMemSQL.SliceSame(A, B))
	fmt.Println(CliffMemSQL.SliceDiff(A, B))
	fmt.Println(CliffMemSQL.SliceDiffFromA(A, B))
}

func testx() {
	DepartIDList := make([]int64, 0)
	DepartIDList = append(DepartIDList, 1, 2, 3, 4, 5, 6, 7, 9, 8, 10)
	MapDepartInterviewCountPer := make(map[int64]float32)
	MapDepartTop := make(map[int64]int)
	MapDepartInterviewCountPer[1] = 0.1
	MapDepartInterviewCountPer[2] = 0.2
	MapDepartInterviewCountPer[3] = 0.5
	MapDepartInterviewCountPer[4] = 0.2
	MapDepartInterviewCountPer[5] = 0.3
	MapDepartInterviewCountPer[6] = 0.9
	MapDepartInterviewCountPer[7] = 0.15
	MapDepartInterviewCountPer[8] = 0.23
	MapDepartInterviewCountPer[9] = 0.44
	MapDepartInterviewCountPer[10] = 0.56
	//6 10 3 9 5 8 4 2 7 1
	MapDepartTop[1] = 1
	MapDepartTop[2] = 1
	MapDepartTop[3] = 1
	MapDepartTop[4] = 1
	MapDepartTop[5] = 1
	MapDepartTop[6] = 1
	MapDepartTop[7] = 1
	MapDepartTop[8] = 1
	MapDepartTop[9] = 1
	MapDepartTop[10] = 1
	for i, _ := range DepartIDList {
		j := 0
		for j <= len(DepartIDList)-1 {
			if MapDepartInterviewCountPer[DepartIDList[i]] < MapDepartInterviewCountPer[DepartIDList[j]] && i != j {
				MapDepartTop[DepartIDList[i]]++
			}
			j++
		}
	}

	fmt.Println(MapDepartTop)
}
