package main

import (
	"fmt"
	"git.woda.ink/woda/common/utils/CliffMemSQL"
	"time"
	log "github.com/xiaomi-tc/log15"
	"runtime/debug"
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
	elapsed := time.Since(tStart)
	fmt.Println("一分钱测试导入结束 ", " 时间= "+time.Now().String(), " 耗时:"+elapsed.String())
	for i := 0; i < 100; i++ {
		pT.InsertRow(insertRowTmp)
	}
	elapsed = time.Since(tStart)
	fmt.Println("一分钱测试导入结束 ", " 时间= "+time.Now().String(), " 耗时:"+elapsed.String())

	pT.AddRemark("hello", "记返费给的状态\n0\t未处理\n1\t未面试\n2\t通过\n3\t未通过\n4\t放弃")
	pT.AddRemark("nihao", "你好2")
	pT.AddRemark("dajiahao", "大家好")
	pT.AddRemark("countx", "总计1111大家好大家好大家好大家好11111111家好1111$$$$家好$家好$$$$$$$、$$$家好$$$$$$$$$$#、家好######家好#####、##、#家好大家####家好##家好#家好###家好123、##########@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!$$$$$$$@@@@@@@@@@@@@@@@@@@@@@@@@!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!$$$$$$$33333333333333333")
	pT2.AddRemark("countx", "总计1111")

	//for _,val := range pT.PrintTable(){
	//	fmt.Println(val)
	//}
	//for _,val := range pT2.PrintTable(){
	//	fmt.Println(val)
	//}

	//pTinsert,_ := pT.InserTable(pT2)
	pT.InserTable(pT2)
	//for _,val := range pTinsert.PrintTable(){
	//	fmt.Println(val)
	//}
	elapsed = time.Since(tStart)
	fmt.Println("一分钱测试导入结束 ", " 时间= "+time.Now().String(), " 耗时:"+elapsed.String())

	whereInAnd := make(map[string][]interface{})
	whereInAnd["hello"] = make([]interface{}, 0)
	whereInAnd["nihao"] = make([]interface{}, 0)
	whereInAnd["hello"] = append(whereInAnd["hello"], "a", "aa")
	whereInAnd["nihao"] = append(whereInAnd["nihao"], "b", "")
	pT.QueryTableInAnd(whereInAnd)

	elapsed = time.Since(tStart)
	fmt.Println("一分钱测试导入结束 ", " 时间= "+time.Now().String(), " 耗时:"+elapsed.String())

	//for _,val := range pT.PrintTable(){
	//	fmt.Println(val)
	//}
	//for _,val := range pTIn.PrintTable(){
	//	fmt.Println(val)
	//}

	//whereMapJoin := make(map[string]string)
	//whereMapJoin["countx"] = "countx"

	//p1_2, _ := pT.Join_1Cover2(pT2, whereMapJoin)
	//p2_1, _ := pT.Join_2Cover1(pT2, whereMapJoin)
	//
	//pT.GroupBy("hello")
	//for _, val := range pT.PrintTable() {
	//	fmt.Println(val)
	//}
	//for i := 0; i < pT.GetRowCount_Total(); i++ {
	//	fmt.Println(pT.GetRows(i, 1))
	//}
	//for i := 0; i < pT.GetRowCount(); i++ {
	//	fmt.Println(pT.GetRows_IndexOK(i, 1))
	//}
	//for i := 0; i < pT.GetRowCount(); i++ {
	//	fmt.Println(pT.GetRows(i, -1))
	//}
	//
	//for _, val := range p1_2.PrintTable() {
	//	fmt.Println(val)
	//}
	//for _, val := range p2_1.PrintTable() {
	//	fmt.Println(val)
	//}

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

	//where := make(map[string]string)
	//where["countx"] = "countx"
	//pT3, _ := pT.LeftJoin(pT2, where)
	//_, _, _, _ := pT3.GetRows(0, -1)

	//for _, val := range pT.PrintTable_Remark() {
	//	fmt.Println(val)
	//}
	//for _, val := range pT.PrintTable() {
	//	fmt.Println(val)
	//}
	//outPt, _ := pT.Subset([]string{"hello", "nihao"})
	//for _, val := range outPt.PrintTable_Remark() {
	//	fmt.Println(val)
	//}

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

var tStart time.Time // get current time
func main() {
	tStart = time.Now() // get current time
	fmt.Println("一分钱测试导入开始 ", " 时间= ", tStart.String())
	defer func() {
		if err := recover(); err != nil {
			log.Crit("OneCentTestBat", "panic: ", err)
			log.Crit("OneCentTestBat", "stack: ", string(debug.Stack()))
		}
		elapsed := time.Since(tStart)
		log.Info("一分钱测试导入结束 ", " 时间= "+time.Now().String(), " 耗时:"+elapsed.String())
		fmt.Println("一分钱测试导入结束 ", " 时间= "+time.Now().String(), " 耗时:"+elapsed.String())
	}()
	testMultiThread()
	//TestMyMemSQL()

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

func testMultiThread() {
	colNameType := make(map[string]string)
	colNameType["hello"] = "string"
	colNameType["cnt"] = "int"
	colNameType["id"] = "int64"
	pT := CliffMemSQL.NewMemTable(colNameType)
	writeCnt1 := 0
	writeCnt2 := 0
	go func() {
		for i := 0; i < 100000; i++ {
			insertRow := make(map[string]interface{})
			insertRow["hello"] = "nihao"
			insertRow["cnt"] = i
			insertRow["id"] = int64(i * 100)
			_, err := pT.InsertRow(insertRow)
			if err != nil {
				fmt.Println(err)
			}
			writeCnt1++
		}
		fmt.Println("w1 end")
	}()
	go func() {
		for i := 0; i < 100000; i++ {
			insertRow := make(map[string]interface{})
			insertRow["hello"] = "nihao"
			insertRow["cnt"] = i
			insertRow["id"] = int64(i * 100)
			_, err := pT.InsertRow(insertRow)
			if err != nil {
				fmt.Println(err)
			}
			writeCnt2++
		}
		fmt.Println("w2 end")
	}()

	queryCnt1 := 0
	queryCnt2 := 0
	go func() {
		for i := 0; i < 100000; i++ {
			whereMapIn := make(map[string][]interface{})
			whereMapIn["hello"] = []interface{}{"nihao"}
			whereMapIn["cnt"] = []interface{}{1, 2, 3, 4, 5, 15, 16, 17, 18, 19, 20}
			whereMapIn["id"] = []interface{}{100, 200, 300, 400, 500, 800, 1000, 100, 200, 300, 400, 500, 800, 1000, 100, 200, 300, 400, 500, 800, 1000, 100, 200, 300, 400, 500, 800, 1000, 100, 200, 300, 400, 500, 800, 1000, 100, 200, 300, 400, 500, 800, 1000, 100, 200, 300, 400, 500, 800, 1000, 100, 200, 300, 400, 500, 800, 1000, 100, 200, 300, 400, 500, 800, 1000, 100, 200, 300, 400, 500, 800, 1000, 100, 200, 300, 400, 500, 800, 1000, 100, 200, 300, 400, 500, 800, 1000, 100, 200, 300, 400, 500, 800, 1000, 100, 200, 300, 400, 500, 800, 1000, 100, 200, 300, 400, 500, 800, 1000, 100, 200, 300, 400, 500, 800, 1000, 100, 200, 300, 400, 500, 800, 1000, 100, 200, 300, 400, 500, 800, 1000, 100, 200, 300, 400, 500, 800, 1000, 100, 200, 300, 400, 500, 800, 1000,}
			_, err := pT.QueryTableInAnd(whereMapIn)
			if err != nil {
				fmt.Println(err)
			}
			queryCnt1++
		}
		fmt.Println("r1 end")
	}()
	go func() {
		for i := 0; i < 100000; i++ {
			whereMapIn := make(map[string][]interface{})
			whereMapIn["hello"] = []interface{}{"nihao"}
			whereMapIn["cnt"] = []interface{}{1, 2, 3, 4, 5, 15, 16, 17, 18, 19, 20}
			whereMapIn["id"] = []interface{}{100, 200, 300, 400, 500, 800, 1000}
			_, err := pT.QueryTableInAnd(whereMapIn)
			if err != nil {
				fmt.Println(err)
			}
			queryCnt2++
		}
		fmt.Println("r2 end")
	}()
	for i := 0; i < 100; i++ {
		time.Sleep(time.Second)
		fmt.Println(writeCnt1, writeCnt2, queryCnt1, queryCnt2)
	}

}
