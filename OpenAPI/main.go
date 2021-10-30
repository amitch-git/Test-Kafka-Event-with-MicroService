package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"

	"strings"

	_ "github.com/go-sql-driver/mysql"

	"github.com/kataras/iris"
	"github.com/kataras/iris/context"
)

type Info struct {
	Name    string `json:"name"`
	Dept  string `json:"dept"`
	EmpID string `json:"id"`
	PunchTm string `json:"punch_time"`
}
type Input struct {
	Date   string `json:"date"`
	EmpID  []string `json:"emplist"`
}
type Response struct {
	Date   string `json:"date"`
	Details  []Info `json:"details"`
}
var (
	host = "localhost"
	port = "3306"
	user = "root"
	pwd = "12345"
	dbname = "emp"
	dsn = fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?timeout=5s", user, pwd, host, port, dbname)
	dbObj *sql.DB
	dberr error 
)

func main(){
	app :=iris.New()

	err := makedbConn()
	if(err != nil){
		panic(err)
	}
	app.Handle("GET","/api/v1/ping",func(ctx iris.Context){
		ctx.JSON(context.Map{"pong": "punch Service"})
	})

	app.Handle("POST","/api/v1/punch",func(ctx iris.Context){
		// read the request data and store them as they are (not recommended in production ofcourse, do your own checks here).
		body, err := ctx.GetBody()
		if err != nil {
			fmt.Println("Error in Getbody()")
			return
		}
		var resp Info
		json.Unmarshal(body, &resp)
		fmt.Println(resp.EmpID)
		fmt.Println(resp.Name)
		fmt.Println(resp.Dept)		
		fmt.Println(resp.PunchTm)	
		
		var queryArr []string
		queryArr = append(queryArr, ("INSERT INTO emp_attendence (EMP_ID, EMP_NAME, EMP_DEPT, EMP_PUNCHTM) VALUES ('"))
		queryArr = append(queryArr, (resp.EmpID + "', '"))
		queryArr = append(queryArr, (resp.Name + "', '"))
		queryArr = append(queryArr, (resp.Dept + "', '"))
		queryArr = append(queryArr, (resp.PunchTm + "') ON DUPLICATE KEY UPDATE EMP_PUNCHTM = '"))
		queryArr = append(queryArr, (resp.PunchTm + "'"))

		querystr := strings.Join(queryArr, "")
		//fmt.Println(querystr)
		savedata(querystr)
		ctx.JSON(context.Map{"stat": "ok"})
	})
	
	//Pass date and employee ID/IDs
	app.Handle("POST","/api/v1/getevents",func(ctx iris.Context){

		input := new(Input)

		body, err := ctx.GetBody()
		if(err != nil){
			fmt.Println(err.Error())
		}
		err = json.Unmarshal([]byte(body), input)
		if(err != nil){
			fmt.Println(err.Error())
		}
		if(len(input.Date) <= 0){
			ctx.JSON(context.Map{"error": "Invalid Date"})
			return;
		}
		if(len(input.EmpID) <= 0){
			ctx.JSON(context.Map{"error": "Invalid employee id list"})
			return;
		}

		//Make query and get data
		//select * from emp_attendence where EMP_PUNCHTM >= '26-10-2021 00:00:00' AND EMP_PUNCHTM <= '26-10-2021 23:59:59' AND emp_id IN ('1','1112')
		var queryArr []string
		stDate :=  input.Date + " 00:00:00"
		endDate :=  input.Date + " 23:59:59"
		queryArr = append(queryArr, ("SELECT * FROM emp_attendence where EMP_PUNCHTM >= '"))
		queryArr = append(queryArr, (stDate + "' AND EMP_PUNCHTM <= '"))
		queryArr = append(queryArr, (endDate + "' AND emp_id IN ("))

		var infoList []Info
		nsize := len(strings.TrimSpace(strings.Join(input.EmpID, "")))
		if nsize <= 0{
			emptyResponse(input.Date, ctx)			
			return
		}
		nsize = len(input.EmpID)
		for i := 0; i< nsize; i++{
			queryArr = append(queryArr, ( "'" + input.EmpID[i] + "'"))
			if i < (nsize-1){queryArr = append(queryArr, ( ","))}
		}
		queryArr = append(queryArr, ( ")"))
		querystr := strings.Join(queryArr, "")
		fmt.Println(querystr)
		result, selErr := dbObj.Query(querystr)
		if selErr != nil {
			ctx.ResponseWriter().WriteHeader(http.StatusNotFound)
			ctx.JSON(context.Map{"error": selErr.Error()})
		}else if result !=nil{			
			var name, dept, empid, punchtm string
			var bRecord bool
			bRecord = false
			for result.Next(){
					selErr = result.Scan(&empid, &name, &dept, &punchtm)
					if selErr == nil{
					info := Info{
						EmpID: empid,
						Name:  name,
						Dept:  dept,
						PunchTm: punchtm,
					}
					infoList = append(infoList, info)
				}
				bRecord = true
			}
			if (!bRecord){
				emptyResponse(input.Date, ctx)			
				return
			}
			resp := Response{
				Date: input.Date,
				Details: infoList,
			}
			ctx.ResponseWriter().WriteHeader(http.StatusOK)
			json.NewEncoder(ctx.ResponseWriter()).Encode(resp)
		}
	})
	fmt.Println("Server Started")
	app.Run(iris.Addr(":8080"), iris.WithoutServerError(iris.ErrServerClosed))
}
func makedbConn() error{
	dbObj, dberr = sql.Open("mysql", dsn)

    // if there is an error opening the connection, handle it
    if dberr != nil {
       return dberr
    }
	dberr = dbObj.Ping()
	if dberr != nil {
        fmt.Println("DB Ping Error: " + dberr.Error())
    }
	return dberr
}
func savedata(query string) {

	// perform a db.Query insert
    insert, err := dbObj.Query(query)

    // if there is an error inserting, handle it
    if err != nil {
        panic(err.Error())
    }
	// be careful deferring Queries if you are using transactions
    defer insert.Close()
}
func emptyResponse(inpdate string, ctx iris.Context){
	var infoList []Info
	info := Info{
		EmpID: "",
		Name:  "",
		Dept:  "",
		PunchTm: "",
	}
	infoList = append(infoList, info)
	resp := Response{
		Date: inpdate,
		Details: infoList,
	}
	ctx.ResponseWriter().WriteHeader(http.StatusOK)
	json.NewEncoder(ctx.ResponseWriter()).Encode(resp)
}