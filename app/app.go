package app

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	_ "github.com/go-sql-driver/mysql" //import mysql driver
	"github.com/jmoiron/sqlx"
	"github.com/spf13/viper"
)

//AWSProductDetails is used in the application server(billing software)
type AWSProductDetails struct {
	Title        string      `db:"ProductTitle"`
	Desc         string      `db:"ProductDesc" `
	Mrp          float32     `db:"MRP"`
	SellingPrice float32     `db:"SellingPrice" `
	Tag          string      `db:"tag"`
	DateCr       string      `db:"DateCreated"`
	ImgName      interface{} `db:"img_name"`
}

//GCPProductDetails is used for the PHP application (¬_¬)
type GCPProductDetails struct {
	Title        string  `db:"name"`
	Desc         string  `db:"description" `
	Mrp          float32 `db:"mrp"`
	SellingPrice float32 `db:"price" `
	Tag          string  `db:"tag"`
	DateCr       string  `db:"date_created"`
}

type sm struct {
	Success bool
	Message string
}

//Services has contains both db resources
type Services struct {
	AWSProductionDB *sqlx.DB
	GCPWebAppDb     *sqlx.DB
}

var serv *Services

//InitilizeApp engages
func InitilizeApp() {
	viper.AddConfigPath("config")
	if err := viper.ReadInConfig(); err != nil {
		log.Fatal(err)
	}
	log.Println("config loaded")
	AWSSource := fmt.Sprintf("%s:%s@tcp(%s)/%s", viper.GetString("prod_db_user"), viper.GetString("prod_db_pass"), viper.GetString("prod_db_host"), viper.GetString("prod_db_name"))
	AWSDB := sqlx.MustConnect("mysql", AWSSource)
	AWSDB.SetMaxOpenConns(viper.GetInt("web_db_max_open"))
	AWSDB.SetMaxIdleConns(viper.GetInt("web_db_max_idle"))
	log.Println("Opening producion db")
	if err := AWSDB.Ping(); err != nil {
		panic(fmt.Sprintf("(╯‵Д′)╯彡┻━┻ unable to connect to web DB  err: %s", err))
	}
	log.Println("Production db opened")

	GCPDB := sqlx.MustOpen("mysql", fmt.Sprintf("%s:%s@tcp(%s)/%s", viper.GetString("web_db_user"), viper.GetString("web_db_pass"), viper.GetString("web_db_host"), viper.GetString("web_db_name")))
	GCPDB.SetMaxOpenConns(viper.GetInt("web_db_max_open"))
	GCPDB.SetMaxIdleConns(viper.GetInt("web_db_max_idle"))
	if err := GCPDB.Ping(); err != nil {
		panic(fmt.Sprintf("(╯‵Д′)╯彡┻━┻ unable to connect to web DB err: %s", err))
	}
	log.Println("web db opened")

	serv = &Services{
		AWSProductionDB: AWSDB,
		GCPWebAppDb:     GCPDB,
	}
}

//BeginTransfer starts the transfer process
func BeginTransfer(w http.ResponseWriter, r *http.Request) {

	AWSProducts := []AWSProductDetails{}
	if err := serv.AWSProductionDB.Select(&AWSProducts, `SELECT ProductTitle, ProductDesc, MRP, SellingPrice, tag, DateCreated, img_details.img_name 
	FROM product_details
	LEFT JOIN img_details ON product_details.tag = img_details.product_tag AND img_details.img_slot = '1' WHERE Deleted = 0;`); err != nil {
		log.Println(err)
	}

	GCPProducts := []GCPProductDetails{}
	if err := serv.GCPWebAppDb.Select(&GCPProducts, `SELECT name, tag, date_created
	FROM smartshop.product ;`); err != nil {
		log.Println(err)
	}
	log.Println("web products fetched")
	AWSProductsCount := len(AWSProducts)
	GCPProductsCount := len(GCPProducts)
	fmt.Println(AWSProductsCount, GCPProductsCount)
	if AWSProductsCount != GCPProductsCount {
		log.Println("DB out of sync")
		log.Println("Synchronization imminent")
		if AWSProductsCount > GCPProductsCount {
			for i := GCPProductsCount; i <= AWSProductsCount; i++ {
				GCPProducts = append(GCPProducts, GCPProductDetails{
					Tag: "null",
				})
			}
			for i := range AWSProducts {
				mismatchCount := 0
				for j := range GCPProducts {
					if AWSProducts[i].Tag != GCPProducts[j].Tag {
						mismatchCount++
					}
				}
				if mismatchCount != AWSProductsCount {
					if _, err := serv.GCPWebAppDb.Exec(`INSERT INTO smartshop.product
					(id_category, name, description, price, mrp, date_created, thumbnail, tag)
					VALUES(0, ?, ?, ?, ?, ?, ?, ?);
					`, AWSProducts[i].Title, AWSProducts[i].Desc, AWSProducts[i].SellingPrice, AWSProducts[i].Mrp, AWSProducts[i].DateCr, AWSProducts[i].ImgName, AWSProducts[i].Tag); err != nil {
						log.Println("Sync failure", err)
					}
				}
			}
		} else {
			for i := AWSProductsCount; i <= GCPProductsCount; i++ {
				AWSProducts = append(AWSProducts, AWSProductDetails{
					Tag: "null",
				})
			}
			for i := range GCPProducts {
				matchFound := false
				for j := range AWSProducts {
					if GCPProducts[i].Tag == AWSProducts[j].Tag {
						matchFound = true
						break
					}
				}
				if !matchFound {
					if _, err := serv.GCPWebAppDb.Exec(`DELETE FROM product WHERE tag = ?`, GCPProducts[i].Tag); err != nil {
						log.Println("Deletion failed along with sync", err)
					}
				}
			}
		}
	}
	log.Println("Done")
	out, _ := json.Marshal(sm{
		Success: true,
		Message: "DB synced",
	})
	w.Write(out)
}
