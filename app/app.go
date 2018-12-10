package app

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"

	_ "github.com/go-sql-driver/mysql" //import mysql driver
	"github.com/jmoiron/sqlx"
	"github.com/spf13/viper"
)

//ShopDetails contains details of shop wihout the inclusion of password
type ShopDetails struct {
	ShopID    string  `json:"shop-id" db:"shop_id" valid:"uuidv4"`
	Name      string  `json:"shop-name" db:"shop_name" valid:"required~Shop Name cannot be left blank"`
	GSTIN     string  `json:"gstin" db:"gstin" valid:"alphanum,required~GSTIN cannot be left blank"`
	CNo       string  `json:"contact-no" db:"contact_no" valid:"required~Contact no cannot be left blank"`
	Email     string  `json:"email" db:"email"`
	Longitude float64 `json:"longitude" db:"longitude"`
	Latitude  float64 `json:"latitude" db:"latitude"`
	BldName   string  `json:"building-name" db:"building_name"`
	Place     string  `json:"place" db:"place" valid:"required~Place cannot be left blank"`
	District  string  `json:"district" db:"district" valid:"required~District cannot be left blank"`
	City      string  `json:"city" db:"city" valid:"required~City cannot be left blank"`
	Lndmrk    string  `json:"landmark" db:"landmark" valid:"-"`
	State     string  `json:"state" db:"state" valid:"required~State cannot be left blank"`
	Postcode  string  `json:"postcode" db:"postcode" valid:"required~Postcode cannot be left blank"`
	Country   string  `json:"country" db:"country" valid:"required~Country cannot be left blank"`
	Desc      string  `json:"description" db:"description"`
	OT        string  `json:"opening-time" db:"open_time" valid:"required~Opening time cannot be left blank"`
	CT        string  `json:"closing-time" db:"close_time" valid:"required~Closing time cannot be left blank"`
	Cards     bool    `json:"accept-cards" db:"accept_cards"`
	Park      string  `json:"parking" db:"Parking" valid:"-"`
	ShopState string  `json:"-" db:"shop_state" valid:"-"`
	ShopCat   string  `json:"category" db:"shop_cat" valid:"required~Shop category cannot be left blank"`
	StateCode int     `json:"state-code" db:"state_code" valid:"-"`
}

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
	Tag string `db:"tag"`
}

type sm struct {
	Success bool
	Message string
}

type shop struct {
	ShopID string `db:"shop_id"`
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

//SyncProducts starts the product sync from AWS to GCP
func SyncProducts(w http.ResponseWriter, r *http.Request) {
	AWSProducts := []AWSProductDetails{}
	if err := serv.AWSProductionDB.Select(&AWSProducts, `SELECT ProductTitle, ProductDesc, MRP, SellingPrice, tag, DateCreated, img_details.img_name 
	FROM product_details
	LEFT JOIN img_details ON product_details.tag = img_details.product_tag AND img_details.img_slot = '1' WHERE Deleted = 0 ORDER BY tag ASC`); err != nil { //join image details table and select non-deleted products form AWS database
		log.Println(err)
	}
	GCPProducts := []GCPProductDetails{}
	if err := serv.GCPWebAppDb.Select(&GCPProducts, `SELECT tag
<<<<<<< HEAD
	FROM smartshop.product ;`); err != nil {
=======
	FROM smartshop.product ORDER BY tag ASC;`); err != nil {
>>>>>>> master
		log.Println(err)
	}
	matchFound := false
	temp := GCPProducts
	GCPCount := len(GCPProducts)
	for i := range AWSProducts {
		matchFound = false
		for j := 0; j < GCPCount; j++ {
			if AWSProducts[i].Tag == GCPProducts[j].Tag {
				matchFound = true
				GCPProducts = removeGCP(GCPProducts, j)
				GCPCount--
				break
			}
		}
		if !matchFound {
			if _, err := serv.GCPWebAppDb.Exec(`INSERT INTO smartshop.product
					(id_category, name, description, price, mrp, date_created, thumbnail, tag)
					VALUES(0, ?, ?, ?, ?, ?, ?, ?);
					`, AWSProducts[i].Title, AWSProducts[i].Desc, AWSProducts[i].SellingPrice, AWSProducts[i].Mrp, AWSProducts[i].DateCr, AWSProducts[i].ImgName, AWSProducts[i].Tag); err != nil {
				log.Println("Insert failure", err)
			}
		}
	}

	GCPProducts = temp
	AWSCount := len(AWSProducts)
	for i := range GCPProducts {
		matchFound = false
		for j := 0; j < AWSCount; j++ {
			if GCPProducts[i].Tag == AWSProducts[j].Tag {
				matchFound = true
				AWSProducts = removeAWS(AWSProducts, j)
				AWSCount--
				break
			}
		}
		if !matchFound {
			if _, err := serv.GCPWebAppDb.Exec(`DELETE FROM product WHERE tag = ?`, GCPProducts[i].Tag); err != nil {
				log.Println("Deletion failed along with sync", err)
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

<<<<<<< HEAD
//SyncShopDetails does the same but for shop details
func SyncShopDetails(w http.ResponseWriter, r *http.Request) {
	AWSShops := []ShopDetails{}
	if err := serv.AWSProductionDB.Select(&AWSShops, `SELECT shop_id, shop_name, gstin, contact_no, email, longitude, latitude, building_name, place, district, city, landmark, state, postcode, country, description, open_time, close_time, accept_cards, Parking, shop_state, shop_cat, state_code
	FROM Find_DB.shop_details ;
	`); err != nil { //join image details table and select non-deleted products form AWS database
		log.Println(err)
	}
	GCPShops := []shop{}
	if err := serv.GCPWebAppDb.Select(&GCPShops, `SELECT shop_id
	FROM smartshop.shop_details ;`); err != nil {
		log.Println(err)
	}
	GCPCount := len(GCPShops)
	startAt := 0
	for i := range AWSShops {
		matchFound := false
		for j := startAt; j < GCPCount; j++ {
			if AWSShops[i].ShopID == GCPShops[j].ShopID {
				matchFound = true
				GCPShops[0], GCPShops[j] = GCPShops[j], GCPShops[0]
				startAt++
				break
			}
		}
		if !matchFound {
			params := []interface{}{
				AWSShops[i].ShopID,
				AWSShops[i].Name,
				AWSShops[i].GSTIN,
				AWSShops[i].CNo,
				AWSShops[i].Email,
				AWSShops[i].Longitude,
				AWSShops[i].Latitude,
				AWSShops[i].BldName,
				AWSShops[i].Place,
				AWSShops[i].District,
				AWSShops[i].City,
				AWSShops[i].Lndmrk,
				AWSShops[i].State,
				AWSShops[i].Postcode,
				AWSShops[i].Country,
				AWSShops[i].Desc,
				AWSShops[i].OT,
				AWSShops[i].CT,
				AWSShops[i].Cards,
				AWSShops[i].Park,
				AWSShops[i].ShopState,
				AWSShops[i].ShopCat,
				AWSShops[i].StateCode,
			}
			if _, err := serv.GCPWebAppDb.Exec(`INSERT INTO smartshop.shop_details 
			(shop_id, shop_name, gstin, contact_no, email, longitude, latitude, building_name, place, district, city, landmark, state, postcode, country, description, open_time, close_time, accept_cards, Parking, shop_state, shop_cat, state_code)
			VALUES(`+strings.Repeat(",?", len(params))[1:]+`);`, params...); err != nil {
				log.Println("Insert failure", err)
			}
		}
	}
	AWSCount := len(AWSShops)
	startAt = 0
	for i := range GCPShops {
		matchFound := false
		for j := startAt; j < AWSCount; j++ {
			if GCPShops[i].ShopID == AWSShops[j].ShopID {
				matchFound = true
				AWSShops[0], AWSShops[j] = AWSShops[j], AWSShops[0]
				startAt++
				break
			}
		}
		if !matchFound {
			if _, err := serv.GCPWebAppDb.Exec(`DELETE FROM shop_details WHERE shop_id = ?`, GCPShops[i].ShopID); err != nil {
				log.Println("Deletion failed along with sync", err)
			}
		}
	}

	log.Println("Done")
	out, _ := json.Marshal(sm{
		Success: true,
		Message: "Shop details synced",
	})
	w.Write(out)
}
func swap(s []struct{}, i int) []struct{} {
	s[0], s[i] = s[i], s[0]
	return s
=======
func removeGCP(s []GCPProductDetails, i int) []GCPProductDetails {
	s[len(s)-1], s[i] = s[i], s[len(s)-1]
	return s[:len(s)-1]
}

func removeAWS(s []AWSProductDetails, i int) []AWSProductDetails {
	s[len(s)-1], s[i] = s[i], s[len(s)-1]
	return s[:len(s)-1]
>>>>>>> master
}
