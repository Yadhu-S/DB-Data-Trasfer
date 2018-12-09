package main

import (
	"fmt"
	"log"
	"net/http"
	"transfer_server/app"

	"goji.io/pat"

	"goji.io"
)

func main() {
	app.InitilizeApp()
	router := goji.NewMux()
	router.HandleFunc(pat.Get("/sync/products"), app.SyncProducts)
	router.HandleFunc(pat.Get("/sync/shop"), app.SyncShopDetails)
	fmt.Println("Transfer Server online..")
	if err := http.ListenAndServe(":9001", router); err != nil {
		log.Fatal("Transfer Server failed to start", err)
	}
}
