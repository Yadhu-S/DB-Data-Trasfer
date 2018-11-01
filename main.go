package main

import (
	"fmt"
	"net/http"
	"transfer_server/app"

	"goji.io/pat"

	"goji.io"
)

func main() {
	router := goji.NewMux()
	router.HandleFunc(pat.Get("/insert/products"), app.BeginTransfer)
	fmt.Println("Transfer Server online..")
	http.ListenAndServe(":9001", router)
}
