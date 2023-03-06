package main

import (
	"context"
	"fmt"
	"github.com/kellegous/go/backend"
	"github.com/kellegous/go/backend/firestore"
	"github.com/kellegous/go/backend/leveldb"
	"github.com/kellegous/go/backend/mysql"
	"github.com/kellegous/go/web"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"log"
	"strings"
)

func main() {
	//db, err := leveldb.New("data")
	//if err != nil {
	//	log.Panic(err)
	//}
	//db, err := mysql.New("root:123@tcp(127.0.0.1:3306)/shortener?charset=utf8mb4&parseTime=True&loc=Local")
	//if err != nil {
	//	fmt.Errorf("mysql error")
	//	return
	//}
	//err = db.Del(nil, "truong")
	//if err != nil {
	//	fmt.Println(err)
	//}
	//	db.GetAll(nil)
	//rt := internal.Route{
	//	URL:  "www.facebook.com/gio.lanh.90813237",
	//	Time: time.Now(),
	//}
	//db.Put(nil, "truong", &rt)

	//e, err := db.Get(nil, "truong")
	////	route, err := db.Get(nil, "4")
	//if err != nil {
	//	fmt.Println(err.Error())
	//	return
	//}
	//
	//fmt.Println(e)
	//fmt.Println(route)

	pflag.String("addr", ":8067", "default bind address")
	pflag.Bool("admin", false, "allow admin-level requests")
	pflag.String("version", "", "version string")
	pflag.String("backend", "mysql", "backing store to use. 'leveldb' and 'firestore' or 'mysql' currently supported.")
	pflag.String("data", "data", "The location of the leveldb data directory")
	pflag.String("project", "", "The GCP project to use for the firestore backend. Will attempt to use application default creds if not defined.")
	pflag.String("host", "", "The host field to use when gnerating the source URL of a link. Defaults to the Host header of the generate request")
	pflag.Parse()

	if err := viper.BindPFlags(pflag.CommandLine); err != nil {
		log.Panic(err)
	}

	// allow env vars to set pflags
	viper.AutomaticEnv()
	viper.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))

	var backend backend.Backend

	switch viper.GetString("backend") {
	case "leveldb":
		log.Default().Println("Using leveldb")
		var err error
		backend, err = leveldb.New(viper.GetString("data"))
		if err != nil {
			log.Panic(err)
		}
	case "firestore":
		log.Default().Println("Using firestore")
		var err error

		backend, err = firestore.New(context.Background(), viper.GetString("project"))
		if err != nil {
			log.Panic(err)
		}
	case "mysql":
		log.Default().Println("Using mysql")
		var err error
		backend, err = mysql.New("root:123@tcp(127.0.0.1:3306)/shortener?charset=utf8mb4&parseTime=True&loc=Local")
		if err != nil {
			fmt.Errorf("mysql error")
			return
		}
	default:
		log.Panic(fmt.Sprintf("unknown backend %s", viper.GetString("backend")))
	}

	defer backend.Close()

	log.Panic(web.ListenAndServe(backend))
}
