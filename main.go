package main

import (
	"fmt"

	"github.com/connnorchen/MyDb/internal/kvstore"
)

const PATH string = "/Users/connor/proj/build-your-own-db/db"

func main() {
    db := kvstore.KV{Path: PATH}
    if err := db.Open(); err != nil {
        fmt.Printf("err in open: %s\n", err.Error())
        goto end
    }
    for {
        var op string
        fmt.Println("what op?")
        fmt.Scanln(&op)
        switch (op) {
        case "set":
            set(&db)
            break
        case "get":
            get(&db)
            break
        case "del":
            del(&db)
            break
        }
    }
    
end:
    db.Close()
    fmt.Printf("error occured\n")
    return
}

func set(db *kvstore.KV) {
    fmt.Println("key: ")
    var key string
    fmt.Scanln(&key)

    fmt.Println("val: ")
    var val string
    fmt.Scanln(&val)
    if err := db.Set([]byte(key), []byte(val)); err != nil {
        fmt.Printf("error in set, %s", err.Error())
    }
}

func get(db *kvstore.KV) {
    fmt.Println("key: ")
    var key string
    fmt.Scanln(&key)
    val, found := db.Get([]byte(key))
    if !found {
        fmt.Println("not found such key")
    } else {
        fmt.Printf("val is %s\n", val)
    }
}

func del(db *kvstore.KV) {
    fmt.Println("key: ")
    var key string
    fmt.Scanln(&key)
    deleted, err := db.Del([]byte(key))
    if err != nil {
        fmt.Printf("error in del, %s", err.Error())
    }
    fmt.Printf("del %t\n", deleted)
}
