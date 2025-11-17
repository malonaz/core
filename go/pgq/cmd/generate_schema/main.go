package main

import (
	"fmt"

	"github.com/malonaz/pgq/x/schema"
)

func main() {
	out := schema.GenerateCreateTableQuery("job")
	fmt.Println(out)
}
