package flags

import (
	"fmt"
	"os"
	"time"
)

var (
	BaseCollectionPath string
	SinceTime          time.Time
)

func LogCollectionArgs() (arg string) {
	if v, has := os.LookupEnv("MUST_GATHER_SINCE"); has {
		arg = fmt.Sprintf("--since=\"%v\"", v)
	}

	if v, has := os.LookupEnv("MUST_GATHER_SINCE_TIME"); has {
		arg = fmt.Sprintf("--since-time=\"%v\"", v)
	}

	return
}
