package profiles

import (
	"log"
	"os"
	"strconv"
)

// get os.Env(s), must be larger and equal than low
func getOSEnvFlagInt(v string, low int) int {
	s := os.Getenv(v)

	if s == "" {
		return low
	} else {
		i, err := strconv.Atoi(s)
		panicOnError(err)

		if i < low {
			log.Panicln("Got ", v, " $le 0, valure read ==>", i)
		}

		return i
	}
}
