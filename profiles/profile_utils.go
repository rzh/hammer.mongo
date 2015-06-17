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
			log.Panicln("Got ", v, " less than minimal allowed value ", low, ", failed to read env variable ", i)
		}

		return i
	}
}
