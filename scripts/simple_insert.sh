#!/bin/bash

# how many DB to be insert into
export HT_MULTI_DB=1

# how many collections per DB
export HT_MULTI_COLLECTION=16

# number of workers
export HT_CMD_WORKERS=16

# report stats every N second
export HT_CMD_MONITOR_INTERVAL=10

# how long shall the run in sercond, 0 means infinity
export HT_CMD_TOTAL_TIME=40

# how long shall the run in number of OP, 0 means infinity
export HT_CMD_TOTAL_OPS=0

# server URL
export HT_SERVER_URL="localhost:27017"

go run ../hammer.mongo.go -profile=INSERT -max -worker $HT_CMD_WORKERS -server $HT_SERVER_URL -monitor $HT_CMD_MONITOR_INTERVAL -total $HT_CMD_TOTAL_OPS -totaltime $HT_CMD_TOTAL_TIME

