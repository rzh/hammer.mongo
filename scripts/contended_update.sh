#!/bin/bash

# number of workers
export HT_CMD_WORKERS=16

# report stats every N second
export HT_CMD_MONITOR_INTERVAL=1

# how long shall the run in sercond, 0 means infinity
export HT_CMD_TOTAL_TIME=120

# how long shall the run in number of OP, 0 means infinity
export HT_CMD_TOTAL_OPS=0

# whether to use legacy Op, value shall be 0: use writeCmd or 1: use legacy op
export HT_USE_LEGACY_WRITE=0

# server URL
export HT_SERVER_URL="localhost:27017"

source config.sh

$BINARY -profile=CONTENDED_UPDATE -max -worker $HT_CMD_WORKERS -server $HT_SERVER_URL -monitor $HT_CMD_MONITOR_INTERVAL -total $HT_CMD_TOTAL_OPS -totaltime $HT_CMD_TOTAL_TIME

