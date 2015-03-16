#!/bin/bash

function build() {
    platform=$1
    GOARCH=amd64 CGO_ENABLE=0 GOPATH=`pwd`:$GOPATH \
        go build -o ./bin/hammer.$platform ./hammer.mongo.go
}

echo "build binary for Linux"
GOOS=linux build linux

echo "build binary for MacOS"
GOOS=darwin build macos

# make releaes file
rm ./bin/hammer.tar.gz
cd bin
tar cvf hammer.tar *
gzip hammer.tar

