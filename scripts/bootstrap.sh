#!/usr/bin/bash

VERSION="v0.1-latest"
SCRIPTS=(bulk_insert.bash simple_insert.sh config.sh contended_update.sh single_query.sh capped_insert.sh capped_insert_read.sh evergreen_insert.sh)
echo "bootstrap hammer.time"

if [ -f hammer.tar.gz ]; then
    rm hammer.tar.gz
fi

if [ ! -d bin ]; then
    mkdir bin
fi

if hash wget 2>/dev/null; then
    wget https://github.com/rzh/hammer.mongo/releases/download/$VERSION/hammer.tar.gz
elif hash curl 2>/dev/null; then
    curl -O https://github.com/rzh/hammer.mongo/releases/download/$VERSION/hammer.tar.gz
else
    echo "Please install wget or curl"
fi

# install binaries
tar zxvf hammer.tar.gz
mv hammer.linux bin
mv hammer.macos bin
rm hammer.tar.gz

# get server and CA certificates for SSL
mkdir certs
wget https://raw.githubusercontent.com/rzh/hammer.mongo/master/certs/ca.pem -O certs/ca.pem
wget https://raw.githubusercontent.com/rzh/hammer.mongo/master/certs/key.pem -O certs/key.pem
chmod 400 certs/ca.pem
chmod 400 certs/key.pem

# download scripts
for f in "${SCRIPTS[@]}"
do
    if [ ! -f $f ]; then
        wget https://raw.githubusercontent.com/rzh/hammer.mongo/master/scripts/$f
        chmod +x $f
    fi
    # skip file if the file already exist
done
