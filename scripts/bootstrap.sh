#!env bash

VERSION="v0.1.2"
SCRIPTS=(bulk_insert.bash simple_insert.sh config.sh)
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

# download scripts
for f in "${SCRIPTS[@]}"
do
    if [ -f $f ]; then
        rm $f
    fi

    wget https://raw.githubusercontent.com/rzh/hammer.mongo/master/scripts/$f
    chmod +x $f
done
