
# config runtime environment for hammer.mongo

# check hammer binary
PLATFORM='unknown'
unamestr=`uname`
if [[ "$unamestr" == 'Linux' ]]; then
    PLATFORM='linux'
elif [[ "$unamestr" == 'Darwin' ]]; then
    PLATFORM='macos'
fi

BINARY="go run ../hammer.mongo.go"
if [ -x "./bin/hammer.$PLATFORM" ]; then
    echo "Found executable binary, using ./bin/hammer.$PLATFORM"
    BINARY="./bin/hammer.$PLATFORM" 
fi

