# Files
- **build.sh** : script to build binaries
- **hammer.mongo.go** : entry file for the tool

# Usage:

Command line options of ./bin/hammer.macos:
```
-config="": To use config file
-debug=false: debug flag (true|false)
-initdb=false: Drop DB before start testing
-max=false: To find out Max
-monitor=1: Monitor interval
-profile="": to specify a traffic profile, all UPPERCASE
-quiet=false: To silent monitor output
-rps=500: Set Request Per Second, 0 to find Max possible
-run="": To specify run id, used for archive test report
-server="localhost:27017": Define server to be tested, default to localhost:27017
-thread=0: Number of system thread to be used
-total=0: Total request to be sent, default to unlimited (0)
-totaltime=0: To set how long (seconds) to run the test
-warmup=0: To set how long (seconds) for warmup DB
-worker=10: Number of workers, every worker will have two connections to mongodb
```

Some examples:
```sh
./hammer.linux -monitor 1 -server ec2-107-21-153-123.compute-1.amazonaws.com:27017 -thread 4 -max -initdb=false -max=true -profile=insert -worker 32 -total=100000
./hammer.linux -monitor 1 -server ec2-107-21-153-123.compute-1.amazonaws.com:27017 -thread 4 -max -initdb=false -max=true -profile=singleQuery -worker 32 -total=200000
./hammer.linux -monitor 1 -server ec2-107-21-153-123.compute-1.amazonaws.com:27017 -thread 4 -max -initdb=false -max=true -profile=inplaceupdate -worker 32 -total=100000
./hammer.linux -monitor 1 -server ec2-107-21-153-123.compute-1.amazonaws.com:27017 -thread 4 -max -initdb=false -max=true -profile=extendedupdate -worker 32 -total=100000
./hammer.linux -monitor 1 -server ec2-107-21-153-123.compute-1.amazonaws.com:27017 -thread 4 -max -initdb=false -max=true -profile=insertsmall -worker 8 -total=95000
```


# Quick Install Using Released Binaries:

If you want use `hammer.mongo` as-is, the easiest way is to use the pre-built binaries. To get the binaries, run:

```sh
wget --no-check-certificate https://raw.githubusercontent.com/rzh/hammer.mongo/master/scripts/bootstrap.sh -O - | bash
```

It will download the binaries from `https://github.com/rzh/hammer.mongo/releases/latest`, and necessary scripts. No need to install Go. Linux/64 and Darwin/64 are support at this moment. Simply run the script to run the workload

You can then run pre-written shell scripts, such as:

```sh
./simple_insert.sh

```


# Build or Run from Source:

If you want to make modifications to hammer, it's best to build and run it from source. To do so:

- Make sure `Go` is installed and set up properly. For Mac OS, the easiest way is to use homebrew with `--cross-compile-common`. This will allow you to build the linux binaries as well:
```sh
  brew install go --cross-compile-common
```
For more detail, please refer to http://golang.org/doc/install. If you did not install go with homebrew, please refer to this article for cross platform builds. http://dave.cheney.net/2012/09/08/an-introduction-to-cross-compilation-with-go.

- set your `GOPATH` to a directory containing all your Go code, similar to a Eclipse workspace. You can put this in your ~/.profile file for a more convenient reuse.
```
mkdir $HOME/go
export GOPATH=$HOME/go
```
More information is available here: `https://golang.org/doc/code.html#GOPATH`

- Use `go get` to install hammer.mongo, this will clone this repository and its dependencies into `$GOPATH/src/github.com/$GITHUB_USERNAME/hammer.mongo`
```sh
  go get github.com/rzh/hammer.mongo
```
It will also install hammer.mongo into your `$GOPATH/bin`, if your `PATH` is  configured to include this directory, you can simply call `hammer.mongo` to run this.

To build again after making changes to the code, use `build.sh`, which will build hammer.mongo for Mac OS and Linux 64. The binaries will be placed in `bin`
```sh
$ ls -1 ./bin
hammer.linux
hammer.macos
```
To run from source directly after making changes, run `go run hammer.mongo.go` with options. This will automatically build before running. For example
```sh
go run hammer.mongo.go -monitor 1 -max -worker 9 -server localhost:27017 -max -initdb=true -profile=insert
```

###instructions for using SSL
to hammer against an SSL enabled server, set the `ssl, sslCAFile, sslPEMKeyFile` options as you would any mongo shell. There is also an included server certificate for testing in the `certs` directory. You can set up an SSL enabled mongod with  `mongod --sslMode requireSSL --sslPEMKeyFile certs/key.pem`. To run hammer against this test server, only the `-ssl=true` flag needs to be specified, and it will default to using the provided PEM files.
