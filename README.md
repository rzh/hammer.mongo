# Files
- **./bin** : executables for Linux (works for Linux/AMD64, including AWS) and MacOS. 
   Yes, I do check in binary now to save your setup time. Please check later section
   on how to properly setup Go environment
   - **Please checkout release branches for binaries, master branch is not for test run, will NOT have binaries**
   - **hammer.linux** : Linux binary for amd64, this good for Linux server and AWS 64bit instance
   - **hammer.macos** : Binary for MacOS, use it from your MBP
   - **reporter.linux** : csv file reporter for linux
   - **reporter.macos** : csv file reporter for MacOS
- **build.sh** : script to build binaries
- **hammer.mongo.go** : main file for the tool

# How to use:
- Command line options:
   <pre>
Usage of ./bin/hammer.macos:
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
</pre>

a sample run:
<pre>
GOPATH=`pwd`:$GOPATH go run hammer.mongo.go -monitor 1 -max -worker 9 -server localhost:27017 -max -initdb=true -profile=insert
</pre>

use the binary 
<pre>
./hammer.linux -monitor 1 -server ec2-107-21-153-123.compute-1.amazonaws.com:27017 -thread 4 -max -initdb=false -max=true -profile=insert -worker 32 -total=100000
./hammer.linux -monitor 1 -server ec2-107-21-153-123.compute-1.amazonaws.com:27017 -thread 4 -max -initdb=false -max=true -profile=singleQuery -worker 32 -total=200000
./hammer.linux -monitor 1 -server ec2-107-21-153-123.compute-1.amazonaws.com:27017 -thread 4 -max -initdb=false -max=true -profile=inplaceupdate -worker 32 -total=100000
./hammer.linux -monitor 1 -server ec2-107-21-153-123.compute-1.amazonaws.com:27017 -thread 4 -max -initdb=false -max=true -profile=extendedupdate -worker 32 -total=100000
./hammer.linux -monitor 1 -server ec2-107-21-153-123.compute-1.amazonaws.com:27017 -thread 4 -max -initdb=false -max=true -profile=insertsmall -worker 8 -total=95000
</pre>

to use the binary together with the reporter:
<pre>
./bin/hammer.linux -monitor 1 -worker 18 -server localhost:27017 -rps=1000 -initdb=true -profile=QA408; cat perf_test_data.csv| ./bin/reporter.linux
</pre>

Note:
- To build the tool, please make sure your Go is installed and setup properly. Please refer to this doc http://golang.org/doc/install. For Mac OS, the easier way it to use homebrew to install with --cross-compile-common option:
<pre>
  brew install go --cross-compile-common
</pre>
- For cross platform build, make sure you compile Go runtime properly, please refer to this blog for details: http://dave.cheney.net/2012/09/08/an-introduction-to-cross-compilation-with-go. You will not need this if you install with homebrew according to the above step.
- You will need install proper Go package, run following command after your GOPATH is properly set
<pre>
  GOPATH=`pwd`:$GOPATH go get -d
</pre>
- Wiki page https://wiki.mongodb.com/display/10GEN/Design+Considerations+for+Hammer+Performance+Client
