# mqtt-base-gateway


This repository contains the gateway source code, json-c-0.11 library, mqtt c library.

Main source code is Gateway.c Json_parser.h

Default will launch 10 gateways with 100 street lights each.

## Build requirements / compilation using CMake

The build process requires the following tools:
```
  * libtool
  * autoconf
  * make
  * build-essential
  * gcc
  * cmake
  * cmake-gui 
  * cmake-curses-gui
```
Please build json-c-0.11 library:

```
cd json-c
sh autogen.sh
./configure
make
```
Install OpenSSL library:

```
apt-get install libssl-dev
```

build Gateway
```
cd ..
make 
```
export path to $HOME/.bash_profile
```
export LD_LIBRARY_PATH=$HOME/mqtt-base-gateway/build/output:$HOME/mqtt-base-gateway/json-c-0.11/.libs:$LD_LIBRARY_PATH
```
```
. $HOME/.bash_profile
```
Run Gateway
```
./build/output/samples/Gateway
```
