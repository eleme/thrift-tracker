## [thriftpy](https://github.com/eleme/thriftpy/tree/develop/thriftpy/contrib/tracking)-like tracker for golang

Only support request header currently, response header is not so useful as request header.

Unlike example/, always client/processor factory to avoid state race.

### Requirements

A modified version of thrift compiler: https://github.com/damnever/thrift

```Bash
$ git clone git@github.com:damnever/thrift.git
$ git checkout tracker
$ ./bootstrap.sh
$ ./configure --prefix=/usr/local/ --with-boost=/usr/local --with-libevent=/usr/local --without-haskell --without-java --without-php --without-nodejs --without-python --without-cpp --without-lua
$ make
$ sudo make install  # Or, sudo cp compiler/cpp/thrift /usr/local/bin/tracker-thrift
$ # You can now use thrift compiler to generate go code, see example/
```
