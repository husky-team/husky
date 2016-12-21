Husky
=======

[![Build Status](https://travis-ci.org/husky-team/husky.svg?branch=master)](https://travis-ci.org/husky-team/husky)
[![Husky License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](https://github.com/husky-team/husky/blob/master/LICENSE)

[Husky](http://www.husky-project.com/) is a distributed computing system designed to handle mixed jobs of coarse-grained transformations, graph computing and machine learning. The core of Husky is written in C++ so as to leverage the performance of native runtime. For machine learning, Husky supports relaxed consistency level and asynchronous computing in order to exploit higher network/CPU throughput.

For more details about Husky, please check our [Wiki](https://github.com/husky-team/husky/wiki).

For bugs in Husky, please file an issue on [github issue platform](https://github.com/husky-team/husky/issues).

For further discussions, please send email to *support@husky-project.com*.

Dependencies
-------------

Husky has the following minimal dependencies:

* CMake (Version >= 3.0.2)
* ZeroMQ (including both [libzmq](https://github.com/zeromq/libzmq) and [cppzmq](https://github.com/zeromq/cppzmq))
* Boost (Version >= 1.58)
* A working C++ compiler (clang/gcc Version >= 4.9/icc/MSVC)
* TCMalloc (In [gperftools](https://github.com/gperftools/gperftools))
* [GLOG](https://github.com/google/glog) (Latest version, it will be included automatically)

Some optional dependencies:

* libhdfs3 [C/C++ HDFS Client](https://github.com/Pivotal-Data-Attic/pivotalrd-libhdfs3)
* MongoDB C++ Driver (Version [legacy 1.1.2](https://github.com/mongodb/mongo-cxx-driver/tree/legacy))

Build
-----

Download the Husky source code:

    git clone https://github.com/husky-team/husky.git

We assume the root directory of Husky is `$HUSKY_ROOT`. Go to `$HUSKY_ROOT` and do a out-of-source build using CMake:

    cd $HUSKY_ROOT
    mkdir release
    cd release
    cmake -DCMAKE_BUILD_TYPE=Release ..
    make help               # List all build target
    make -j8 Master         # Build the Husky master
    make $ApplicationName   # Build the Husky application

Configuration
-------------

Husky is supposed to run on any platform. Configurations can be stored in a configure file (INI format) or can be the command arguments when running Husky. An example file for configuration is like the following:

    # Required
    master_host=xxx.xxx.xxx.xxx
    master_port=yyyyy
    comm_port=yyyyy

    # Optional
    hdfs_namenode=xxx.xxx.xxx.xxx
    hdfs_namenode_port=yyyyy

    # For Master
    serve=1

    # Session for worker information
    [worker]
    info=master:3


For single-machine environment, use the hostname of the machine as both the master and the (only) worker.

For distributed environment, first copy and modify `$HUSKY_ROOT/exec.sh` according to actual configuration. `exec.sh` depends on `pssh`.

Run a Husky Program
--------------------

Run `./Master --help` for helps. Check the examples in `examples` directory.

First make sure that the master is running. Use the following to start the master

    ./Master --conf /path/to/your/conf

In the single-machine environment, use the following,

    ./<executable> --conf /path/to/your/conf

In the distributed environment, use the following to execute workers on all machines,

    ./exec.sh <executable> --conf /path/to/your/conf

If MPI has been installed in the distributed environment, you may use the following alternatively,

    ./mpiexec.sh <executable> --conf /path/to/your/conf

Run Husky Unit Test
--------------------

Husky provides a set unit tests (based on [gtest 1.7.0](https://github.com/google/googletest)) in `core/`. Run it with:

    make HuskyUnitTest
    ./HuskyUnitTest

Documentation
---------------

Do the following to generate API documentation,

    doxygen doxygen.config

Or use the provided script,

    ./scripts/doxygen.py --gen

Then go to `html/` for HTML documentation, and `latex/` for LaTeX documentation

Start a http server to view the documentation by browser,

    ./scripts/doxygen.py --server

License
---------------

Copyright 2016 Husky Team

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
