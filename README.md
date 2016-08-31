Husky
=======

[Husky](http://www.husky-project.com/) is a distributed computing system designed to handle mixed jobs of coarse-grained transformations, graph computing and machine learning. The core of Husky is written in C++ so as to leverage the performance of native runtime. For machine learning, Husky supports relaxed consistency level and asynchronous computing in order to exploit higher network/CPU throughput.


Dependencies
-------------

Husky has the following minimal dependencies:

* CMake
* ZeroMQ (libzmq and cppzmq)
* Boost
* A working C++ compiler (clang/gcc/icc/MSVC)
* TCMalloc

Some optional dependencies:

* libhdfs3
* MongoDB C++ Driver

Build
-----

Get the latest Husky tarball and unpack it. We assume the root directory of Husky is $HUSKY\_ROOT. Change directory to $HUSKY\_ROOT and do a out-of-source build using CMake:

    mkdir release
    cd release
    cmake -DCMAKE_BUILD_TYPE=Release ..
    make help           # List all build target
    make -j8 Master    # Build the Husky master
    make $ApplicationName # Build the Husky application

Config
-------------

Husky is supposed to run on any platform. Configurations can be stored in a configure file(INI format) or can be the command arguments when running Husky. An example file for configuration is like the following:

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

For distributed environment, first copy and modify `$HUSKY_ROOT/exec.sh` according to actual configuration.

Run a Husky Program
--------------------

Run `./Master --help` for helps. Check the examples in `examples` directory.

First make sure that the master is running. Use the following to start the master

    ./Master --conf /path/to/your/conf

In the distributed environment, use the following to execute workers on all machines,

    ./exec.sh <executable> /path/to/your/conf

In the single-machine environment, use the following,

    ./<executable> --conf /path/to/your/conf

Documentation
---------------

Do the following to generate API documentation,

    doxygen doxygen.config

Then go to html/ for HTML documentation, and latex/ for LaTeX documentation


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
