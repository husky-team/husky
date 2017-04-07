FROM ubuntu:14.04
ENV http_proxy http://proxy.cse.cuhk.edu.hk:8000
ENV https_proxy https://proxy.cse.cuhk.edu.hk:8000
RUN apt-get -y update
RUN apt-get install -y software-properties-common
RUN add-apt-repository -y ppa:ubuntu-toolchain-r/test
RUN add-apt-repository -y ppa:kojoley/boost
RUN add-apt-repository -y ppa:george-edison55/cmake-3.x
RUN apt-get -y update
RUN apt-get install -y apt-transport-https build-essential \
    gcc-4.9 g++-4.9 cmake git
RUN update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-4.9 50
RUN update-alternatives --install /usr/bin/g++ g++ /usr/bin/g++-4.9 50
RUN apt-get install -y libboost-chrono1.58-dev libboost-program-options1.58-dev \
    libboost-thread1.58-dev libboost-filesystem1.58-dev libboost-regex1.58-dev
RUN apt-get install -y libgoogle-perftools-dev libzmq3-dev libprotobuf8
RUN echo "deb https://dl.bintray.com/wangzw/deb trusty contrib" | tee /etc/apt/sources.list.d/bintray-wangzw-deb.list
RUN apt-get -y update
RUN apt-get install -y --force-yes libhdfs3 libhdfs3-dev

RUN mkdir temp && cd temp && git clone https://github.com/zeromq/cppzmq \
    && cd cppzmq && git reset --hard 4648ebc9643119cff2a433dff4609f1a5cb640ec \
    && cp zmq.hpp /usr/local/include
RUN rm -rf temp
WORKDIR /husky-source
RUN git clone https://github.com/husky-team/husky.git
RUN cd husky && mkdir release && cd release && cmake -DCMAKE_BUILD_TYPE=release ..
RUN cd husky/release && make -j4 Master PI WordCountMR
ENTRYPOINT ["husky/release/PI","--master_host","localhost","--master_port","10086","--comm_port","10010","--worker.info","localhost:4"]
