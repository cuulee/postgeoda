FROM ubuntu:18.04

# Install dev environment
RUN apt-get update
RUN apt-get upgrade -y

RUN apt-get install -y curl wget git libcurl4-gnutls-dev
RUN apt-get install -y build-essential 
RUN apt-get install -y cmake libc6-dev

WORKDIR /usr/src/libgeoda

# Copy the current directory contents into the container at WORKDIR
COPY . .

# build libgeoda
RUN \
    git submodule update --init --recursive && \
    rm -rf build && \
    mkdir build && \
    cd build
    cmake .. && \
    make

# build rgeoda

# build pygeoda

# launch jupyter server for rgeoda and pygeoda



