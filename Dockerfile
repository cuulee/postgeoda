FROM ubuntu:20.10

# Install dev environment
ENV DEBIAN_FRONTEND noninteractive
RUN apt-get update && apt-get install -y -q apt-utils

RUN apt-get install -y -q \
    build-essential \
    autoconf \
    automake \
    cmake \
    git \
    postgresql-12 \
    postgresql-contrib-12 \
    postgresql-server-dev-12

# Copy the current directory contents into the container at WORKDIR
COPY . .

# build libgeoda
RUN cd / && rm -rf postgeoda && \
    git clone --recursive https://github.com/geodacenter/postgeoda && \
    cd postgeoda && \
    mkdir build && \
    cd build && \
    cmake -DCMAKE_BUILD_TYPE=Release .. && \
    make && \
    make install

