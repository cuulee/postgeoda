FROM ubuntu:20.04

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

# Setting PostgreSQL
RUN sed -i 's/\(peer\|md5\)/trust/' /etc/postgresql/12/main/pg_hba.conf && \
    service postgresql start && \
    createuser publicuser --no-createrole --no-createdb --no-superuser -U postgres && \
    createuser tileuser --no-createrole --no-createdb --no-superuser -U postgres && \
    service postgresql stop

# Copy the current directory contents into the container at WORKDIR
COPY . /tmp

# build libgeoda
RUN cd /tmp && rm -rf postgeoda && \
    git clone --recursive https://github.com/geodacenter/postgeoda && \
    cd postgeoda && \
    mkdir build && \
    cd build && \
    cmake -DCMAKE_BUILD_TYPE=Release .. && \
    make && \
    make install

# install postgeoda
RUN service postgresql start && /bin/su postgres -c \
      install_postgeoda.sh && service postgresql stop