# First stage is the build environment
FROM sgrio/java-oracle:jdk_8 as builder
MAINTAINER Jordan Halterman <jordan@opennetworking.org>

# Set the environment variables
ENV HOME /root
ENV JAVA_TOOL_OPTIONS=-Dfile.encoding=UTF8

# Get arguments
ARG REPO=https://github.com/atomix/atomix.git
ARG BRANCH=master
ARG PROFILER=2018.04-b86

# Change to the build directory
WORKDIR /src/atomix
RUN apt-get update && apt-get install -y zip maven git bzip2 build-essential iptables stress \
    && git clone --branch $BRANCH $REPO /src/atomix \
    && cd /src/atomix \
    && ln -s /usr/lib/jvm/java-8-oracle/bin/jar /etc/alternatives/jar \
    && ln -s /etc/alternatives/jar /usr/bin/jar \
    && mvn package -DskipTests -Ddockerfile.skip \
    && mkdir -p /src/tar \
    && cd /src/tar \
    && tar -xf /src/atomix/dist/target/atomix.tar.gz \
    && rm -rf .git
RUN mkdir -p /src/yourkit
RUN cd /src/yourkit
RUN curl -o YourKit-JavaProfiler-$PROFILER.zip https://www.yourkit.com/download/YourKit-JavaProfiler-$PROFILER.zip
RUN unzip YourKit-JavaProfiler-$PROFILER.zip
RUN mv YourKit-JavaProfiler-$(echo $PROFILER | sed 's/\(.*\)-.*/\1/')/bin/linux-x86-64/libyjpagent.so /src/tar/lib/libyjpagent.so

# Second stage is the runtime environment
FROM anapsix/alpine-java:8_server-jre

ENV ATOMIX_DATA_DIR=/var/lib/atomix/data
ENV ATOMIX_LOG_DIR=/var/log/atomix

# Change to /root directory
RUN apk update \
    && mkdir -p /root/atomix
WORKDIR /root/atomix

# Install Atomix
COPY --from=builder /src/tar/ .

# Ports
# 5678 - Atomix REST API
# 5679 - Atomix intra-cluster communication
EXPOSE 5678 5679

RUN set -x \
  && mkdir -p $ATOMIX_DATA_DIR $ATOMIX_LOG_DIR

ENTRYPOINT ["./bin/atomix-agent"]
