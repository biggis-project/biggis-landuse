FROM biggis/base:java8-jre-alpine

MAINTAINER aklink

# The Scala 2.11 build is currently recommended by the project.
ARG SCALA_VERSION=2.11
ARG SPARK_VERSION=2.1.0
ARG GEOTRELLIS_VERSION=1.1.0-RC4
ARG KAFKA_VERSION=0.10.1.1
ARG SNAPSHOT_VERSION=0.0.2
ARG MAVEN_VERSION=3.3.9

ARG BUILD_DATE
ARG VCS_REF

LABEL eu.biggis-project.build-date=$BUILD_DATE \
      eu.biggis-project.license="MIT" \
      eu.biggis-project.name="BigGIS" \
      eu.biggis-project.url="http://biggis-project.eu/" \
      eu.biggis-project.vcs-ref=$VCS_REF \
      eu.biggis-project.vcs-type="Git" \
      eu.biggis-project.vcs-url="https://github.com/biggis-project/biggis-landuse" \
      eu.biggis-project.environment="dev" \
      eu.biggis-project.version=$SNAPSHOT_VERSION

ENV SNAPSHOT_ARCHIVE biggis-landuse-${SNAPSHOT_VERSION}.jar

WORKDIR ../

RUN \
	set -x && \
    apk --update add --virtual build-dependencies git && \
	git clone "https://github.com/biggis-project/biggis-landuse" && \
	wget -q "http://apache.ip-connect.vn.ua/maven/maven-3/$MAVEN_VERSION/binaries/apache-maven-$MAVEN_VERSION-bin.tar.gz" && tar -xzf "apache-maven-$MAVEN_VERSION-bin.tar.gz" && \
	cd "/biggis-landuse" && \
	"/apache-maven-$MAVEN_VERSION/bin/mvn" package && \
	apk del build-dependencies && \
    rm -rf /var/cache/apk/* && \
	ls && \
	echo "Successfull Build"
	
# COPY ../target/${SNAPSHOT_ARCHIVE} .

RUN ls