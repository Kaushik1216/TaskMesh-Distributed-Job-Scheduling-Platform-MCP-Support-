#!/bin/sh
# Maven Wrapper Script
# Download Maven if not already cached and run the build

MAVEN_VERSION="3.9.8"
MAVEN_DIR="$HOME/.m2/wrapper/dists/apache-maven-${MAVEN_VERSION}"
MAVEN_ZIP="$MAVEN_DIR/apache-maven-${MAVEN_VERSION}-bin.zip"
MAVEN_HOME="$MAVEN_DIR/apache-maven-${MAVEN_VERSION}"

if [ ! -d "$MAVEN_HOME" ]; then
    echo "Downloading Apache Maven $MAVEN_VERSION..."
    mkdir -p "$MAVEN_DIR"
    curl -L "https://downloads.apache.org/maven/maven-3/${MAVEN_VERSION}/binaries/apache-maven-${MAVEN_VERSION}-bin.zip" -o "$MAVEN_ZIP"
    unzip -q "$MAVEN_ZIP" -d "$MAVEN_DIR"
    rm "$MAVEN_ZIP"
fi

exec "$MAVEN_HOME/bin/mvn" "$@"
