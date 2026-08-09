@REM Maven Wrapper for Windows
@REM Downloads Maven if not cached and runs the build

@echo off
SET MAVEN_VERSION=3.9.8
SET MAVEN_DIR=%USERPROFILE%\.m2\wrapper\dists\apache-maven-%MAVEN_VERSION%
SET MAVEN_HOME=%MAVEN_DIR%\apache-maven-%MAVEN_VERSION%
SET MAVEN_ZIP=%MAVEN_DIR%\apache-maven-%MAVEN_VERSION%-bin.zip

IF NOT EXIST "%MAVEN_HOME%" (
    echo Downloading Apache Maven %MAVEN_VERSION%...
    mkdir "%MAVEN_DIR%"
    powershell -Command "Invoke-WebRequest -Uri 'https://downloads.apache.org/maven/maven-3/%MAVEN_VERSION%/binaries/apache-maven-%MAVEN_VERSION%-bin.zip' -OutFile '%MAVEN_ZIP%'"
    powershell -Command "Expand-Archive -Path '%MAVEN_ZIP%' -DestinationPath '%MAVEN_DIR%' -Force"
    del "%MAVEN_ZIP%"
)

"%MAVEN_HOME%\bin\mvn.cmd" %*
