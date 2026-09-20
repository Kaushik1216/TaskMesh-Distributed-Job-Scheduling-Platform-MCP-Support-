FROM eclipse-temurin:21-jre-alpine
WORKDIR /app
COPY target/taskmesh-worker-*.jar app.jar
EXPOSE 8083
ENTRYPOINT ["java", "-jar", "app.jar"]
