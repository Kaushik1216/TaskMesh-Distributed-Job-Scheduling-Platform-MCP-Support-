FROM eclipse-temurin:21-jre-alpine
WORKDIR /app
COPY target/taskmesh-mcp-*.jar app.jar
EXPOSE 8090
ENTRYPOINT ["java", "-jar", "app.jar"]
