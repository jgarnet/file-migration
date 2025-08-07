FROM gradle:8.7.0-jdk21 AS builder

WORKDIR /app

# gradle setup
COPY build.gradle settings.gradle ./
COPY gradle ./gradle
COPY gradlew ./
COPY /gradle ./gradle

# copy project src
COPY src ./src

# build new jar
RUN ./gradlew clean shadowJar

# lightweight runtime image
FROM eclipse-temurin:21-jre

WORKDIR /app

COPY --from=builder /app/build/libs/file-migration.jar app.jar

ENTRYPOINT ["java", "-jar", "app.jar"]