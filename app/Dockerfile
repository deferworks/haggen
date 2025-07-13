# Use a base image with Java 21
FROM docker.io/eclipse-temurin:21-jdk-jammy as build

# Set the working directory
WORKDIR /app

# Copy the Gradle wrapper and build files
COPY gradlew .
COPY gradle gradle/
COPY settings.gradle .

# Make gradlew executable
RUN chmod +x gradlew

# Download Gradle dependencies and distribution (this layer will be cached)
# This ensures the Gradle distribution is downloaded and cached before the main build.
RUN ./gradlew --version

# Copy the entire project source
COPY . .

# Build the application
RUN ./gradlew app:installDist

# Use a smaller runtime image
FROM docker.io/eclipse-temurin:21-jre-jammy

# Set the working directory
WORKDIR /app

# Copy the built application from the build stage
COPY --from=build /app/app/build/install/app .

# Expose any ports if your application listens on them (e.g., for a web server)
# EXPOSE 8080

# Define the entrypoint to run the application
ENTRYPOINT ["bin/app"]