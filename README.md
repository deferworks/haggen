# Haggen

Haggen is a reliable and fault-tolerant PostgreSQL-backed job queue written in modern Java. It's designed for
simplicity, high performance under concurrency, and robust reliability.

## Features

Haggen's core features and reliability are based on very simple principles:

*   **Reliability & Fault Tolerance**: Implements a lease/heartbeat/reaper model to ensure no job is lost due to worker failure.
*   **Transactional Enqueueing**: Allows jobs to be enqueued with transactional semantics.
*   **Robust Job Processing**: Supports reliable processing of jobs with a lease-based system that prevents jobs from being lost or unnecessarily retried when workers fail.
*   **Distributed Job Fetching**: Leverages PostgreSQL's `FOR UPDATE SKIP LOCKED` to efficiently fetch and lock jobs across multiple worker instances.
*   **Extensible Hooks**: Provides a `HookRegistry` to allow users to run custom logic at various points in a job's lifecycle (enqueue, dequeue, complete, fail, discard, reap).
*   **Idiomatic Modern Java**: Embraces the latest LTS version of Java (Java 21+) and its features for concise, expressive, and safe code.

## Getting Started

### Prerequisites

* Java Development Kit (JDK) 21 or later
* Gradle (optional, for local development without Docker)
* Docker or Podman (recommended for easy setup)

### Running the Example with Docker Compose (Recommended)

The easiest way to get started with Haggen is using Docker Compose (or Podman Compose). This will spin up a PostgreSQL
database and the Haggen application.

1. **Build the Docker images**:
   ```bash
   docker compose build
   ```
   (If you are using Podman, replace `docker compose` with `podman compose`.)

2. **Start the application and database**:
   ```bash
   docker compose up
   ```
   This command will:
    * Start the PostgreSQL database container.
    * Wait for the database to be healthy.
    * Run Flyway database migrations to set up the schema.
    * Start the Haggen application container.

   You should see logs from both the database and the application, indicating jobs being enqueued and processed.

3. **Stop the services**:
   ```bash
   docker compose down
   ```

### Local Development (without Docker Compose)

1. **Set up PostgreSQL**: Ensure you have a PostgreSQL instance running and accessible. Update the `JDBC_URL`,
   `DB_USER`, and `DB_PASSWORD` environment variables or directly in `app/src/main/java/co/deferworks/haggen/App.java`
   to match your database configuration.
2. **Run Flyway Migrations**:
   ```bash
   ./gradlew haggen-db:flywayMigrate
   ```
3. **Run the application**:
   ```bash
   ./gradlew app:run
   ```

## Project Structure

* `haggen-core`: Contains the pure, dependency-free data models (`Job`, `JobState`, `JobLeaseKind`, `JobHandler`).
* `haggen-driver`: The main engine. Contains the database repository, worker pool, and runtime logic (
  `PostgresJobRepository`, `HaggenEngine`, `Worker`, `HookRegistry`).
* `haggen-db`: Manages the database schema evolution using Flyway. Contains only `.sql` migration files.
* `haggen-app`: A sample application demonstrating the `haggen` library.

## License

This project is under an [APACHE 2.0](LICENSE) license.
