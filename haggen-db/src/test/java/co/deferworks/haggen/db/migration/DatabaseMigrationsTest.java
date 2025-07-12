package co.deferworks.haggen.db.migration;

import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class DatabaseMigrationsTest {

    private static final PostgreSQLContainer<?> pgContainer = new PostgreSQLContainer<>("postgres:16.9")
            .withDatabaseName("haggen-tests-db")
            .withUsername("haggen-driver-user")
            .withPassword("haggen-driver-secret");

    @BeforeAll
    static void beforeAll() {
        pgContainer.start();
    }

    @AfterAll
    static void afterAll() {
        pgContainer.stop();
    }

    @Test
    public void postgresContainerShouldBeRunning() {
        Assertions.assertTrue(pgContainer.isRunning());
    }

    @Test
    public void migrationShouldRunSuccessfully() {
        var jdbcUrl = pgContainer.getJdbcUrl();
        var username = pgContainer.getUsername();
        var password = pgContainer.getPassword();

        var flyway = Flyway.configure()
                .dataSource(jdbcUrl, username, password)
                .locations("classpath:db/migration")
                .load();

        flyway.migrate();

        try (Connection conn = DriverManager.getConnection(jdbcUrl, username, password);
             Statement st = conn.createStatement()) {

            var tables = st.executeQuery("SELECT to_regclass('public.jobs') IS NOT NULL AS table_exists");
            Assertions.assertTrue(tables.next());
            Assertions.assertTrue(tables.getBoolean("table_exists"), "'jobs' table should be created by the migration");

            var migrationsCount = st.executeQuery("SELECT COUNT(*) FROM flyway_schema_history WHERE version = '1' AND success = TRUE");
            Assertions.assertTrue(migrationsCount.next());
            Assertions.assertEquals(1, migrationsCount.getInt(1));
        } catch (SQLException e) {
            Assertions.fail(e);
        }
    }
}
