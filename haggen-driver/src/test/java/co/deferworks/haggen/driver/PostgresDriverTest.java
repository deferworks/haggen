package co.deferworks.haggen.driver;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

class PostgresDriverTest {
    private static final PostgreSQLContainer<?> pgContainer = new PostgreSQLContainer("postgres:16.9")
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
    void shouldNotBeNull() {
        assertNotNull(PostgresDriver.class);
    }

    @Test
    void shouldQueryPostgresContainer() {
        var jdbcUrl = pgContainer.getJdbcUrl();
        var properties = new Properties();
        properties.setProperty("user", "haggen-driver-user");
        properties.setProperty("password", "haggen-driver-secret");
        properties.setProperty("ssl", "false");


        System.out.println("Jdbc Url: " + jdbcUrl);

        try {
            var conn = DriverManager.getConnection(jdbcUrl, properties);
            var stmt = conn.createStatement();
            var rs = stmt.executeQuery("SELECT 1 FROM pg_database WHERE datname = 'haggen-tests-db';");

            while (rs.next()) {
                assertEquals(1, rs.getInt(1));
            }
        } catch (SQLException e) {
            fail(e);
        }


    }
}
