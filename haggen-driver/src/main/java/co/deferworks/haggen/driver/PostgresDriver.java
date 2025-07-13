package co.deferworks.haggen.driver;

import co.deferworks.haggen.core.JobHandler;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;

public class PostgresDriver {

    private static final Logger log = LoggerFactory.getLogger(PostgresDriver.class);

    private final HikariDataSource dataSource;
    private final HaggenEngine haggenEngine;

    public PostgresDriver(String jdbcUrl, String username, String password, JobHandler jobHandler, int numberOfWorkers, HookRegistry hookRegistry) {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(jdbcUrl);
        config.setUsername(username);
        config.setPassword(password);
        config.setMaximumPoolSize(10); // Example pool size
        config.setMinimumIdle(2); // Example minimum idle connections

        this.dataSource = new HikariDataSource(config);
        this.haggenEngine = new HaggenEngine(dataSource, jobHandler, numberOfWorkers, hookRegistry);
    }

    /**
     * Creates an returns a handle to a Postgres connection.
     *
     * @return Connection
     */
    public Connection getConnection() throws SQLException {
        return dataSource.getConnection();
    }

    public void start() {
        log.info("Starting PostgresDriver...");
        haggenEngine.start();
        log.info("PostgresDriver started.");
    }

    public void shutdown() {
        log.info("Shutting down PostgresDriver...");
        haggenEngine.shutdown();
        if (dataSource != null) {
            dataSource.close();
        }
        log.info("PostgresDriver shut down.");
    }

    public Queue getQueue() {
        return haggenEngine.getQueue();
    }
}