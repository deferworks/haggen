package co.deferworks.haggen.driver;


import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.util.UUID;

public class PostgresJobRegistry {
    private final HikariDataSource dataSource;
    private final String workerId = UUID.randomUUID().toString();

    public PostgresJobRegistry(String jdbcUrl, String username, String password) {
        var config = new HikariConfig();
        config.setJdbcUrl(jdbcUrl);
        config.setUsername(username);
        config.setPassword(password);

        // Enable prepared statement and server side cached queries.
        config.addDataSourceProperty("prepareThreshold", "3");
        config.addDataSourceProperty("preparedStatementCacheQueries", "256");

        this.dataSource = new HikariDataSource(config);
    }
}
