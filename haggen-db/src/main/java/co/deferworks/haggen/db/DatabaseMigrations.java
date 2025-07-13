package co.deferworks.haggen.db;

import org.flywaydb.core.Flyway;

public class DatabaseMigrations {

    public static void runMigrations(String jdbcUrl, String username, String password) {
        Flyway flyway = Flyway.configure()
                .dataSource(jdbcUrl, username, password)
                .locations("classpath:db/migration")
                .load();
        flyway.migrate();
    }
}