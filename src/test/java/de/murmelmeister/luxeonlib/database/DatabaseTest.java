package de.murmelmeister.luxeonlib.database;

import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class DatabaseTest {
    private static final int DEFAULT_PORT = 3306;
    private static final String DOT_ENV_OVERRIDE_PROPERTY = "luxeonlib.dotenv.path";

    private static DatabaseConfig cachedConfig;
    private static Properties dotEnvCache;

    private final Database database = new Database();

    @AfterEach
    void tearDown() {
        database.disconnect();
    }

    @Test
    void testConnectUsingPropertyFile() throws IOException {
        DatabaseConfig config = config();
        Path tempFile = createTemporaryPropertyFile(config);
        try {
            database.connect(tempFile.toString());
        } finally {
            Files.deleteIfExists(tempFile);
        }
        assertValidConnection();
    }

    @Test
    void testConnectUsingProperties() {
        DatabaseConfig config = config();
        Properties properties = new Properties();
        properties.setProperty("jdbcUrl", jdbcUrl(config));
        properties.setProperty("username", config.username());
        properties.setProperty("password", config.password());

        database.connect(properties);
        assertValidConnection();
    }

    @Test
    void testConnectUsingUrlUsernamePassword() {
        DatabaseConfig config = config();
        database.connect(jdbcUrl(config), config.username(), config.password());
        assertValidConnection();
    }

    @Test
    void testConnectInvalidConfiguration() {
        Properties invalidProperties = new Properties();
        invalidProperties.setProperty("jdbcUrl", "invalid:url");

        IllegalStateException exception = assertThrows(IllegalStateException.class, () -> database.connect(invalidProperties));
        assertEquals("Could not connect to database", exception.getMessage());
    }

    @Test
    void testDisconnect() {
        DatabaseConfig config = config();
        database.connect(jdbcUrl(config), config.username(), config.password());
        database.disconnect();

        assertNull(database.getDataSource(), "The data source should be cleared after disconnect");
        assertThrows(IllegalStateException.class, () -> database.queryAsync("SELECT 1", 1, rs -> rs.getInt(1), stmt -> {
        }));
    }

    @Test
    void testDisconnectIsIdempotent() {
        assertDoesNotThrow(database::disconnect);
        assertDoesNotThrow(database::disconnect);
    }

    @Test
    void testReconnectClosesPreviousDataSource() {
        DatabaseConfig config = config();
        database.connect(jdbcUrl(config), config.username(), config.password());
        HikariDataSource first = (HikariDataSource) database.getDataSource();

        database.connect(jdbcUrl(config), config.username(), config.password());
        HikariDataSource second = (HikariDataSource) database.getDataSource();

        assertTrue(first.isClosed(), "Previous data source should be closed");
        assertNotNull(second, "New data source should be available");
        assertNotSame(first, second, "A fresh data source should be created");
    }

    @Test
    void testSynchronousCrudOperations() throws Exception {
        connectToFreshDatabase();
        String table = uniqueTable("users_sync");
        createUsersTable(table);

        assertEquals(1, database.update("INSERT INTO " + table + "(name) VALUES(?)", stmt -> stmt.setString(1, "Alice")));

        int[] batchResult = database.updateBatch("INSERT INTO " + table + "(name) VALUES(?)", stmt -> {
            stmt.setString(1, "Bob");
            stmt.addBatch();
            stmt.setString(1, "Carol");
            stmt.addBatch();
        });
        assertEquals(2, batchResult.length, "Two batch statements should have executed");

        long generatedId = database.updateAndGetGeneratedKeys("INSERT INTO " + table + "(name) VALUES(?)", stmt -> stmt.setString(1, "Dave"));
        assertTrue(generatedId > 0, "Generated key should be greater than zero");

        List<String> names = database.queryList(
                "SELECT name FROM " + table + " ORDER BY id",
                rs -> rs.getString("name"),
                stmt -> {
                });
        assertEquals(Arrays.asList("Alice", "Bob", "Carol", "Dave"), names);

        String found = database.query(
                "SELECT name FROM " + table + " WHERE name = ?",
                "missing",
                rs -> rs.getString("name"),
                stmt -> stmt.setString(1, "Bob"));
        assertEquals("Bob", found);

        String fallback = database.query(
                "SELECT name FROM " + table + " WHERE name = ?",
                "fallback",
                rs -> rs.getString("name"),
                stmt -> stmt.setString(1, "Zoe"));
        assertEquals("fallback", fallback);

        assertTrue(database.exists(
                "SELECT 1 FROM " + table + " WHERE name = ?",
                stmt -> stmt.setString(1, "Alice")));
        assertFalse(database.exists(
                "SELECT 1 FROM " + table + " WHERE name = ?",
                stmt -> stmt.setString(1, "Mallory")));
    }

    @Test
    void testCallableAndAsyncOperations() throws Exception {
        connectToFreshDatabase();
        String table = uniqueTable("users_callable");
        createUsersTable(table);

        String insertProcedure = uniqueProcedure("insert_user");
        String selectProcedure = uniqueProcedure("select_user");
        String listProcedure = uniqueProcedure("list_user");

        createProcedure(insertProcedure, "IN p_name VARCHAR(100)", "INSERT INTO " + table + "(name) VALUES(p_name);");
        createProcedure(selectProcedure, "IN p_name VARCHAR(100)", "SELECT name FROM " + table + " WHERE name = p_name;");
        createProcedure(listProcedure, "", "SELECT name FROM " + table + " ORDER BY name;");

        assertEquals(1, database.updateCallable("CALL " + insertProcedure + "(?)", stmt -> stmt.setString(1, "Anna")));
        assertEquals(1, database.updateCallable("CALL " + insertProcedure + "(?)", stmt -> stmt.setString(1, "Ben")));

        String callableResult = database.queryCallable(
                "CALL " + selectProcedure + "(?)",
                "unknown",
                rs -> rs.getString("name"),
                stmt -> stmt.setString(1, "Anna"));
        assertEquals("Anna", callableResult);

        String callableFallback = database.queryCallable(
                "CALL " + selectProcedure + "(?)",
                "fallback",
                rs -> rs.getString("name"),
                stmt -> stmt.setString(1, "Cara"));
        assertEquals("fallback", callableFallback);

        CompletableFuture<String> asyncCallable = database.queryCallableAsync(
                "CALL " + selectProcedure + "(?)",
                "fallback",
                rs -> rs.getString("name"),
                stmt -> stmt.setString(1, "Ben"));
        assertEquals("Ben", asyncCallable.get(1, TimeUnit.SECONDS));

        List<String> callableList = database.queryListCallable(
                "CALL " + listProcedure + "()",
                rs -> rs.getString("name"),
                stmt -> {
                });
        assertEquals(Arrays.asList("Anna", "Ben"), callableList);

        CompletableFuture<List<String>> asyncCallableList = database.queryListCallableAsync(
                "CALL " + listProcedure + "()",
                rs -> rs.getString("name"),
                stmt -> {
                });
        assertEquals(callableList, asyncCallableList.get(1, TimeUnit.SECONDS));

        assertTrue(database.existsCallable(
                "CALL " + selectProcedure + "(?)",
                stmt -> stmt.setString(1, "Anna")));
        assertFalse(database.existsCallable(
                "CALL " + selectProcedure + "(?)",
                stmt -> stmt.setString(1, "Cara")));

        CompletableFuture<Boolean> existsCallableAsync = database.existsCallableAsync(
                "CALL " + selectProcedure + "(?)",
                stmt -> stmt.setString(1, "Ben"));
        assertTrue(existsCallableAsync.get(1, TimeUnit.SECONDS));

        CompletableFuture<Boolean> missingCallableAsync = database.existsCallableAsync(
                "CALL " + selectProcedure + "(?)",
                stmt -> stmt.setString(1, "Dana"));
        assertFalse(missingCallableAsync.get(1, TimeUnit.SECONDS));
    }

    @Test
    void testAsyncLifecycleAcrossReconnect() throws Exception {
        connectToFreshDatabase();
        String initialTable = uniqueTable("users_async");
        createUsersTable(initialTable);
        database.update("INSERT INTO " + initialTable + "(name) VALUES(?)", stmt -> stmt.setString(1, "Eve"));

        CompletableFuture<String> asyncName = database.queryAsync(
                "SELECT name FROM " + initialTable,
                "fallback",
                rs -> rs.getString("name"),
                stmt -> {
                });
        assertEquals("Eve", asyncName.get(1, TimeUnit.SECONDS));

        CompletableFuture<Boolean> existsAsync = database.existsAsync(
                "SELECT 1 FROM " + initialTable + " WHERE name = ?",
                stmt -> stmt.setString(1, "Eve"));
        assertTrue(existsAsync.get(1, TimeUnit.SECONDS));

        database.disconnect();

        assertThrows(IllegalStateException.class, () ->
                database.queryAsync("SELECT 1", 0, rs -> rs.getInt(1), stmt -> {
                }));

        connectToFreshDatabase();
        String tableAfterReconnect = uniqueTable("users_async_reconnect");
        createUsersTable(tableAfterReconnect);
        database.update("INSERT INTO " + tableAfterReconnect + "(name) VALUES(?)", stmt -> stmt.setString(1, "Frank"));

        List<String> namesAfterReconnect = database.queryListAsync(
                "SELECT name FROM " + tableAfterReconnect,
                rs -> rs.getString("name"),
                stmt -> {
                }).get(1, TimeUnit.SECONDS);
        assertEquals(Arrays.asList("Frank"), namesAfterReconnect);
    }

    @Test
    void testGetAutoIncrementMissingTableReturnsNull() throws Exception {
        connectToFreshDatabase();
        String table = "missing_" + UUID.randomUUID();
        CompletableFuture<Long> future = database.getAutoIncrement(table);
        assertNull(future.get(1, TimeUnit.SECONDS));
    }

    @Test
    void testUpdateAndGetGeneratedKeysThrowsWhenNoRows() {
        connectToFreshDatabase();
        String table = uniqueTable("users_no_rows");
        createUsersTable(table);

        RuntimeException exception = assertThrows(RuntimeException.class, () ->
                database.updateAndGetGeneratedKeys(
                        "INSERT INTO " + table + "(name) SELECT ? WHERE 1 = 0",
                        stmt -> stmt.setString(1, "Nope")));
        assertTrue(exception.getCause() instanceof SQLException);
    }

    @Test
    void testProcedureQueryValidation() {
        String procedureSql = Database.getProcedureQuery(
                "test_proc",
                "IN id INT",
                "SELECT id;");
        assertEquals(
                "CREATE PROCEDURE IF NOT EXISTS test_proc(IN id INT)\n BEGIN\n    SELECT id; \nEND;",
                procedureSql);

        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> Database.getProcedureQuery("invalid-name", "", ""));
        assertEquals("Invalid procedure name: invalid-name", exception.getMessage());
    }

    @Test
    void testAutoIncrementValidationAndFailure() throws Exception {
        connectToFreshDatabase();
        String table = uniqueTable("users_autoinc");
        createUsersTable(table);
        database.update("INSERT INTO " + table + "(name) VALUES(?)", stmt -> stmt.setString(1, "Grace"));

        IllegalArgumentException invalidName = assertThrows(
                IllegalArgumentException.class,
                () -> database.getAutoIncrement("invalid-name!"));
        assertEquals("Invalid table name: invalid-name!", invalidName.getMessage());

        CompletableFuture<Long> autoIncrementFuture = database.getAutoIncrement(table);
        try {
            Long value = autoIncrementFuture.get(1, TimeUnit.SECONDS);
            assertNotNull(value, "Auto increment value should be available or the query should fail");
            assertTrue(value >= 2, "Next auto increment value should be at least 2");
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof RuntimeException, "Failures should wrap in RuntimeException");
        }

        IllegalArgumentException invalidTable = assertThrows(
                IllegalArgumentException.class,
                () -> database.createTable("invalid-name!", "id INT"));
        assertEquals("Invalid table name: invalid-name!", invalidTable.getMessage());
    }

    @Test
    void testConfigLoadsFromDotEnvOverride() throws Exception {
        Path tempDotEnv = Files.createTempFile("dotenv-test", ".env");
        Files.writeString(tempDotEnv, String.join("\n",
                "DB_HOST=10.0.0.5",
                "DB_PORT=12345",
                "MARIADB_DATABASE=test_db",
                "MARIADB_USER=test_user",
                "MARIADB_PASSWORD=\"test_pwd\"",
                ""));

        String previousOverride = System.getProperty(DOT_ENV_OVERRIDE_PROPERTY);
        System.setProperty(DOT_ENV_OVERRIDE_PROPERTY, tempDotEnv.toString());
        resetConfigCache();
        try {
            DatabaseConfig overrideConfig = config();
            assertEquals("10.0.0.5", overrideConfig.host());
            assertEquals(12345, overrideConfig.port());
            assertEquals("test_db", overrideConfig.database());
            assertEquals("test_user", overrideConfig.username());
            assertEquals("test_pwd", overrideConfig.password());
        } finally {
            if (previousOverride != null)
                System.setProperty(DOT_ENV_OVERRIDE_PROPERTY, previousOverride);
            else
                System.clearProperty(DOT_ENV_OVERRIDE_PROPERTY);
            resetConfigCache();
            Files.deleteIfExists(tempDotEnv);
        }
    }

    private void connectToFreshDatabase() {
        DatabaseConfig config = config();
        database.connect(jdbcUrl(config), config.username(), config.password());
    }

    private void createUsersTable(String tableName) {
        database.update("DROP TABLE IF EXISTS " + tableName, stmt -> {
        });
        database.createTable(tableName, "id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(100)");
    }

    private String jdbcUrl(DatabaseConfig config) {
        return "jdbc:mariadb://" + config.host() + ":" + config.port() + "/" + config.database() + "?allowPublicKeyRetrieval=true&useSSL=false";
    }

    private String uniqueTable(String prefix) {
        return prefix + "_" + UUID.randomUUID().toString().replace("-", "");
    }

    private String uniqueProcedure(String prefix) {
        return prefix + "_" + UUID.randomUUID().toString().replace("-", "");
    }

    private void assertValidConnection() {
        DataSource dataSource = database.getDataSource();
        assertNotNull(dataSource, "The data source should be initialized");
        assertDoesNotThrow(() -> {
            try (Connection connection = dataSource.getConnection()) {
                assertTrue(connection.isValid(2), "The connection should be valid");
            }
        }, "Should connect to the database without exception");
    }

    private void dropProcedure(String procedureName) {
        database.update("DROP PROCEDURE IF EXISTS " + procedureName, stmt -> {
        });
    }

    private void createProcedure(String name, String parameters, String body) {
        dropProcedure(name);
        database.update(Database.getProcedureQuery(name, parameters, body), stmt -> {
        });
    }

    private Path createTemporaryPropertyFile(DatabaseConfig config) throws IOException {
        Properties properties = loadTemplateProperties();
        properties.setProperty("jdbcUrl", jdbcUrl(config));
        properties.setProperty("username", config.username());
        properties.setProperty("password", config.password());

        Path tempFile = Files.createTempFile("database-test-", ".properties");
        try (OutputStream outputStream = Files.newOutputStream(tempFile)) {
            properties.store(outputStream, "database test override");
        }
        return tempFile;
    }

    private Properties loadTemplateProperties() throws IOException {
        try (InputStream inputStream = getClass().getResourceAsStream("/test-database.properties")) {
            if (inputStream == null)
                throw new IOException("test-database.properties resource not found");
            Properties properties = new Properties();
            properties.load(inputStream);
            return properties;
        }
    }

    private static synchronized DatabaseConfig config() {
        if (cachedConfig == null)
            cachedConfig = loadConfigOrSkip();
        return cachedConfig;
    }

    private static DatabaseConfig loadConfigOrSkip() {
        String host = envOrDefault("DB_HOST", "127.0.0.1");
        int port = parseIntOrDefault(envOrNull("DB_PORT"), DEFAULT_PORT);
        String database = envOrNull("MARIADB_DATABASE");
        String username = envOrNull("MARIADB_USER");
        String password = envOrNull("MARIADB_PASSWORD");

        Assumptions.assumeTrue(database != null && !database.isBlank(), "Skipping database tests: MARIADB_DATABASE is not set");
        Assumptions.assumeTrue(username != null && !username.isBlank(), "Skipping database tests: MARIADB_USER is not set");
        Assumptions.assumeTrue(password != null && !password.isBlank(), "Skipping database tests: MARIADB_PASSWORD is not set");

        return new DatabaseConfig(host, port, database, username, password);
    }

    private static String envOrDefault(String key, String fallback) {
        String value = envOrNull(key);
        return (value == null || value.isBlank()) ? fallback : value;
    }

    private static String envOrNull(String key) {
        String value = sanitize(System.getenv(key));
        if (value == null || value.isBlank())
            value = sanitize(dotEnv().getProperty(key));
        return (value == null || value.isBlank()) ? null : value;
    }

    private static Properties dotEnv() {
        if (dotEnvCache == null)
            dotEnvCache = loadDotEnv(resolveDotEnvPath());
        return dotEnvCache;
    }

    private static Properties loadDotEnv(Path path) {
        Properties properties = new Properties();
        if (path == null || !Files.exists(path))
            return properties;
        try (InputStream inputStream = Files.newInputStream(path)) {
            properties.load(inputStream);
        } catch (IOException ignored) {
        }
        return properties;
    }

    private static Path resolveDotEnvPath() {
        String overridePath = System.getProperty(DOT_ENV_OVERRIDE_PROPERTY);
        if (overridePath != null && !overridePath.isBlank())
            return Path.of(overridePath.trim());
        return Path.of(System.getProperty("user.dir"), ".env");
    }

    private static int parseIntOrDefault(String value, int fallback) {
        if (value == null || value.isBlank())
            return fallback;
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException ignored) {
            return fallback;
        }
    }

    private static String sanitize(String value) {
        if (value == null)
            return null;
        String trimmed = value.trim();
        if (trimmed.length() >= 2 && trimmed.startsWith("\"") && trimmed.endsWith("\""))
            trimmed = trimmed.substring(1, trimmed.length() - 1);
        return trimmed;
    }

    private static synchronized void resetConfigCache() {
        cachedConfig = null;
        dotEnvCache = null;
    }

    private record DatabaseConfig(String host, int port, String database, String username, String password) {
    }
}
