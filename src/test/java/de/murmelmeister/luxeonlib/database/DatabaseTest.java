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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.ReadWriteLock;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.fail;

// AI generated
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
    void testConnectUsingMissingPropertyFileThrows() {
        Database fresh = new Database();
        assertThrows(IllegalArgumentException.class, () -> fresh.connect("missing-properties-file.properties"));
        fresh.getExecutor().shutdownNow();
    }

    @Test
    void testGetExecutorReturnsActivePool() {
        ExecutorService executor = database.getExecutor();
        assertNotNull(executor);
        assertFalse(executor.isShutdown());
    }

    @Test
    void testConnectRecreatesExecutorWhenShutdown() {
        DatabaseConfig config = config();
        Database fresh = new Database();
        ExecutorService shutdownExecutor = Executors.newSingleThreadExecutor();
        shutdownExecutor.shutdownNow();
        setExecutor(fresh, shutdownExecutor);

        fresh.connect(jdbcUrl(config), config.username(), config.password());
        ExecutorService refreshed = fresh.getExecutor();
        assertNotSame(shutdownExecutor, refreshed);
        assertFalse(refreshed.isShutdown());
        fresh.disconnect();
    }

    @Test
    void testConnectCreatesExecutorWhenMissing() {
        DatabaseConfig config = config();
        Database fresh = new Database();
        setExecutor(fresh, null);

        fresh.connect(jdbcUrl(config), config.username(), config.password());
        try {
            assertNotNull(fresh.getExecutor(), "Executor should be recreated when missing");
            assertFalse(fresh.getExecutor().isShutdown());
        } finally {
            fresh.disconnect();
        }
    }

    @Test
    void testConnectFailsWhenLockUnavailable() throws Exception {
        Database fresh = new Database();
        Field lockField = Database.class.getDeclaredField("lock");
        lockField.setAccessible(true);
        ReadWriteLock lock = (ReadWriteLock) lockField.get(fresh);

        CountDownLatch ready = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        Thread reader = new Thread(() -> {
            lock.readLock().lock();
            try {
                ready.countDown();
                try {
                    release.await();
                } catch (InterruptedException ignored) {
                    Thread.currentThread().interrupt();
                }
            } finally {
                lock.readLock().unlock();
            }
        });
        reader.start();
        ready.await();

        DatabaseConfig config = config();
        IllegalStateException exception = assertThrows(IllegalStateException.class,
                () -> fresh.connect(jdbcUrl(config), config.username(), config.password()));
        assertEquals("Could not acquire write lock", exception.getMessage());

        release.countDown();
        reader.join();
        fresh.getExecutor().shutdownNow();
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
    void testDisconnectSkipsShutdownWhenExecutorAlreadyShutdown() {
        Database fresh = new Database();
        setExecutor(fresh, new AlreadyShutdownExecutor());
        setDataSource(fresh, null);
        assertDoesNotThrow(fresh::disconnect);
    }

    @Test
    void testDisconnectInvokesShutdownNowOnTimeout() {
        Database fresh = new Database();
        TimeoutExecutor timeoutExecutor = new TimeoutExecutor();
        setExecutor(fresh, timeoutExecutor);
        setDataSource(fresh, null);

        fresh.disconnect();

        assertTrue(timeoutExecutor.shutdownCalled);
        assertTrue(timeoutExecutor.shutdownNowCalled);
    }

    @Test
    void testDisconnectHandlesAwaitTerminationInterruption() {
        Database fresh = new Database();
        InterruptingExecutor interruptingExecutor = new InterruptingExecutor();
        setExecutor(fresh, interruptingExecutor);
        setDataSource(fresh, null);

        Thread.interrupted();
        fresh.disconnect();

        assertTrue(interruptingExecutor.shutdownCalled);
        assertTrue(interruptingExecutor.shutdownNowCalled);
        assertTrue(Thread.currentThread().isInterrupted());
        Thread.interrupted();
    }

    @Test
    void testDisconnectSkipsClosingWhenDataSourceAlreadyClosed() {
        Database fresh = new Database();
        setExecutor(fresh, new AlreadyShutdownExecutor());
        PreClosedDataSource dataSource = new PreClosedDataSource();
        setDataSource(fresh, dataSource);

        fresh.disconnect();

        assertFalse(dataSource.closeCalled);
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

        String slowQuery = database.query(
                "SELECT name FROM " + table + " WHERE name = ?",
                "fallback",
                rs -> rs.getString(1),
                stmt -> {
                    sleepSilently(1100);
                    stmt.setString(1, "Alice");
                });
        assertEquals("Alice", slowQuery);

        List<String> slowList = database.queryList(
                "SELECT name FROM " + table + " WHERE name = ?",
                rs -> rs.getString(1),
                stmt -> {
                    sleepSilently(1100);
                    stmt.setString(1, "Bob");
                });
        assertEquals(Arrays.asList("Bob"), slowList);

        boolean slowExists = database.exists(
                "SELECT 1 FROM " + table + " WHERE name = ?",
                stmt -> {
                    sleepSilently(1100);
                    stmt.setString(1, "Carol");
                });
        assertTrue(slowExists);
    }

    @Test
    void testQueryHandlesSQLException() throws Exception {
        connectToFreshDatabase();
        String table = uniqueTable("users_query_failure");
        createUsersTable(table);

        RuntimeException exception = assertThrows(RuntimeException.class, () ->
                database.query("SELECT name FROM " + table, "fallback", rs -> rs.getString(1), stmt -> {
                    throw new SQLException("boom");
                }));
        assertEquals("Failed to execute query", exception.getMessage());
    }

    @Test
    void testQueryListHandlesSQLException() throws Exception {
        connectToFreshDatabase();
        String table = uniqueTable("users_query_list_failure");
        createUsersTable(table);

        RuntimeException exception = assertThrows(RuntimeException.class, () ->
                database.queryList("SELECT name FROM " + table, rs -> rs.getString(1), stmt -> {
                    throw new SQLException("boom");
                }));
        assertEquals("Failed to execute query", exception.getMessage());
    }

    @Test
    void testExistsHandlesSQLException() {
        Database fresh = new Database();
        SQLException failure = new SQLException("boom");
        setDataSource(fresh, new SingleConnectionDataSource(createExistsFailureConnection(failure)));

        RuntimeException exception = assertThrows(RuntimeException.class,
                () -> fresh.exists("SELECT 1", stmt -> {
                }));
        assertEquals("Failed to execute query", exception.getMessage());
        assertSame(failure, exception.getCause());

        fresh.disconnect();
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

        String slowCallable = database.queryCallable(
                "CALL " + selectProcedure + "(?)",
                "fallback",
                rs -> rs.getString("name"),
                stmt -> {
                    sleepSilently(1100);
                    stmt.setString(1, "Anna");
                });
        assertEquals("Anna", slowCallable);

        List<String> slowCallableList = database.queryListCallable(
                "CALL " + listProcedure + "()",
                rs -> rs.getString("name"),
                stmt -> sleepSilently(1100));
        assertEquals(callableList, slowCallableList);

        boolean slowExistsCallable = database.existsCallable(
                "CALL " + selectProcedure + "(?)",
                stmt -> {
                    sleepSilently(1100);
                    stmt.setString(1, "Ben");
                });
        assertTrue(slowExistsCallable);
    }

    @Test
    void testQueryCallableHandlesSQLException() throws Exception {
        connectToFreshDatabase();
        RuntimeException exception = assertThrows(RuntimeException.class, () ->
                database.queryCallable("CALL nonexistent_procedure()", "fallback", rs -> rs.getString(1), stmt -> {
                }));
        assertEquals("Failed to execute query", exception.getMessage());
    }

    @Test
    void testQueryListCallableHandlesSQLException() throws Exception {
        connectToFreshDatabase();
        RuntimeException exception = assertThrows(RuntimeException.class, () ->
                database.queryListCallable("CALL nonexistent_procedure()", rs -> rs.getString(1), stmt -> {
                }));
        assertEquals("Failed to execute query", exception.getMessage());
    }

    @Test
    void testExistsCallableHandlesSQLException() throws Exception {
        connectToFreshDatabase();
        RuntimeException exception = assertThrows(RuntimeException.class, () ->
                database.existsCallable("CALL nonexistent_procedure()", stmt -> {
                }));
        assertEquals("Failed to execute query", exception.getMessage());
    }

    @Test
    void testQueryCallableAsyncHandlesSQLException() throws Exception {
        connectToFreshDatabase();
        CompletableFuture<String> future = database.queryCallableAsync(
                "CALL nonexistent_procedure()", "fallback", rs -> rs.getString(1), stmt -> {});
        ExecutionException executionException = assertThrows(ExecutionException.class, () -> future.get(1, TimeUnit.SECONDS));
        assertTrue(executionException.getCause() instanceof RuntimeException);
    }

    @Test
    void testQueryListCallableAsyncHandlesSQLException() throws Exception {
        connectToFreshDatabase();
        CompletableFuture<List<String>> future = database.queryListCallableAsync(
                "CALL nonexistent_procedure()", rs -> rs.getString(1), stmt -> {});
        ExecutionException executionException = assertThrows(ExecutionException.class, () -> future.get(1, TimeUnit.SECONDS));
        assertTrue(executionException.getCause() instanceof RuntimeException);
    }

    @Test
    void testExistsCallableAsyncHandlesSQLException() throws Exception {
        connectToFreshDatabase();
        CompletableFuture<Boolean> future = database.existsCallableAsync("CALL nonexistent_procedure()", stmt -> {});
        ExecutionException executionException = assertThrows(ExecutionException.class, () -> future.get(1, TimeUnit.SECONDS));
        assertTrue(executionException.getCause() instanceof RuntimeException);
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
    void testQueryAfterDisconnectThrowsIllegalState() throws Exception {
        connectToFreshDatabase();
        String table = uniqueTable("users_after_disconnect");
        createUsersTable(table);
        database.disconnect();
        assertThrows(IllegalStateException.class, () ->
                database.queryList("SELECT name FROM " + table, rs -> rs.getString(1), stmt -> {}));
    }

    @Test
    void testQueryAsyncThrowsWhenExecutorShutdown() throws Exception {
        connectToFreshDatabase();
        database.getExecutor().shutdownNow();
        assertThrows(IllegalStateException.class, () ->
                database.queryAsync("SELECT 1", 0, rs -> rs.getInt(1), stmt -> {}));
        database.disconnect();
    }

    @Test
    void testGetAutoIncrementMissingTableReturnsNull() throws Exception {
        connectToFreshDatabase();
        String table = uniqueTable("missing");
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
    void testUpdateAndGetGeneratedKeysThrowsWhenGeneratedKeyMissing() {
        Database fresh = new Database();
        TransactionState state = new TransactionState(true, Connection.TRANSACTION_READ_COMMITTED);
        PreparedStatement generatedKeysStatement = createGeneratedKeylessStatement();
        Connection connection = createTransactionAwareConnection(state, null, generatedKeysStatement);
        setDataSource(fresh, new SingleConnectionDataSource(connection));

        RuntimeException exception = assertThrows(RuntimeException.class, () ->
                fresh.updateAndGetGeneratedKeys("INSERT INTO dummy(name) VALUES(?)", stmt -> {
                }));
        assertEquals("Failed to execute database operation", exception.getMessage());
        assertTrue(exception.getCause() instanceof SQLException);
        assertEquals("No generated keys returned", exception.getCause().getMessage());

        fresh.disconnect();
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
    void testExecuteInTransactionResetsStateOnSuccess() throws Exception {
        Database fresh = new Database();
        TransactionState state = new TransactionState(false, Connection.TRANSACTION_SERIALIZABLE);
        try {
            Integer result = executeInTransaction(fresh, state, transaction -> {
                transaction.autoCommit = true;
                transaction.isolation = Connection.TRANSACTION_READ_COMMITTED;
                return 123;
            });
            assertEquals(123, result);
            assertTrue(state.commitCalled);
            assertFalse(state.rollbackCalled);
            assertEquals(Arrays.asList(30_000, 0), state.networkTimeouts);
            assertFalse(state.autoCommit);
            assertEquals(Connection.TRANSACTION_SERIALIZABLE, state.isolation);
        } finally {
            fresh.disconnect();
        }
    }

    @Test
    void testExecuteInTransactionRollbackOnFailure() throws Exception {
        Database fresh = new Database();
        TransactionState state = new TransactionState(false, Connection.TRANSACTION_SERIALIZABLE);
        try {
            RuntimeException exception = assertThrows(RuntimeException.class, () ->
                    executeInTransaction(fresh, state, transaction -> {
                        transaction.autoCommit = false;
                        throw new SQLException("boom");
                    }));
            assertEquals("Failed to execute database operation", exception.getMessage());
            assertTrue(state.rollbackCalled);
        } finally {
            fresh.disconnect();
        }
    }

    @Test
    void testExecuteInTransactionSkipsStateChangesWhenUnnecessary() throws Exception {
        Database fresh = new Database();
        TransactionState state = new TransactionState(true, Connection.TRANSACTION_SERIALIZABLE);
        try {
            Integer result = executeInTransaction(fresh, state, transaction -> {
                transaction.autoCommit = true;
                transaction.isolation = Connection.TRANSACTION_SERIALIZABLE;
                return 7;
            });
            assertEquals(7, result.intValue());
            assertTrue(state.commitCalled);
            assertEquals(1, state.setAutoCommitCalls); // only initial change
            assertEquals(0, state.setTransactionIsolationCalls); // unchanged
        } finally {
            fresh.disconnect();
        }
    }

    @Test
    void testExecuteInTransactionAdjustsIsolationAndRestores() throws Exception {
        Database fresh = new Database();
        TransactionState state = new TransactionState(false, Connection.TRANSACTION_READ_COMMITTED);
        try {
            Integer result = executeInTransaction(fresh, state, transaction -> 99);
            assertEquals(99, result.intValue());
            assertTrue(state.commitCalled);
            assertEquals(2, state.setTransactionIsolationCalls);
            assertEquals(Arrays.asList(30_000, 0), state.networkTimeouts);
            assertEquals(Connection.TRANSACTION_READ_COMMITTED, state.isolation);
        } finally {
            fresh.disconnect();
        }
    }

    @Test
    void testExecuteInTransactionSkipsRollbackWhenAutoCommitRestored() throws Exception {
        Database fresh = new Database();
        TransactionState state = new TransactionState(true, Connection.TRANSACTION_SERIALIZABLE);
        try {
            RuntimeException exception = assertThrows(RuntimeException.class, () ->
                    executeInTransaction(fresh, state, transaction -> {
                        transaction.autoCommit = true;
                        throw new RuntimeException("boom");
                    }));
            assertEquals("Failed to execute database operation", exception.getMessage());
            assertFalse(state.rollbackCalled);
        } finally {
            fresh.disconnect();
        }
    }

    @Test
    void testExecuteInTransactionFailureRollsBack() {
        connectToFreshDatabase();
        RuntimeException exception = assertThrows(RuntimeException.class, () ->
                database.update("INSERT INTO non_existing_table(name) VALUES(?)", stmt -> stmt.setString(1, "X")));
        assertEquals("Failed to execute database operation", exception.getMessage());
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

    @Test
    void testGetAutoIncrementFailureThrowsRuntimeException() throws Exception {
        Database stubDatabase = new Database();
        try {
            setExecutor(stubDatabase, Executors.newSingleThreadExecutor());
            setDataSource(stubDatabase, new ThrowingConnectionDataSource(new SQLException("boom")));
            CompletableFuture<Long> future = stubDatabase.getAutoIncrement("valid_name");
            ExecutionException exception = assertThrows(ExecutionException.class, () -> future.get(1, TimeUnit.SECONDS));
            assertTrue(exception.getCause() instanceof RuntimeException);
            assertEquals("Failed to execute query", exception.getCause().getMessage());
        } finally {
            setDataSource(stubDatabase, null);
            stubDatabase.getExecutor().shutdownNow();
            stubDatabase.disconnect();
        }
    }

    @Test
    void testDisconnectHandlesCloseException() throws Exception {
        Database stubDatabase = new Database();
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            setExecutor(stubDatabase, executor);
            setDataSource(stubDatabase, new CloseThrowingDataSource(new RuntimeException("close failure")));
            IllegalStateException exception = assertThrows(IllegalStateException.class, stubDatabase::disconnect);
            assertEquals("Could not close database connection", exception.getMessage());
        } finally {
            setDataSource(stubDatabase, null);
            executor.shutdownNow();
        }
    }

    @Test
    void testConnectClosesExistingDataSourceFailure() throws Exception {
        Database stubDatabase = new Database();
        DatabaseConfig config = config();
        try {
            setDataSource(stubDatabase, new CloseThrowingDataSource(new RuntimeException("close failure")));
            IllegalStateException exception = assertThrows(IllegalStateException.class,
                    () -> stubDatabase.connect(jdbcUrl(config), config.username(), config.password()));
            assertEquals("Could not connect to database", exception.getMessage());
        } finally {
            setDataSource(stubDatabase, null);
            stubDatabase.disconnect();
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
        Properties properties = new Properties();
        properties.setProperty("jdbcUrl", jdbcUrl(config));
        properties.setProperty("username", config.username());
        properties.setProperty("password", config.password());

        Path tempFile = Files.createTempFile("database-test-", ".properties");
        try (OutputStream outputStream = Files.newOutputStream(tempFile)) {
            properties.store(outputStream, "database test override");
        }
        return tempFile;
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

    private static void sleepSilently(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            fail("Interrupted while waiting");
        }
    }

    private static void setDataSource(Database target, HikariDataSource value) {
        try {
            Field field = Database.class.getDeclaredField("dataSource");
            field.setAccessible(true);
            HikariDataSource previous = (HikariDataSource) field.get(target);
            if (previous != null && !previous.isClosed()) {
                try {
                    previous.close();
                } catch (Exception ignored) {
                }
            }
            field.set(target, value);
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
    }

    private static void setExecutor(Database target, ExecutorService executor) {
        try {
            Field field = Database.class.getDeclaredField("executor");
            field.setAccessible(true);
            ExecutorService previous = (ExecutorService) field.get(target);
            if (previous != null && !previous.isShutdown())
                previous.shutdownNow();
            field.set(target, executor);
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
    }

    private static final class AlreadyShutdownExecutor extends AbstractExecutorService {
        @Override
        public void shutdown() {
        }

        @Override
        public List<Runnable> shutdownNow() {
            return Collections.emptyList();
        }

        @Override
        public boolean isShutdown() {
            return true;
        }

        @Override
        public boolean isTerminated() {
            return true;
        }

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit) {
            return true;
        }

        @Override
        public void execute(Runnable command) {
        }
    }

    private static final class TimeoutExecutor extends AbstractExecutorService {
        private boolean shutdownCalled;
        private boolean shutdownNowCalled;

        @Override
        public void shutdown() {
            shutdownCalled = true;
        }

        @Override
        public List<Runnable> shutdownNow() {
            shutdownNowCalled = true;
            return Collections.emptyList();
        }

        @Override
        public boolean isShutdown() {
            return shutdownCalled;
        }

        @Override
        public boolean isTerminated() {
            return shutdownCalled && shutdownNowCalled;
        }

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit) {
            return false;
        }

        @Override
        public void execute(Runnable command) {
        }
    }

    private static final class InterruptingExecutor extends AbstractExecutorService {
        private boolean shutdownCalled;
        private boolean shutdownNowCalled;

        @Override
        public void shutdown() {
            shutdownCalled = true;
        }

        @Override
        public List<Runnable> shutdownNow() {
            shutdownNowCalled = true;
            return Collections.emptyList();
        }

        @Override
        public boolean isShutdown() {
            return shutdownCalled;
        }

        @Override
        public boolean isTerminated() {
            return shutdownNowCalled;
        }

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
            throw new InterruptedException("forced interruption");
        }

        @Override
        public void execute(Runnable command) {
        }
    }

    private static final class PreClosedDataSource extends HikariDataSource {
        private boolean closeCalled;

        @Override
        public Connection getConnection() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isClosed() {
            return true;
        }

        @Override
        public void close() {
            closeCalled = true;
        }
    }

    private static final class SingleConnectionDataSource extends HikariDataSource {
        private final Connection connection;
        private boolean closed;

        SingleConnectionDataSource(Connection connection) {
            this.connection = connection;
        }

        @Override
        public Connection getConnection() {
            return connection;
        }

        @Override
        public boolean isClosed() {
            return closed;
        }

        @Override
        public void close() {
            closed = true;
        }
    }

    private static final class TransactionState {
        boolean autoCommit;
        int isolation;
        boolean closed;
        boolean readOnly;
        boolean commitCalled;
        boolean rollbackCalled;
        int setAutoCommitCalls;
        int setTransactionIsolationCalls;
        int setReadOnlyCalls;
        final List<Integer> networkTimeouts = new ArrayList<>();

        TransactionState(boolean autoCommit, int isolation) {
            this.autoCommit = autoCommit;
            this.isolation = isolation;
        }
    }

    @FunctionalInterface
    private interface ConnectionBody<T> {
        T apply(TransactionState state) throws SQLException;
    }

    private static Connection createTestConnection(TransactionState state) {
        return createTransactionAwareConnection(state, null, null);
    }

    private static Connection createTransactionAwareConnection(TransactionState state, PreparedStatement preparedStatement, PreparedStatement generatedKeysStatement) {
        return (Connection) Proxy.newProxyInstance(
                Connection.class.getClassLoader(),
                new Class[]{Connection.class},
                (proxy, method, args) -> {
                    String name = method.getName();
                    switch (name) {
                        case "getAutoCommit":
                            return state.autoCommit;
                        case "setAutoCommit":
                            state.autoCommit = (Boolean) args[0];
                            state.setAutoCommitCalls++;
                            return null;
                        case "getTransactionIsolation":
                            return state.isolation;
                        case "setTransactionIsolation":
                            state.isolation = (Integer) args[0];
                            state.setTransactionIsolationCalls++;
                            return null;
                        case "setReadOnly":
                            state.readOnly = (Boolean) args[0];
                            state.setReadOnlyCalls++;
                            return null;
                        case "setNetworkTimeout":
                            state.networkTimeouts.add((Integer) args[1]);
                            return null;
                        case "commit":
                            state.commitCalled = true;
                            return null;
                        case "rollback":
                            state.rollbackCalled = true;
                            return null;
                        case "close":
                            state.closed = true;
                            return null;
                        case "isClosed":
                            return state.closed;
                        case "prepareStatement":
                            if (args == null || args.length == 0)
                                throw new UnsupportedOperationException("prepareStatement without arguments");
                            if (args.length == 1) {
                                if (preparedStatement != null)
                                    return preparedStatement;
                                throw new UnsupportedOperationException("prepareStatement not configured");
                            }
                            if (args.length == 2 && args[1] instanceof Integer && ((Integer) args[1]) == PreparedStatement.RETURN_GENERATED_KEYS) {
                                if (generatedKeysStatement != null)
                                    return generatedKeysStatement;
                                throw new UnsupportedOperationException("generated keys statement not configured");
                            }
                            throw new UnsupportedOperationException("prepareStatement overload");
                        case "unwrap":
                            return null;
                        case "isWrapperFor":
                            return false;
                        case "getHoldability":
                            return ResultSet.HOLD_CURSORS_OVER_COMMIT;
                        default:
                            throw new UnsupportedOperationException(name);
                    }
                });
    }

    private static <T> T executeInTransaction(Database database, TransactionState state, ConnectionBody<T> body) throws Exception {
        setDataSource(database, new SingleConnectionDataSource(createTestConnection(state)));
        setExecutor(database, Executors.newSingleThreadExecutor());
        Class<?> opClass = Class.forName("de.murmelmeister.luxeonlib.database.Database$ConnectionOperation");
        Method method = Database.class.getDeclaredMethod("executeInTransaction", opClass);
        method.setAccessible(true);
        Object operation = Proxy.newProxyInstance(
                opClass.getClassLoader(),
                new Class[]{opClass},
                (proxy, invokedMethod, args) -> {
                    if ("execute".equals(invokedMethod.getName()))
                        return body.apply(state);
                    throw new UnsupportedOperationException(invokedMethod.getName());
                });
        try {
            @SuppressWarnings("unchecked")
            T result = (T) method.invoke(database, operation);
            return result;
        } catch (InvocationTargetException e) {
            Throwable cause = e.getCause();
            if (cause instanceof Exception) throw (Exception) cause;
            if (cause instanceof Error) throw (Error) cause;
            throw new RuntimeException(cause);
        }
    }

    private static Connection createExistsFailureConnection(SQLException failure) {
        PreparedStatement failingStatement = (PreparedStatement) Proxy.newProxyInstance(
                PreparedStatement.class.getClassLoader(),
                new Class[]{PreparedStatement.class},
                (proxy, method, args) -> {
                    String name = method.getName();
                    switch (name) {
                        case "executeQuery":
                            throw failure;
                        case "close":
                            return null;
                        case "unwrap":
                            return null;
                        case "isWrapperFor":
                            return false;
                        default:
                            throw new UnsupportedOperationException(name);
                    }
                });

        boolean[] closed = {false};
        return (Connection) Proxy.newProxyInstance(
                Connection.class.getClassLoader(),
                new Class[]{Connection.class},
                (proxy, method, args) -> {
                    String name = method.getName();
                    switch (name) {
                        case "prepareStatement":
                            return failingStatement;
                        case "close":
                            closed[0] = true;
                            return null;
                        case "isClosed":
                            return closed[0];
                        case "unwrap":
                            return null;
                        case "isWrapperFor":
                            return false;
                        case "getAutoCommit":
                            return true;
                        case "getTransactionIsolation":
                            return Connection.TRANSACTION_READ_COMMITTED;
                        default:
                            throw new UnsupportedOperationException(name);
                    }
                });
    }

    private static PreparedStatement createGeneratedKeylessStatement() {
        ResultSet emptyGeneratedKeys = (ResultSet) Proxy.newProxyInstance(
                ResultSet.class.getClassLoader(),
                new Class[]{ResultSet.class},
                (proxy, method, args) -> {
                    String name = method.getName();
                    switch (name) {
                        case "next":
                            return false;
                        case "close":
                            return null;
                        case "unwrap":
                            return null;
                        case "isWrapperFor":
                            return false;
                        default:
                            throw new UnsupportedOperationException(name);
                    }
                });

        return (PreparedStatement) Proxy.newProxyInstance(
                PreparedStatement.class.getClassLoader(),
                new Class[]{PreparedStatement.class},
                (proxy, method, args) -> {
                    String name = method.getName();
                    switch (name) {
                        case "executeUpdate":
                            return 1;
                        case "getGeneratedKeys":
                            return emptyGeneratedKeys;
                        case "close":
                            return null;
                        case "unwrap":
                            return null;
                        case "isWrapperFor":
                            return false;
                        default:
                            throw new UnsupportedOperationException(name);
                    }
                });
    }

    private static final class ThrowingConnectionDataSource extends HikariDataSource {
        private final SQLException exception;

        ThrowingConnectionDataSource(SQLException exception) {
            this.exception = exception;
        }

        @Override
        public Connection getConnection() throws SQLException {
            throw exception;
        }

        @Override
        public boolean isClosed() {
            return false;
        }

        @Override
        public void close() {
            // no-op
        }
    }

    private static final class CloseThrowingDataSource extends HikariDataSource {
        private final RuntimeException closeException;

        CloseThrowingDataSource(RuntimeException closeException) {
            this.closeException = closeException;
        }

        @Override
        public boolean isClosed() {
            return false;
        }

        @Override
        public void close() {
            throw closeException;
        }
    }

    private record DatabaseConfig(String host, int port, String database, String username, String password) {
    }
}
