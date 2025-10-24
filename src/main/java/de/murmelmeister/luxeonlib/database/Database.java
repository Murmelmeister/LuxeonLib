package de.murmelmeister.luxeonlib.database;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Pattern;

public class Database {
    private final Logger logger = LoggerFactory.getLogger(Database.class);
    private final ReadWriteLock lock = new ReentrantReadWriteLock(true);
    private final ExecutorService executor = Executors.newFixedThreadPool(10);
    private volatile HikariDataSource dataSource;
    private static final Pattern NAME_PATTERN = Pattern.compile("^[a-zA-Z0-9_]+");

    // Threshold in milliseconds for slow queries.
    private static final long SLOW_QUERY_THRESHOLD_MS = 1000;

    /**
     * Establishes a connection to the database using the properties defined in the specified file.
     * This method creates a HikariConfig object from the given file and delegates the connection
     * setup to another method for further processing.
     *
     * @param propertyFileName The name of the file containing database configuration properties.
     *                         Must not be null or empty. The file should include necessary
     *                         configurations such as JDBC URL, username, and password.
     */
    public void connect(String propertyFileName) {
        HikariConfig config = new HikariConfig(propertyFileName);
        connect(config);
    }

    /**
     * Establishes a connection to the database using the provided properties.
     * Creates a HikariConfig object from the properties and delegates the connection setup
     * to another method.
     *
     * @param properties The property object containing database configuration parameters.
     *                   Must not be null and should include the necessary information such
     *                   as JDBC URL, username, and password.
     */
    public void connect(Properties properties) {
        HikariConfig config = new HikariConfig(properties);
        connect(config);
    }

    /**
     * Establishes a connection to the database using the specified URL, username, and password.
     * Creates a HikariConfig object configured with the provided parameters and delegates
     * the connection setup to another method.
     *
     * @param url      The JDBC URL of the database. Must not be null or empty.
     * @param username The username for the database connection. Must not be null or empty.
     * @param password The password for the database connection. Must not be null or empty.
     */
    public void connect(String url, String username, String password) {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(url);
        config.setUsername(username);
        config.setPassword(password);
        connect(config);
    }

    /**
     * Establishes a connection to the database using the provided HikariConfig object.
     * This method attempts to acquire a write lock before closing any existing connection
     * and initializing a new HikariDataSource with the given configuration.
     *
     * @param config The HikariConfig object containing the database connection settings. Must not be null.
     * @throws IllegalStateException If the write lock cannot be acquired within the defined timeout,
     *                               or if there is an issue during the connection setup process.
     */
    private void connect(HikariConfig config) {
        try {
            if (!lock.writeLock().tryLock(10, TimeUnit.SECONDS))
                throw new IllegalStateException("Could not acquire write lock");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while trying to acquire write lock", e);
        }

        try {
            if (dataSource != null && !dataSource.isClosed())
                dataSource.close();

            dataSource = new HikariDataSource(config);
            logger.info("Database connection established successfully. URL: {}", dataSource.getJdbcUrl());
        } catch (Exception e) {
            throw new IllegalStateException("Could not connect to database", e);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Closes the database connection and releases associated resources.
     * This method ensures proper cleanup of the database connection, thread pool,
     * and other associated components. It also manages concurrency with a write lock.
     * <p>
     * The method attempts to: <p>
     * 1. Acquire a write lock to ensure thread-safe closure of resources. <p>
     * 2. Close and nullify the data source if it is not already closed. <p>
     * 3. Shut down the thread pool executor, ensuring all tasks are completed or terminated.
     * <p>
     * Exceptions are handled to maintain the application's stability, logging,
     * and propagating issues where necessary.
     *
     * @throws IllegalStateException If the write lock cannot be acquired within the timeout,
     *                               or if there are errors while closing the data source.
     */
    public void disconnect() {
        try {
            if (!lock.writeLock().tryLock(10, TimeUnit.SECONDS))
                throw new IllegalStateException("Could not acquire write lock");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while trying to acquire write lock", e);
        }

        try {
            if (dataSource != null && !dataSource.isClosed()) {
                dataSource.close();
                dataSource = null;
                logger.info("Database connection closed successfully");
            }
        } catch (Exception e) {
            throw new IllegalStateException("Could not close database connection", e);
        } finally {
            lock.writeLock().unlock();
        }

        if (!executor.isShutdown()) {
            executor.shutdown();
            try {
                if (!executor.awaitTermination(10, TimeUnit.SECONDS))
                    executor.shutdownNow();
            } catch (InterruptedException e) {
                executor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * Executes an update operation on the database using the provided SQL statement
     * and parameter processor within a transactional context.
     *
     * @param sql        The SQL update query to be executed
     * @param parameters The processor used to prepare the statement with necessary parameters
     * @return The number of rows affected by the update operation
     */
    public int update(String sql, ParameterProcessor parameters) {
        return executeInTransaction(connection -> {
            try (PreparedStatement statement = getPreparedStatement(connection, sql, parameters)) {
                return statement.executeUpdate();
            }
        });
    }

    /**
     * Executes the given SQL update statement within a transaction and retrieves the generated key for the updated record.
     *
     * @param sql        The SQL update statement to be executed
     * @param parameters The parameter processor for preparing the SQL statement
     * @return The generated key for the updated record
     */
    public long updateAndGetGeneratedKeys(String sql, ParameterProcessor parameters) {
        return executeInTransaction(connection -> {
            try (PreparedStatement statement = getGeneratedKeysStatement(connection, sql, parameters)) {
                int affectedRows = statement.executeUpdate();
                if (affectedRows < 1)
                    throw new SQLException("No rows affected");

                try (ResultSet resultSet = statement.getGeneratedKeys()) {
                    if (!resultSet.next())
                        throw new SQLException("No generated keys returned");
                    return resultSet.getLong(1);
                }
            }
        });
    }

    /**
     * Executes a database update operation using a callable SQL statement within a transaction.
     *
     * @param sql        The SQL string for the callable statement to execute
     * @param parameters An object implementing ParameterProcessor to process and set parameters for the callable statement
     * @return The number of rows affected by the update operation
     */
    public int updateCallable(String sql, ParameterProcessor parameters) {
        return executeInTransaction(connection -> {
            try (CallableStatement statement = getCallableStatement(connection, sql, parameters)) {
                return statement.executeUpdate();
            }
        });
    }

    /**
     * Executes a batch update using the provided SQL query and parameter processor,
     * and returns an array of update counts indicating the number of rows affected by each batch statement.
     *
     * @param sql        The SQL query to be executed in batch; must not be null or empty
     * @param parameters A processor that applies parameters to the SQL query for batch execution; must not be null
     * @return An array of update counts containing the number of rows affected for each statement in the batch
     */
    public int[] updateBatch(String sql, ParameterProcessor parameters) {
        return executeInTransaction(connection -> {
            try (PreparedStatement statement = getBatchStatement(connection, sql, parameters)) {
                return statement.executeBatch();
            }
        });
    }

    /**
     * Executes the provided SQL query and processes the resulting {@code ResultSet}
     * using the given {@link ResultSetProcessor}. If the query does not yield any results,
     * returns the specified fallback value.
     *
     * @param <T>        The type of the result object returned by the query.
     * @param sql        The SQL query string to be executed.
     * @param fallback   The fallback value to be returned if the query does not yield any results.
     * @param processor  The {@link ResultSetProcessor} that processes the {@code ResultSet}
     *                   and transforms it into the desired result type.
     * @param parameters The {@link ParameterProcessor} used to set parameters in the prepared statement.
     * @return The processed result from the query if it yields a result, or the fallback value
     * if no results are found.
     * @throws RuntimeException If an SQL exception occurs during query execution.
     */
    public <T> T query(String sql, T fallback, ResultSetProcessor<T> processor, ParameterProcessor parameters) {
        long startTime = System.nanoTime();
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = getPreparedStatement(connection, sql, parameters);
             ResultSet resultSet = statement.executeQuery()) {
            if (resultSet.next())
                return processor.process(resultSet);
            return fallback;
        } catch (SQLException e) {
            logger.error("Failed to execute query", e);
            throw new RuntimeException("Failed to execute query", e);
        } finally {
            long durationMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime);
            if (durationMs > SLOW_QUERY_THRESHOLD_MS)
                logger.warn("Slow database query [{}] executed in {} ms", sql, durationMs);
        }
    }

    /**
     * Executes a callable SQL query and processes the result using a provided processor. If no result is found, a fallback value is returned.
     *
     * @param sql        The SQL query to execute as a callable statement
     * @param fallback   The value to return if no rows are found in the result set
     * @param processor  A {@code ResultSetProcessor} to process the {@code ResultSet} and convert it into the desired type
     * @param parameters A {@code ParameterProcessor} for setting parameters on the {@code CallableStatement}
     * @param <T>        The type of the result returned by the processor
     * @return An object of type {@code T} representing the processed result, or the fallback value if no rows were found
     * @throws RuntimeException If the query fails to execute due to an {@code SQLException}
     */
    public <T> T queryCallable(String sql, T fallback, ResultSetProcessor<T> processor, ParameterProcessor parameters) {
        long startTime = System.nanoTime();
        try (Connection connection = dataSource.getConnection();
             CallableStatement statement = getCallableStatement(connection, sql, parameters);
             ResultSet resultSet = statement.executeQuery()) {
            if (resultSet.next())
                return processor.process(resultSet);
            return fallback;
        } catch (SQLException e) {
            logger.error("Failed to execute query", e);
            throw new RuntimeException("Failed to execute query", e);
        } finally {
            long durationMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime);
            if (durationMs > SLOW_QUERY_THRESHOLD_MS)
                logger.warn("Slow database query [{}] executed in {} ms", sql, durationMs);
        }
    }

    /**
     * Executes an asynchronous query using the provided SQL statement, fallback value,
     * result set processor, and parameter processor.
     *
     * @param <T>        The type of the result returned by the query
     * @param sql        The SQL query to execute
     * @param fallback   The fallback value to return in case of query failure
     * @param processor  The result set processor for handling query results
     * @param parameters The parameter processor for managing query parameters
     * @return A CompletableFuture representing the result of the asynchronous query
     */
    public <T> CompletableFuture<T> queryAsync(String sql, T fallback, ResultSetProcessor<T> processor, ParameterProcessor parameters) {
        return CompletableFuture.supplyAsync(() -> query(sql, fallback, processor, parameters), executor);
    }

    /**
     * Executes a query asynchronously, processing the result set to return the desired output.
     *
     * @param <T>        The type of the result.
     * @param sql        The SQL query to execute.
     * @param fallback   The fallback value to return in case of an error.
     * @param processor  The processor that converts the ResultSet into the desired type.
     * @param parameters The processor that manages SQL parameters for the query.
     * @return A CompletableFuture containing the result of the processed query.
     */
    public <T> CompletableFuture<T> queryCallableAsync(String sql, T fallback, ResultSetProcessor<T> processor, ParameterProcessor parameters) {
        return CompletableFuture.supplyAsync(() -> queryCallable(sql, fallback, processor, parameters), executor);
    }

    /**
     * Executes a SQL query and returns a list of results processed by the provided ResultSetProcessor.
     *
     * @param <T>        The type of elements in the list that will be returned.
     * @param sql        The SQL query to be executed.
     * @param processor  The processor used to convert each row of the ResultSet into an object of type T.
     * @param parameters The processor responsible for setting the parameters in the prepared statement.
     * @return A list of objects of type T representing the processed rows of the query result.
     * @throws RuntimeException If a SQL exception occurs during query execution.
     */
    public <T> List<T> queryList(String sql, ResultSetProcessor<T> processor, ParameterProcessor parameters) {
        long startTime = System.nanoTime();
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = getPreparedStatement(connection, sql, parameters);
             ResultSet resultSet = statement.executeQuery()) {
            List<T> list = new ArrayList<>();
            while (resultSet.next())
                list.add(processor.process(resultSet));
            return list;
        } catch (SQLException e) {
            logger.error("Failed to execute query", e);
            throw new RuntimeException("Failed to execute query", e);
        } finally {
            long durationMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime);
            if (durationMs > SLOW_QUERY_THRESHOLD_MS)
                logger.warn("Slow database query [{}] executed in {} ms", sql, durationMs);
        }
    }

    /**
     * Executes the given SQL callable statement, processes the result set, and returns a list of objects.
     *
     * @param <T>        The type of objects to be returned in the list
     * @param sql        The SQL callable statement to execute
     * @param processor  The result set processor used to map the result set rows to objects of type T
     * @param parameters The parameter processor used to bind parameters to the statement
     * @return A list of objects of type T created by processing the result set
     * @throws RuntimeException If an SQL exception occurs while executing the query
     */
    public <T> List<T> queryListCallable(String sql, ResultSetProcessor<T> processor, ParameterProcessor parameters) {
        long startTime = System.nanoTime();
        try (Connection connection = dataSource.getConnection();
             CallableStatement statement = getCallableStatement(connection, sql, parameters);
             ResultSet resultSet = statement.executeQuery()) {
            List<T> list = new ArrayList<>();
            while (resultSet.next())
                list.add(processor.process(resultSet));
            return list;
        } catch (SQLException e) {
            logger.error("Failed to execute query", e);
            throw new RuntimeException("Failed to execute query", e);
        } finally {
            long durationMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime);
            if (durationMs > SLOW_QUERY_THRESHOLD_MS)
                logger.warn("Slow database query [{}] executed in {} ms", sql, durationMs);
        }
    }

    /**
     * Executes an asynchronous query to retrieve a list of results from the database.
     *
     * @param <T>        The type of the list elements to be retrieved from the database.
     * @param sql        The SQL query string to be executed.
     * @param processor  A processor that converts the result set into objects of type T.
     * @param parameters A processor that handles the parameters for the SQL query.
     * @return A CompletableFuture containing the list of results of type T.
     */
    public <T> CompletableFuture<List<T>> queryListAsync(String sql, ResultSetProcessor<T> processor, ParameterProcessor parameters) {
        return CompletableFuture.supplyAsync(() -> queryList(sql, processor, parameters), executor);
    }

    /**
     * Executes a SQL query asynchronously and processes the returned result set into a list of objects.
     *
     * @param sql        The SQL query to execute
     * @param processor  The processor used to convert each row of the result set into an object of type T
     * @param parameters The processor used to set the parameters for the SQL query
     * @param <T>        The type of objects in the resulting list
     * @return A CompletableFuture containing a list of objects of type T resulting from the query
     */
    public <T> CompletableFuture<List<T>> queryListCallableAsync(String sql, ResultSetProcessor<T> processor, ParameterProcessor parameters) {
        return CompletableFuture.supplyAsync(() -> queryListCallable(sql, processor, parameters), executor);
    }

    /**
     * Checks if a query, specified by the SQL string and processed parameters, returns any results.
     * Executes the given SQL query using a prepared statement and checks if the result set contains
     * at least one row.
     *
     * @param sql        The SQL query string to execute
     * @param parameters The {@code ParameterProcessor} used to process the parameters of the SQL query
     * @return {@code true} if the query result contains at least one row, {@code false} otherwise
     * @throws RuntimeException If a database access error occurs during the execution of the query
     */
    public boolean exists(String sql, ParameterProcessor parameters) {
        long startTime = System.nanoTime();
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = getPreparedStatement(connection, sql, parameters);
             ResultSet resultSet = statement.executeQuery()) {
            return resultSet.next();
        } catch (SQLException e) {
            logger.error("Failed to execute query", e);
            throw new RuntimeException("Failed to execute query", e);
        } finally {
            long durationMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime);
            if (durationMs > SLOW_QUERY_THRESHOLD_MS)
                logger.warn("Slow database query [{}] executed in {} ms", sql, durationMs);
        }
    }

    /**
     * Executes a callable SQL query to check if any results exist.
     *
     * @param sql        The SQL query to execute, which must be a callable statement
     * @param parameters An object that processes and sets the parameters for the callable statement
     * @return {@code true} if the query result contains at least one row, {@code false} otherwise
     * @throws RuntimeException If a database access error occurs during the execution of the query
     */
    public boolean existsCallable(String sql, ParameterProcessor parameters) {
        long startTime = System.nanoTime();
        try (Connection connection = dataSource.getConnection();
             CallableStatement statement = getCallableStatement(connection, sql, parameters);
             ResultSet resultSet = statement.executeQuery()) {
            return resultSet.next();
        } catch (SQLException e) {
            logger.error("Failed to execute query", e);
            throw new RuntimeException("Failed to execute query", e);
        } finally {
            long durationMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime);
            if (durationMs > SLOW_QUERY_THRESHOLD_MS)
                logger.warn("Slow database query [{}] executed in {} ms", sql, durationMs);
        }
    }

    /**
     * Checks asynchronously whether a record exists in the database based on the given SQL query and parameters.
     *
     * @param sql        The SQL query to check the existence of a record
     * @param parameters The processor for the parameters to be applied in the SQL query
     * @return A CompletableFuture which resolves to a Boolean indicating whether a record exists (true) or not (false)
     */
    public CompletableFuture<Boolean> existsAsync(String sql, ParameterProcessor parameters) {
        return CompletableFuture.supplyAsync(() -> exists(sql, parameters), executor);
    }

    /**
     * Asynchronously checks for the existence of a callable statement based on the given SQL query
     * and parameter processor.
     *
     * @param sql        The SQL query to evaluate for the callable statement
     * @param parameters The processor to handle the parameters for the callable query
     * @return A CompletableFuture containing a Boolean value indicating whether the callable
     * statement exists (true) or not (false)
     */
    public CompletableFuture<Boolean> existsCallableAsync(String sql, ParameterProcessor parameters) {
        return CompletableFuture.supplyAsync(() -> existsCallable(sql, parameters), executor);
    }

    /**
     * Retrieves the next auto-increment value for a specified database table.
     * The method queries the database to fetch the auto-increment value for a table
     * if it exists. An exception is thrown if the table name is invalid or the query fails.
     *
     * @param tableName The name of the database table for which to retrieve the auto-increment value.
     *                  The table name must consist only of alphanumeric characters and/or underscores.
     * @return A CompletableFuture that resolves to the next auto-increment value of the specified table or
     * null if the table does not exist.
     * @throws IllegalArgumentException If the table name contains invalid characters.
     * @throws RuntimeException         If there is an issue executing the database query.
     */
    public CompletableFuture<Long> getAutoIncrement(String tableName) {
        // Validate table name: Only alphanumeric and underscore characters are allowed.
        if (!NAME_PATTERN.matcher(tableName).matches())
            throw new IllegalArgumentException("Invalid table name: " + tableName);

        String sql = "SHOW TABLE STATUS LIKE ?";
        return CompletableFuture.supplyAsync(() -> {
            try (Connection connection = dataSource.getConnection();
                 PreparedStatement statement = getPreparedStatement(connection, sql, (stmt) -> stmt.setString(1, tableName));
                 ResultSet resultSet = statement.executeQuery()) {
                if (resultSet.next())
                    return resultSet.getLong("Auto_increment");
                else {
                    logger.error("Table not found: {}", tableName);
                    return null;
                }
            } catch (SQLException e) {
                logger.error("Failed to retrieve auto-increment value for table [{}]", tableName);
                throw new RuntimeException("Failed to execute query", e);
            }
        }, executor);
    }

    /**
     * Creates a new table in the database with the specified name and columns.
     *
     * @param name    The name of the table to be created. Must follow naming restrictions allowing only alphanumeric characters and underscores.
     * @param columns The column definitions for the table, formatted as a comma-separated list.
     * @return The number of rows affected by the execution of the SQL statement.
     * @throws IllegalArgumentException If the table name is invalid.
     */
    public int createTable(String name, String columns) {
        // Validate table name: Only alphanumeric and underscore characters are allowed.
        if (!NAME_PATTERN.matcher(name).matches())
            throw new IllegalArgumentException("Invalid table name: " + name);

        String sql = "CREATE TABLE IF NOT EXISTS " + name + "(" + columns + ")";
        return update(sql, statement -> {
        });
    }

    /**
     * Executes a database operation within a transactional context. This method manages the transaction lifecycle,
     * including beginning, committing, and rolling back the transaction if an exception occurs. It also ensures the
     * connection's state is properly restored after the operation.
     *
     * @param <T>       The type of result returned by the operation
     * @param operation The database operation to execute, represented as a {@code ConnectionOperation<T>}
     * @return The result of the database operation
     * @throws RuntimeException If the database operation fails or an error occurs while managing the transaction
     */
    private <T> T executeInTransaction(ConnectionOperation<T> operation) {
        long startTime = System.nanoTime();
        Connection connection = null;
        boolean previousAutoCommit = true;
        int previousIsolation = Connection.TRANSACTION_READ_COMMITTED;

        try {
            connection = dataSource.getConnection();

            previousAutoCommit = connection.getAutoCommit();
            previousIsolation = connection.getTransactionIsolation();

            if (previousAutoCommit) connection.setAutoCommit(false);
            int desiredIsolation = Connection.TRANSACTION_SERIALIZABLE;
            if (previousIsolation != desiredIsolation)
                connection.setTransactionIsolation(desiredIsolation);

            connection.setReadOnly(false);
            connection.setNetworkTimeout(executor, 30_000); // Set network timeout to 30 seconds

            T result = operation.execute(connection);
            connection.commit();
            return result;
        } catch (SQLException | RuntimeException e) {
            if (connection != null) {
                try {
                    if (!connection.getAutoCommit())
                        connection.rollback();
                } catch (SQLException rollbackException) {
                    logger.error("Failed to rollback transaction", rollbackException);
                }
            }
            throw new RuntimeException("Failed to execute database operation", e);
        } finally {
            long durationMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime);
            if (durationMs > SLOW_QUERY_THRESHOLD_MS)
                logger.warn("Slow database operation executed in {} ms", durationMs);

            if (connection != null) {
                try {
                    if (connection.getAutoCommit() != previousAutoCommit)
                        connection.setAutoCommit(previousAutoCommit);

                    if (connection.getTransactionIsolation() != previousIsolation)
                        connection.setTransactionIsolation(previousIsolation);

                    connection.setReadOnly(false);
                    connection.setNetworkTimeout(executor, 0);
                } catch (SQLException e) {
                    logger.error("Failed to reset database connection", e);
                } finally {
                    try {
                        connection.close();
                    } catch (SQLException e) {
                        logger.error("Failed to close database connection", e);
                    }
                }
            }
        }
    }

    /**
     * Prepares a CallableStatement using the provided database connection and SQL string,
     * then applies parameter operations to the statement.
     *
     * @param connection The active database connection used to prepare the CallableStatement
     * @param sql        The SQL string used to create the CallableStatement
     * @param parameters The operations to apply to the CallableStatement for parameter configuration
     * @return The CallableStatement with the applied parameter operations
     * @throws SQLException If a database access error occurs or the SQL string is invalid
     */
    private CallableStatement getCallableStatement(Connection connection, String sql, ParameterProcessor parameters) throws SQLException {
        CallableStatement statement = connection.prepareCall(sql);
        parameters.execute(statement);
        return statement;
    }

    /**
     * Prepares a batch SQL statement using the provided connection, SQL query, and parameter operation.
     * Remember to use {@code statement.addBatch()} to add each batch statement to the prepared statement.
     *
     * @param connection The database connection to be used for preparing the statement.
     * @param sql        The SQL query string to prepare as a batch statement.
     * @param parameters A functional operation to apply parameters to the prepared statement.
     * @return The prepared batch SQL statement after applying the parameter operation.
     * @throws SQLException If an error occurs while preparing the statement or applying the parameters.
     */
    private PreparedStatement getBatchStatement(Connection connection, String sql, ParameterProcessor parameters) throws SQLException {
        PreparedStatement statement = connection.prepareStatement(sql);
        parameters.execute(statement);
        return statement;
    }

    /**
     * Prepares a {@link PreparedStatement} capable of returning generated keys using the provided SQL query.
     * A given parameter operation is applied to the prepared statement before returning it.
     *
     * @param connection The database connection to be used for preparing the {@link PreparedStatement}.
     *                   Must not be null and should represent a valid, open connection.
     * @param sql        The SQL query string to prepare the {@link PreparedStatement} with.
     *                   Must not be null or empty.
     * @param parameters An operation defining how to set parameters and operate on the {@link PreparedStatement}.
     *                   Must not be null and should properly handle the {@link PreparedStatement}.
     * @return The prepared and parameterized {@link PreparedStatement} ready for execution,
     * configured to return generated keys.
     * @throws SQLException If an error occurs while preparing or handling the {@link PreparedStatement}.
     */
    private PreparedStatement getGeneratedKeysStatement(Connection connection, String sql, ParameterProcessor parameters) throws SQLException {
        PreparedStatement statement = connection.prepareStatement(sql, PreparedStatement.RETURN_GENERATED_KEYS);
        parameters.execute(statement);
        return statement;
    }

    /**
     * Prepares a {@link PreparedStatement} using the provided SQL and connection, and applies a parameter
     * operation on the prepared statement before returning it.
     *
     * @param connection The database connection to be used for preparing the {@link PreparedStatement}.
     *                   Must not be null and should represent a valid, open connection.
     * @param sql        The SQL query string to prepare the {@link PreparedStatement} with.
     *                   Must not be null or empty.
     * @param parameters An operation defining how to set parameters and execute the {@link PreparedStatement}.
     *                   Must not be null and should properly handle the {@link PreparedStatement}.
     * @return The prepared and parameterized {@link PreparedStatement} ready for execution.
     * @throws SQLException If an error occurs while preparing or handling the {@link PreparedStatement}.
     */
    private PreparedStatement getPreparedStatement(Connection connection, String sql, ParameterProcessor parameters) throws SQLException {
        PreparedStatement statement = connection.prepareStatement(sql);
        parameters.execute(statement);
        return statement;
    }

    /**
     * Retrieves the current data source used for database operations.
     * The data source is typically configured using HikariCP for connection pooling
     * and efficient database resource management.
     *
     * @return A {@link HikariDataSource} instance representing the active database connection pool.
     * Returns null if the data source is not initialized or has been closed.
     */
    public HikariDataSource getDataSource() {
        return dataSource;
    }

    /**
     * Retrieves the thread pool executor service associated with the database operations.
     * This executor service is used to manage and execute asynchronous tasks related to
     * database interactions, providing efficient thread management.
     *
     * @return The {@link ExecutorService} instance used for managing database operation tasks.
     */
    public ExecutorService getExecutor() {
        return executor;
    }

    /**
     * Generates a SQL query string for creating a stored procedure in the database.
     * The procedure is created only if it does not already exist. The query includes
     * the procedure name, its parameters, and the procedure body.
     *
     * @param name       The name of the procedure. Must match the required naming pattern.
     *                   If the name is invalid, an {@link IllegalArgumentException} is thrown.
     * @param parameters A string specifying the parameters of the procedure, including types.
     *                   This string is directly appended to the generated SQL.
     * @param body       The body of the procedure, containing the SQL logic to be executed
     *                   within the procedure.
     * @return A SQL string representing the creation of the specified stored procedure.
     * @throws IllegalArgumentException If the procedure name does not match the expected pattern.
     */
    public static String getProcedureQuery(String name, String parameters, String body) {
        // Validate procedure name: Only alphanumeric and underscore characters are allowed.
        if (!NAME_PATTERN.matcher(name).matches())
            throw new IllegalArgumentException("Invalid procedure name: " + name);

        return "CREATE PROCEDURE IF NOT EXISTS " + name + "(" + parameters + ")\n BEGIN\n    " + body + " \nEND;";
    }

    /**
     * A functional interface representing a database operation that executes a specific task
     * using a provided {@link Connection} object. This operation is designed to encapsulate
     * reusable database logic and handle SQL-related exceptions within a connection context.
     *
     * @param <T> The type of result produced by the operation executed within the database connection
     */
    @FunctionalInterface
    private interface ConnectionOperation<T> {
        /**
         * Executes a database operation using the provided {@link Connection} and returns the result of type {@code T}.
         * This method encapsulates the logic for performing a specific task within the scope of the database connection.
         *
         * @param connection The {@link Connection} object to be used for executing the operation. Must not be null.
         * @return An object of type {@code T}, representing the result of the executed database operation.
         * @throws SQLException If a database access error occurs or the operation fails.
         */
        T execute(Connection connection) throws SQLException;
    }
}
