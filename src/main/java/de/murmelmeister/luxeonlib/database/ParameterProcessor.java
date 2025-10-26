package de.murmelmeister.luxeonlib.database;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Represents a functional interface for processing parameters in a {@link PreparedStatement}.
 * This interface provides a method to execute parameter setting logic for prepared statements
 * based on the provided parameters.
 * <p>
 * The implementation of this interface is expected to handle the parameter mapping
 * to the {@link PreparedStatement} appropriately and can throw an {@link SQLException}
 * if any database access error occurs.
 */
@FunctionalInterface
public interface ParameterProcessor {
    /**
     * Executes a parameter setting operation on the provided {@link PreparedStatement}.
     * Implementations should define the logic for mapping parameters to the prepared statement.
     * This method may throw an {@link SQLException} if an error occurs while accessing the database.
     *
     * @param statement The {@link PreparedStatement} on which parameter mapping and execution logic is applied
     * @throws SQLException If a database access error occurs
     */
    void execute(PreparedStatement statement) throws SQLException;
}
