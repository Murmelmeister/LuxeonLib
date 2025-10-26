package de.murmelmeister.luxeonlib.database;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * A functional interface designed for processing a {@link ResultSet} obtained from database queries.
 * This interface provides a single abstract method to handle the transformation or extraction
 * of data from a {@link ResultSet} into a desired type {@code T}.
 *
 * @param <T> The type of object that will result from processing the {@link ResultSet}
 */
@FunctionalInterface
public interface ResultSetProcessor<T> {
    /**
     * Processes the given {@link ResultSet} and transforms it into an object of type {@code T}.
     * This method allows extraction and conversion of database query results into a desired data structure.
     *
     * @param resultSet The {@link ResultSet} obtained from executing a database query. Must not be null.
     * @return An object of type {@code T}, representing the processed data extracted from the {@link ResultSet}.
     * @throws SQLException If an SQL operation fails while processing the {@link ResultSet}.
     */
    T process(ResultSet resultSet) throws SQLException;
}
