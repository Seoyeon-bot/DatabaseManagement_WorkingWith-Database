package rdb;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Stream;

import expression.ParsingException;

/**
 * A {@code RelationalDatabase} is a collection of {@code Relation}s.
 * 
 * @author Jeong-Hyon Hwang (jhh@cs.albany.edu)
 */
public class RelationalDatabase {

	/**
	 * The name of this {@code RelationalDatabase}.
	 */
	String databaseName;

	/**
	 * The {@code Relation}s in this {@code RelationalDatabase}.
	 */
	Map<String, Relation> relations = new TreeMap<String, Relation>();

	/**
	 * Constructs a table/relation in this {@code RelationalDatabase}.
	 * 
	 * @param tableName
	 *            the name of the table/relation
	 * @return the {@code RelationSchema} of the table
	 */
	public RelationSchema createTable(String tableName) {
		var schema = new RelationSchema();
		relations.put(tableName, new Relation(schema));
		return schema;
	}

	/**
	 * Constructs a {@code RelationalDatabase}.
	 * 
	 * @param databaseName
	 *            the name of the {@code RelationalDatabase}
	 */
	public RelationalDatabase(String databaseName) {
		this.databaseName = databaseName;
	}

	/**
	 * Returns a string representation of this {@code RelationalDatabase}.
	 * 
	 * @return a string representation of this {@code RelationalDatabase}
	 */
	@Override
	public String toString() {
		return databaseName + relations;
	}

	/**
	 * Constructs a {@code Tuple} containing the specified attribute values and then inserts it to the specified table
	 * 
	 * @param tableName
	 *            the name of the table
	 * @param attributeValues
	 *            attribute values
	 * @return the constructed {@code Tuple}
	 */
	public Tuple addTuple(String tableName, Object... attributeValues) {
		return relations.get(tableName).addTuple(attributeValues);
	}

	/**
	 * Returns a {@code Stream} of {@code Tuple}s that contain the specified attributes and are generated from the
	 * specified tables.
	 * 
	 * @param attributeDefinitions
	 *            the definitions of attributes
	 * @param tableNames
	 *            the names of the tables
	 * @return a {@code Stream} of {@code Tuple}s that contain the specified attributes and are generated from the
	 *         specified tables
	 * @throws ParsingException
	 *             if an error occurs while parsing the expressions
	 */
	public Stream<Tuple> select(String attributeDefinitions, String tableNames) throws ParsingException {
		return query(attributeDefinitions, tableNames, null, null).stream();
	}

	/**
	 * Returns a {@code Stream} of {@code Tuple}s that contain the specified attributes and are generated from the
	 * specified tables using the specified predicate.
	 * 
	 * @param attributeDefinitions
	 *            the definitions of attributes
	 * @param tableNames
	 *            the names of the tables
	 * @param predicate
	 *            a predicate
	 * @return a {@code Stream} of {@code Tuple}s that contain the specified attributes and are generated from the
	 *         specified tables using the specified predicate
	 * @throws ParsingException
	 *             if an error occurs while parsing the expressions
	 */
	public Stream<Tuple> select(String attributeDefinitions, String tableNames, String predicate)
			throws ParsingException {
		return query(attributeDefinitions, tableNames, predicate, null).stream();
	}

	/**
	 * Returns a {@code Stream} of {@code Tuple}s that contain the specified attributes and are generated from the
	 * specified tables using the specified grouping attributes.
	 * 
	 * @param attributeDefinitions
	 *            the definitions of attributes
	 * @param tableNames
	 *            the names of the tables
	 * @param groupingAttributes
	 *            the names of the attributes used for grouping
	 * @return a {@code Stream} of {@code Tuple}s that contain the specified attributes and are generated from the
	 *         specified tables using the specified grouping attributes
	 * @throws ParsingException
	 *             if an error occurs while parsing the expressions
	 */
	public Stream<Tuple> selectGroupBy(String attributeDefinitions, String tableNames, String groupingAttributes)
			throws ParsingException {
		return query(attributeDefinitions, tableNames, null, groupingAttributes).stream();
	}

	/**
	 * Returns a {@code Stream} of {@code Tuple}s that contain the ite and grouping attributes.
	 * 
	 * @param attributeDefinitions
	 *            the definitions of attributes
	 * @param tableNames
	 *            the names of the tables
	 * @param predicate
	 *            a predicate
	 * @param groupingAttributes
	 *            the names of the attributes used for grouping
	 * @return a {@code Stream} of {@code Tuple}s that contain the specified attributes and are generated from the
	 *         specified tables using the specified predicate and grouping attributes
	 * @throws ParsingException
	 *             if an error occurs while parsing the expressions
	 */
	public Stream<Tuple> selectGroupBy(String attributeDefinitions, String tableNames, String predicate,
			String groupingAttributes) throws ParsingException {
		return query(attributeDefinitions, tableNames, predicate, groupingAttributes).stream();
	}

	/**
	 * Returns an {@code Operator} that provides {@code Tuple}s containing the specified attributes and generated using
	 * the specified tables, predicate, and grouping attributes.
	 * 
	 * @param attributeDefinitions
	 *            a string representation of attribute definitions
	 * @param tableNames
	 *            a string representation of table names
	 * @param predicate
	 *            a predicate
	 * @param groupingAttributes
	 *            a string representation of the attributes used for grouping
	 * @return an {@code Operator} that provides {@code Tuple}s containing the specified attributes and generated using
	 *         the specified tables predicate, and grouping attributes
	 * @throws ParsingException
	 *             if an error occurs while parsing the expressions
	 */
	private Operator query(String attributeDefinitions, String tableNames, String predicate, String groupingAttributes)
			throws ParsingException {
		return query(Arrays.stream(attributeDefinitions.split(",")).map(s -> s.trim()).toList(),
				Arrays.stream(tableNames.split("natural join")).map(s -> s.trim()).toList(), predicate,
				groupingAttributes == null ? null
						: Arrays.stream(groupingAttributes.split(",")).map(s -> s.trim()).toList());
	}

	/**
	 * Returns an {@code Operator} that provides {@code Tuple}s that contain the specified attributes and that are
	 * generated using the specified tables, predicate, and grouping attributes.
	 * 
	 * @param attributeDefinitions
	 *            a {@code List} of attributes
	 * @param tableNames
	 *            a {@code List} of table names
	 * @param predicate
	 *            a predicate
	 * @param groupingAttributes
	 *            the names of the attributes used for grouping
	 * @return a {@code Stream} of {@code Tuple}s that contain the specified attributes and that are generated using the
	 *         specified tables predicate, and grouping attributes
	 * @throws ParsingException
	 *             if an error occurs while parsing the expressions
	 */
	private Operator query(List<String> attributeDefinitions, List<String> tableNames, String predicate,
			List<String> groupingAttributes) throws ParsingException {
		Operator o = new Scan(relations.get(tableNames.get(0)));
		for (int i = 1; i < tableNames.size(); i++)
			o = new NaturalJoin(o, relations.get(tableNames.get(i)));
		if (predicate != null)
			o = new Selection(o, predicate);
		if (groupingAttributes != null)
			return new Aggregate(o, groupingAttributes, attributeDefinitions);
		if (this.hasAggregateFunctions(attributeDefinitions))
			return new Aggregate(o, null, attributeDefinitions);
		if (attributeDefinitions == null || attributeDefinitions.size() == 1 && "*".equals(attributeDefinitions.get(0)))
			return o;
		return new Projection(o, attributeDefinitions);
	}

	/**
	 * Generates {@code Tuple}s that contain the specified attributes using the specified tables and then stores these
	 * {@code Tuple}s in a temporary table.
	 * 
	 * @param tableName
	 *            the name of the temporary table
	 * @param attributeDefinitions
	 *            the definitions of attributes
	 * @param tableNames
	 *            the names of the tables
	 * @return a {@code RelationalDatabase} that contains the temporary table
	 * @throws ParsingException
	 *             if an error occurs while parsing the expressions
	 */
	// database.with("t", "min(balance) as balance", "accounts")
	public RelationalDatabase with(String tableName, String attributeDefinitions, String tableNames)
			throws ParsingException {
		return withGroupBy(tableName, attributeDefinitions, tableNames, null, null);
	}

	/**
	 * Generates {@code Tuple}s that contain the specified attributes using the specified tables and predicate and then
	 * stores these {@code Tuple}s in a temporary table.
	 * 
	 * @param tableName
	 *            the name of the temporary table
	 * @param attributeDefinitions
	 *            the definitions of attributes
	 * @param tableNames
	 *            the names of the tables
	 * @param predicate
	 *            a predicate
	 * @return a {@code RelationalDatabase} that contains the temporary table
	 * @throws ParsingException
	 *             if an error occurs while parsing the expressions
	 */
	public RelationalDatabase with(String tableName, String attributeDefinitions, String tableNames, String predicate)
			throws ParsingException {
		return withGroupBy(tableName, attributeDefinitions, tableNames, predicate, null);
	}

	/**
	 * Generates {@code Tuple}s that contain the specified attributes using the specified tables and grouping attributes
	 * and then stores these {@code Tuple}s in a temporary table.
	 * 
	 * @param tableName
	 *            the name of the temporary table
	 * @param attributeDefinitions
	 *            the definitions of attributes
	 * @param tableNames
	 *            the names of the tables
	 * @param groupingAttributes
	 *            the names of the attributes used for grouping
	 * @return a {@code RelationalDatabase} that contains the temporary table
	 * @throws ParsingException
	 *             if an error occurs while parsing the expressions
	 */
	public RelationalDatabase withGroupBy(String tableName, String attributeDefinitions, String tableNames,
			String groupingAttributes) throws ParsingException {
		return withGroupBy(tableName, attributeDefinitions, tableNames, null, groupingAttributes);
	}

	/**
	 * Generates {@code Tuple}s that contain the specified attributes using the specified tables, predicate, and
	 * grouping attributes and then stores these {@code Tuple}s in a temporary table.
	 * 
	 * @param tableName
	 *            the name of the temporary table
	 * @param attributeDefinitions
	 *            the definitions of attributes
	 * @param tableNames
	 *            the names of the tables
	 * @param predicate
	 *            a predicate
	 * @param groupingAttributes
	 *            the names of the attributes used for grouping
	 * @return a {@code RelationalDatabase} that contains the temporary table
	 * @throws ParsingException
	 *             if an error occurs while parsing the expressions
	 */
	public RelationalDatabase withGroupBy(String tableName, String attributeDefinitions, String tableNames,
			String predicate, String groupingAttributes) throws ParsingException {
		var n = new RelationalDatabase(this.databaseName);
		n.relations.putAll(this.relations);
		var o = query(attributeDefinitions, tableNames, predicate, groupingAttributes);
		var r = new Relation(o.outputSchema());
		n.relations.put(tableName, r);
		o.stream().forEach(t -> r.addTuple(t.attributeValues));
		return n;
	}

	/**
	 * Determines whether or not the specified {@code List} of attribute definitions has aggregate functions.
	 * 
	 * @param attributeDefinitions
	 *            a {@code List} of attribute definitions
	 * @return {@code true} if the specified {@code List} of attribute definitions has aggregate functions;
	 *         {@code false} otherwise
	 */
	private boolean hasAggregateFunctions(List<String> attributeDefinitions) {
		for (var a : attributeDefinitions)
			for (var name : Aggregate.aggregateFunctionNames())
				if (a.contains(name))
					return true;
		return false;
	}

}
