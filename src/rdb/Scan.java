package rdb;

import java.util.stream.Stream;

/**
 * A {@code Scan} can access a {@code Relation} and provide a {@code Stream} of {@code Tuple}s that are stored in that
 * {@code Relation}.
 */
public class Scan extends Operator {

	/**
	 * The {@code Relation} that this {@code Scan} accesses.
	 *  variable references a Relation storing the Tuples 
	 *  that the Scan operator needs to access.
	 */
	Relation relation;

	/**
	 * Constructs a {@code Scan}.
	 * @param relation the {@code Relation} that the {@code Scan} needs to access
	 */
	public Scan(Relation relation) {
		this.relation = relation;
	}

	/**
	 * returns the output RelationSchema of the Operator.
	 * that this {@code Scan} accesses.
	 * 
	 * @return the {@code RelationSchema} of the {@code Relation} that this {@code Scan} accesses
	 */
	@Override
	public RelationSchema outputSchema() {
		return relation.schema();
	}

	/**
	 * returns the Stream of Tuples that the Operator output
	 * that this {@code Scan} accesses.
	 * 
	 * @return a {@code Stream} of the {@code Tuple}s from the {@code Relation} that this {@code Scan} accesses
	 */
	@Override
	public Stream<Tuple> stream() {
		// return a Stream of Tuples from the Relation that the relation member variable
		Stream<Tuple> output = relation.tuples.values().stream();
		return output;
	}

}
