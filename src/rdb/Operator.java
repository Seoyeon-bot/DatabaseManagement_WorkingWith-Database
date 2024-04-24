package rdb;

import java.util.stream.Stream;

/**
 * An {@code Operator} processes {@code Tuple}s and produces {Tuple}s.
 * 
 * @author Jeong-Hyon Hwang (jhh@cs.albany.edu)
 */
public abstract class Operator {

	/**
	 * Returns the output schema of this {@code Operator}.
	 * 
	 * @return the output schema of this {@code Operator}
	 */
	public abstract RelationSchema outputSchema();

	/**
	 * Returns the output {@code Stream<Tuple>} of this {@code Operator}.
	 * 
	 * @return the output {@code Stream<Tuple>} of this {@code Operator}
	 */
	public abstract Stream<Tuple> stream();

}
