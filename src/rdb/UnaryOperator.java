package rdb;

/**
 * A {@code UnaryOperator} processes {@code Tuple}s from only one {Operator}.
 * 
 * @author Jeong-Hyon Hwang (jhh@cs.albany.edu)
 */
public abstract class UnaryOperator extends Operator {

	/**
	 * The input {@code Operator} for this {@code UnaryOperator}.
	 */
	protected Operator input;

	/**
	 * Constructs a {@code UnaryOperator}.
	 * 
	 * @param input
	 *            the input {@code Operator} for the {@code UnaryOperator}
	 */
	public UnaryOperator(Operator input) {
		this.input = input;
	}

	/**
	 * Returns the input schema of this {@code UnaryOperator}.
	 * 
	 * @return the input schema of this {@code UnaryOperator}
	 */
	public RelationSchema inputSchema() {
		return input.outputSchema();
	}

}
