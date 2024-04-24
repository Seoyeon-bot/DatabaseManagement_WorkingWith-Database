package rdb;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import expression.ArithmeticExpression;
import expression.ParsingException;

/**
 * A {@code Projection} converts each input {@code Tuple} into an output {@code Tuple}.
 * 
 * @author Jeong-Hyon Hwang (jhh@cs.albany.edu)
 */
public class Projection extends UnaryOperator {

	/**
	 * The {@code ExpressionEvaluator}s for this {@code Projection}.
	 */
	protected List<ExpressionEvaluator> evaluators = new ArrayList<ExpressionEvaluator>();

	/**
	 * The output schema of this {@code Projection}.
	 */
	protected RelationSchema outputSchema = new RelationSchema();

	/**
	 * Constructs a {@code Projection}.
	 * 
	 * @param input
	 *            the input {@code Operator} for the {@code Projection}
	 * @param attributeDefinitions
	 *            strings representing expressions that define the attributes to include the output schema of this
	 *            {@code Projection}
	 * @throws ParsingException
	 *             if an error occurs while parsing the expressions.
	 */
	public Projection(Operator input, List<String> attributeDefinitions) throws ParsingException {
		super(input);
		for (String attribute : attributeDefinitions) {
			var tokens = attribute.split(" as ");
			var attributeName = tokens.length == 2 ? tokens[1].trim() : attribute.trim();
			var expression = tokens.length == 2 ? new ArithmeticExpression(tokens[0])
					: new ArithmeticExpression(attribute);
			try { // for each attribute definition, add an attribute to the output schema
				evaluators.add(new ExpressionEvaluator(expression, input.outputSchema()));
				outputSchema.attribute(attributeName, expression.resultType());
			} catch (Exception e) {
				throw new ParsingException();
			}
		}
	}

	/**
	 * Returns the output {@code RelationSchema} of this {@code Projection}.
	 * 
	 * @return the output {@code RelationSchema} of this {@code Projection}
	 */
	@Override
	public RelationSchema outputSchema() {
		return outputSchema;
	}

	/**
	 * Returns the output {@code Stream<Tuple>} of this {@code Projection}.
	 * 
	 * @return the output {@code Stream<Tuple>} of this {@code Projection}
	 */
	@Override
	public Stream<Tuple> stream() {
		return input.stream().map(t -> tuple(t));
	}

	/**
	 * Constructs an output {@code Tuple} using the specified input {@code Tuple}.
	 * 
	 * @param t
	 *            an input {@code Tuple}
	 * @return the output {@code Tuple} generated from the specified input {@code Tuple}
	 */
	private Tuple tuple(Tuple t) {
		Object[] attributValues = new Object[outputSchema.size()];
		for (int i = 0; i < attributValues.length; i++) // for each output attribute
			attributValues[i] = evaluators.get(i).evaluate(t); // get an attribute value from the evaluator
		return new Tuple(outputSchema, attributValues); // construct a tuple using the attribute values
	}

}
