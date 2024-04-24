package rdb;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import expression.LogicalExpression;
import expression.ParsingException;

/**
 * A {@code Selection} outputs, among the input {@code Tuple}s, those that satisfy a specified predicate.
 * 
 * @author Jeong-Hyon Hwang (jhh@cs.albany.edu)
 */
public class Selection extends UnaryOperator {

	/**
	 * The {@code ExpressionEvaluator} for this {@code Selection}.
	 */
	protected ExpressionEvaluator evaluator;

	/**
	 * The predicate for this {@code Selection}.
	 */
	protected String predicate;

	/**
	 * Constructs a {@code Selection}.
	 * 
	 * @param input
	 *            the input {@code Operator} for the {@code Selection}
	 * @param predicate
	 *            the predicate for the {@code Selection}
	 * @throws ParsingException
	 *             if an error occurs while parsing the expressions
	 */
	public Selection(Operator input, String predicate) throws ParsingException {
		super(input);
		this.predicate = predicate;
		// Each Selection has its own ExpressionEvaluator, which can evaluate an expression
		evaluator = new ExpressionEvaluator(new LogicalExpression(predicate), input.outputSchema());
	}

	/**
	 * Returns the predicate of this {@code Selection}.
	 * 
	 * @return the predicate of this {@code Selection}
	 */
	public String predicate() {
		return predicate;
	}

	/**
	 * Returns the output {@code RelationSchema} of this {@code Selection}.
	 * 
	 * @return the output {@code RelationSchema} of this {@code Selection}
	 */
	@Override
	public RelationSchema outputSchema() {
		return input.outputSchema();
	}

	/**
	 * Returns the output {@code Stream<Tuple>} of this {@code Selection}.
	 * 
	 * @return the output {@code Stream<Tuple>} of this {@code Selection}
	 */
	@Override
	public Stream<Tuple> stream() {
		// the Selection filters out all Tuples that do not match the predicate.
		
		//access all the input Tuples by calling input.stream(). 
        Stream<Tuple> inputs = input.stream();
        boolean flag = true;
     // Filter the input stream based on the predicate ->  collect matching tuples into a list
        List<Tuple> satisfiedTuples = inputs.filter(t ->{
        	if(evaluator.evaluate(t).equals(Boolean.valueOf(flag))) { // case : true
        		return true; 
        	}else {
        		return false; 
        	}
        }).collect(Collectors.toList());
      
        // convert list to stream
        Stream<Tuple> outputs = satisfiedTuples.stream();

        // Return the output stream 
        return outputs;

	}

}
