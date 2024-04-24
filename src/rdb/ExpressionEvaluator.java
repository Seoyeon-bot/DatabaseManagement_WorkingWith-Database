package rdb;

import java.util.LinkedList;

import expression.Expression;
import expression.UnboundVariableException;
import expression.Variable;

/**
 * An {@code ExpressionEvaluator} can evaluate an {@code Expression} on each given {@code Tuple}.
 * 
 * @author Jeong-Hyon Hwang (jhh@cs.albany.edu)
 */
public class ExpressionEvaluator {

	/**
	 * The {@code Expression} of this {@code ExpressionEvaluator}.
	 */
	Expression expression;

	/**
	 * The indices of the attributes that correspond to the {@code Variable}s of the {@code ArithmeticExpression}.
	 */
	Integer[] indices;

	/**
	 * Constructs an {@code ExpressionEvaluator}.
	 * 
	 * @param expression
	 *            an {@code Expression}
	 * @param schema
	 *            a {@code RelationSchema}
	 * @throws UnboundVariableException
	 *             if a variable in the {@code Expression} does not correspond to any attribute in the
	 *             {@code RelationSchema}
	 */
	public ExpressionEvaluator(Expression expression, RelationSchema schema) throws UnboundVariableException {
		this.expression = expression;
		LinkedList<Integer> indices = new LinkedList<Integer>();
		for (Variable v : expression.variables()) {
			Integer i = schema.attributeIndex(v.name());
			if (i == null)
				throw new UnboundVariableException(v.name());
			indices.add(i);
				v.setType(schema.attributeType(i));
		}
		this.indices = indices.toArray(new Integer[0]);
	}

	/**
	 * Evaluates the {@code Expression} of this {@code ExpressionEvaluator} on the specified {@code Tuple}.
	 * 
	 * @param t
	 *            a {@code Tuple}
	 * @return the result of evaluating the {@code Expression} on the specified {@code Tuple}
	 */
	public Object evaluate(Tuple t) {
		int i = 0;
		for (Variable v : expression.variables())
			v.setValue(t.attributeValue(indices[i++]));
		return expression.evaluate();
	}

}
