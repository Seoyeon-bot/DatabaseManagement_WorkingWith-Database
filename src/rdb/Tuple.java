package rdb;

import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.stream.IntStream;

/**
 * A {@code Tuple} represents a record in a relation containing a number of attributes.
 * 
 * @author Jeong-Hyon Hwang (jhh@cs.albany.edu)
 */
public class Tuple {

	/**
	 * A {@code TypeException} is thrown when an attribute value does not match the type of the attribute.
	 * 
	 * @author Jeong-Hyon Hwang (jhh@cs.albany.edu)
	 */
	public static class TypeException extends RuntimeException {

		/**
		 * Automatically generated serial version UID.
		 */
		private static final long serialVersionUID = 2260118532930630008L;

	}

	/**
	 * The {@code RelationSchema} for this {@code Tuple}.
	 */
	RelationSchema schema;

	/**
	 * The attribute values of this {@code Tuple}.
	 */
	Object[] attributeValues;

	/**
	 * Constructs a {@code Tuple}.
	 * 
	 * @param schema
	 *            a {@code RelationSchema} that the {@code Tuple} must be in compliance with
	 * @param attributeValues
	 *            the attribute values of the {@code Tuple}
	 * @throws TypeException
	 *             if an attribute value does not match the type of the attribute
	 */
	public Tuple(RelationSchema schema, Object... attributeValues) {
		this.schema = schema;
		this.attributeValues = new Object[schema.size()];  // array keep attribute values.
	
		// iterate over attributesValues 
		for (int i = 0; i < attributeValues.length; i++) {
			var value = attributeValues[i];
			// if type of attributeValue != type of attribute at attributeIndex -> throw TypeException
			var attributeIndexValueType = schema.attributeType(i); 
			if (!attributeIndexValueType.equals(value.getClass())){
				throw new TypeException(); 
			}
			// set attribute value at each index 
			setAttribute(i, value); 
		}
	}

	/**
	 * Concatenate the two specified {@code Tuple}s.
	 * 
	 * @param schema
	 *            the {@code RelationSchema} to use when concatenating the two {@code Tuple}s
	 * @param t1
	 *            a {@code Tuple}
	 * @param t2
	 *            a {@code Tuple}
	 * @return a new {@code Tuple} obtained by concatenating the two {@code Tuple}s
	 */
	public static Tuple concatenate(RelationSchema schema, Tuple t1, Tuple t2) {
		var values = new LinkedList<Object>();
		for (var attributeName : schema.attributeIndices.keySet()) {
			var value = t1.attributeValue(attributeName);
			if (value == null)
				value = t2.attributeValue(attributeName);
			values.add(value);
		}
		return new Tuple(schema, values.toArray());
	}

	/**
	 * Returns a string representation of this {@code Tuple}.
	 * 
	 * @return a string representation of this {@code Tuple}
	 */
	@Override
	public String toString() {
		var m = new LinkedHashMap<String, Object>();
		schema.attributeIndices.keySet().stream()
				.forEach(n -> m.put(n, attributeValues[schema.attributeIndices.get(n)]));
		return m.toString();
	}

	/**
	 * Returns the value of the specified attribute.
	 * 
	 * @param attributeIndex
	 *            the index of an attribute
	 * @return the value of the specified attribute
	 */
	public Object attributeValue(int attributeIndex) {
		return attributeValues[attributeIndex];
	}

	/**
	 * Returns the value of the specified attribute.
	 * 
	 * @param attributeName
	 *            the name of an attribute
	 * @return the value of the specified attribute
	 */
	public Object attributeValue(String attributeName) {
		Integer index = schema.attributeIndex(attributeName);
		if (index == null)
			return null;
		return attributeValues[index];
	}

	/**
	 * Returns the values of the specified attributes.
	 * 
	 * @param attributeNames
	 *            the names of the attributes
	 * @return the values of the specified attributes
	 */
	public Object[] attributeValues(String... attributeNames) {
		if (attributeNames == null)
			return null;
		var values = new Object[attributeNames.length];
		IntStream.range(0, attributeNames.length).forEach(i -> values[i] = attributeValue(attributeNames[i]));
		return values;
	}

	/**
	 * Sets the value of the specified attribute.
	 * 
	 * @param attributeIndex
	 *            the index of an attribute
	 * @param attributeValue
	 *            the value of the attribute
	 * @throws TypeException
	 *             if the specified attribute value does not match the type of the specified attribute
	 */
	public void setAttribute(int attributeIndex, Object attributeValue) throws TypeException {
		if (schema.attributeType(attributeIndex).isInstance(attributeValue))
			attributeValues[attributeIndex] = attributeValue;
		else
			throw new TypeException();
	}

}
