package rdb;

import java.util.ArrayList;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A {@code RelationSchema} represents the schema of a relation.
 * 
 * @author Jeong-Hyon Hwang (jhh@cs.albany.edu)
 */
public class RelationSchema {

	/**
	 * The mapping from each attribute name to attribute index.
	 * e.g., customerNumber to 0 and zipCode to 1)
	 */
	Map<String, Integer> attributeIndices = new LinkedHashMap<String, Integer>();

	/**
	 * The types of the attributes.
	 * [String.class, Integer.class]
	 */
	List<Class<?>> attributeTypes = new ArrayList<Class<?>>();

	/**
	 * The names of the attributes that constitute the primary key of this {@code RelationSchema}.
	 */
	List<String> primaryKey;

	/**
	 * A {@code DuplicateAttributeNameException} is thrown if there is an attempt to include multiple attributes with
	 * the same name to a {@code RelationSchema}.
	 * 
	 * @author Jeong-Hyon Hwang (jhh@cs.albany.edu)
	 */
	public static class DuplicateAttributeNameException extends RuntimeException {

		/**
		 * An automatically generated serial version UID.
		 */
		private static final long serialVersionUID = -6346797088300026496L;

		/**
		 * Constructs a {@code DuplicateAttributeNameException}.
		 * 
		 * @param attributeName
		 *            the name of the attribute that caused the {@code DuplicateAttributeNameException}
		 */
		public DuplicateAttributeNameException(String attributeName) {
			super(attributeName);
		}

	}
	//constructor
	public RelationSchema() {
	}

	/**
	 * Constructs a {@code RelationSchema} by combining the two specified {@code RelationSchema}s.
	 * 
	 * @param schema1
	 *            a {@code RelationSchema}
	 * @param schema2
	 *            a {@code RelationSchema}
	 */
	public RelationSchema(RelationSchema schema1, RelationSchema schema2) {
		for (var attributeName : schema1.attributeIndices.keySet())
			this.attribute(attributeName, schema1.attributeType(schema1.attributeIndex(attributeName)));
		
		for (var attributeName : schema2.attributeIndices.keySet())
			if (!this.attributeIndices.containsKey(attributeName))
				this.attribute(attributeName, schema2.attributeType(schema2.attributeIndex(attributeName)));
	}

	/**
	 * Adds an attribute to this {@code RelationSchema}.
	 * 
	 * key is attribute name in attributeIndices
	 * data type of attribute 
	 * @return this {@code RelationSchema}
	 * @throws DuplicateAttributeNameException
	 *             if the specified attribute name is already registered in this {@code RelationSchema}
	 */
	public RelationSchema attribute(String attributeName, Class<?> attributeType) {
		// TODO add some code here (it must not throw the UnsupportedOperationException)
        for (Map.Entry<String, Integer> entry : attributeIndices.entrySet()) {
            String name = entry.getKey(); 
            if(name == attributeName) {
            	// case we found duplicataed attributeName -> throw  DuplicateAttributeNameException(String attributeName) 
            	throw new DuplicateAttributeNameException(attributeName);
            }
        }
        	// not same -> add attribute 
        	attributeIndices.put(attributeName, attributeIndices.size()); 
        	attributeTypes.add(attributeType);
    
		// return current instance of  RelationSchema
        	return this;  
	}

	/**
	 * Sets the primary key of this {@code RelationSchema}.
	 * 
	 * @param primaryKey
	 *            the primary key of this {@code RelationSchema}
	 * @return this {@code RelationSchema}
	 */
	public RelationSchema primaryKey(String... primaryKey) {
		this.primaryKey = List.of(primaryKey);
		return this;
	}

	/**
	 * Returns a string representation of this {@code RelationSchema}.
	 * 
	 * @return a string representation of this {@code RelationSchema}
	 */
	@Override
	public String toString() {
		var m = new LinkedHashMap<String, String>();
		
		attributeIndices.keySet().stream()
				.forEach(n -> m.put(n, attributeTypes.get(attributeIndices.get(n)).getName()));
		return m.toString();
	}

	/**
	 * Returns the number of attributes in this {@code RelationSchema}.
	 * 
	 * @return the number of attributes in this {@code RelationSchema}
	 */
	public int size() {
		return attributeIndices.size();
	}

	/**
	 * Returns the type of the specified attribute.
	 * 
	 * @param attributeIndex
	 *            the index of the attribute
	 * @return the type of the specified attribute
	 */
	public Class<?> attributeType(int attributeIndex) {
		return attributeTypes.get(attributeIndex);
	}

	/**
	 * Returns the type of the specified attribute.
	 * 
	 * @param attributeName
	 *            the name of the attribute
	 * @return the type of the specified attribute
	 */
	public Class<?> attributeType(String attributeName) {
		return attributeType(attributeIndex(attributeName));
	}

	/**
	 * Returns the index of the specified attribute in this {@code RelationSchema} ({@code null} if no such attribute).
	 * 
	 * @param attributeName
	 *            the name of the attribute
	 * @return the index of the specified attribute in this {@code RelationSchema}; {@code null} if no such attribute
	 */
	public Integer attributeIndex(String attributeName) {
		return attributeIndices.get(attributeName);
	}

	/**
	 * Returns the names of the common attributes between this {@code RelationSchema} and the specified
	 * {@code RelationSchema}.
	 * 
	 * @param schema
	 *            a {@code RelationSchema}
	 * @return the names of the common attributes between this {@code RelationSchema} and the specified
	 *         {@code RelationSchema}
	 */
	public Set<String> commonAttributeNames(RelationSchema schema) {
		var commonAttributes = new LinkedHashSet<String>(attributeIndices.keySet());
		
		commonAttributes.retainAll(schema.attributeIndices.keySet());
		return commonAttributes;
	}

}
