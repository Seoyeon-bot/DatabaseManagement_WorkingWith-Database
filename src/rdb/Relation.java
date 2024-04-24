package rdb;

import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * A {@code Relation} is a collection of {@code Tuple}s that share the same {@code RelationSchema}.
 * 
 * @author Jeong-Hyon Hwang (jhh@cs.albany.edu)
 */
public class Relation {

	/**
	 * The {@code RelationSchema} of this {@code Relation}.
	 */
	RelationSchema schema;

	/**
	 * The {@code Tuple}s in this {@code Relation}.
	 * map that storeing tuples, ex) key : c10
	 */
	Map<Object, Tuple> tuples = new TreeMap<Object, Tuple>(new Comparator<Object>() {

		@SuppressWarnings("unchecked")
		@Override
	public int compare(Object o1, Object o2) {
			if (o1 instanceof Comparable)
				return ((Comparable<Object>) o1).compareTo(o2);
			
			else if (o1.getClass().isArray()) {
				var a1 = (Object[]) o1;
				var a2 = (Object[]) o2;
				
				for (int i = 0; i < a1.length; i++)
					if (a1[i] instanceof Comparable) {
						int c = ((Comparable<Object>) a1[i]).compareTo(a2[i]);
						if (c != 0)
							return c;
					}
			}
			return System.identityHashCode(o1) - System.identityHashCode(o2);
		}

	});

	/**
	 * A {@code DuplicateKeyException} is thrown if there is an attempt to insert multiple {@code Tuple}s with the same
	 * key into a {@code Relation}.
	 * 
	 * @author Jeong-Hyon Hwang (jhh@cs.albany.edu)
	 */
	public static class DuplicateKeyException extends RuntimeException {

		/**
		 * An automatically generated serial version UID.
		 */
		private static final long serialVersionUID = 8979694097821536398L;

		/**
		 * Constructs a {@code DuplicateKeyException}.
		 * 
		 * @param key
		 *            the key that caused the {@code DuplicateKeyException}
		 */
		public DuplicateKeyException(String key) {
			super(key);
		}

	}

	/**
	 * Constructs a {@code Relation}.
	 * 
	 * @param schema
	 *            the {@code RelationSchema} of the {@code Relation}
	 */
	public Relation(RelationSchema schema) {
		this.schema = schema;
	}

	/**
	 * Returns a string representation of this {@code Relation}.
	 * 
	 * @return a string representation of this {@code Relation}
	 */
	@Override
	public String toString() {
		return "" + schema + ":" + tuples.size();
	}

	/**
	 * Returns the {@code RelationSchema} of this {@code Relation}.
	 * 
	 * @return the {@code RelationSchema} of this {@code Relation}
	 */
	public RelationSchema schema() {
		return schema;
	}

	/**
	 * Constructs a {@code Tuple} containing the specified attribute values and then adds it to this {@code Relation}.
	 * 
	 * @param attributeValues
	 *            attribute values
	 * @return the constructed {@code Tuple}
	 */
	public Tuple addTuple(Object[] attributeValues) {
		// construct tuple with given atributeValues for schema
		var tuple = new Tuple(schema, attributeValues); 
		@SuppressWarnings("unused")
		var key = primaryKey(tuple); // get primary key : it can be single atribute value or array of atribute values 
		// check if Relation contains tuple with same primary key -> Yes-> duplicatekey exception
		for(Map.Entry<Object, Tuple> entry : tuples.entrySet()) {
			var sampleKey = entry.getKey(); 
			if(sampleKey == key) {
				throw new DuplicateKeyException("Same key founded!! " );
			}
		}
		// No -> insert tuple in tuples map with key as key
		tuples.put(key,tuple); 
		// return newly created Tuple (not tuples array)
		return tuple;
	}

	/**
	 * Finds the {@code Tuple}s in this {@code Relation} that match the specified {@code Tuple}.
	 * 
	 * @param t
	 *            a {@code Tuple}
	 * @param commonAttributes
	 *            the names of the common attributes of the {@code Tuple}s
	 * @return the {@code Tuple}s in this {@code Relation} that match the specified {@code Tuple}
	 */
	public Collection<Tuple> matchingTuples(Tuple t, Set<String> commonAttributes) {
		if (schema.primaryKey != null && commonAttributes.containsAll(schema.primaryKey)) {
			var k = primaryKey(t);
			var tuple = tuples.get(k);
			if (tuple != null && matching(t, tuple, commonAttributes))
				return List.of(tuple);
			return List.of();
		}
		var l = new LinkedList<Tuple>();
		for (var tuple : tuples.values())
			if (matching(t, tuple, commonAttributes))
				l.add(tuple);
		return l;
	}

	/**
	 * Returns the primary key value of the specified {@code Tuple}.
	 * 
	 * @param tuple
	 *            a {@code Tuple}
	 * @return the primary key value of the specified {@code Tuple}
	 */
	private Object primaryKey(Tuple tuple) {
		if (schema.primaryKey == null)
			return tuple.attributeValues;
		
		if (schema.primaryKey.size() == 1)
			return tuple.attributeValue(schema.primaryKey.get(0));
		
		return tuple.attributeValues(schema.primaryKey.toArray(new String[0]));
	}

	/**
	 * Determines whether or not the two specified {@code Tuple}s match each other (i.e., have the same value for each
	 * common attribute).
	 * 
	 * @param t1
	 *            a {@code Tuple}
	 * @param t2
	 *            a {@code Tuple}
	 * @param commonAttributes
	 *            the common attributes of these {@code Tuple}s
	 * @return {@code true} if the two specified {@code Tuple}s match each other (i.e., have the same value for each
	 *         common attribute); {@code false} otherwise
	 */
	private boolean matching(Tuple t1, Tuple t2, Set<String> commonAttributes) {
		for (var a : commonAttributes)
			if (!t1.attributeValue(a).equals(t2.attributeValue(a)))
				return false;
		
		return true;
	}

}
