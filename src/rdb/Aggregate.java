package rdb;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * An {@code Aggregate} can group {@code Tuple}s by certain attributes (e.g., ZIP code, gender) and obtain aggregate
 * values (e.g., maximum, average, and count) for each group of {@code Tuple}s.
 * 
 * @author Jeong-Hyon Hwang (jhh@cs.albany.edu)
 */
public class Aggregate extends UnaryOperator {

	/**
	 * A map that associates the name of each {@code AggregateFunction} with a {@code BiFunction} that can construct
	 * that {@code AggregateFunction}.
	 */
	static Map<String, BiFunction<String, Class<?>, AggregateFunction>> name2function = Map.of("count",
			(n, t) -> new Count(n, t), "sum", (n, t) -> new Sum(n, t), "min", (n, t) -> new Minimum(n, t), "max",
			(n, t) -> new Maximum(n, t), "avg", (n, t) -> new Average(n, t));

	/**
	 * Provides a {@code List} containing the name of each aggregate function.
	 * 
	 * @return a {@code List} containing the name of each aggregate function
	 */
	static List<String> aggregateFunctionNames() {
		return name2function.keySet().stream().map(s -> s + "(").toList();
	}

	/**
	 * The output schema of this {@code Projection}.
	 */
	RelationSchema outputSchema = new RelationSchema();

	/**
	 * The names of the grouping attributes.
	 */
	List<String> groupingAttributes;

	/**
	 * A {@code List} of {@code BiFunction}s that can generate {@code AggregateFunction}s.
	 */
	List<BiFunction<String, Class<?>, AggregateFunction>> aggregateFunctions = new ArrayList<BiFunction<String, Class<?>, AggregateFunction>>();

	/**
	 * The names of the input attributes.
	 */
	List<String> inputAttributeNames = new ArrayList<String>();

	/**
	 * Constructs an {@code Aggregate}.
	 * 
	 * @param input
	 *            the input {@code Operator} for the {@code Aggregate}
	 * @param groupingAttributes
	 *            the names of the grouping attributes
	 * @param attributeDefinitions
	 *            the attribute definitions for the {@code Aggregate}
	 */
	public Aggregate(Operator input, List<String> groupingAttributes, List<String> attributeDefinitions) {
		super(input);
		
		attributeDefinitions = new ArrayList<String>(attributeDefinitions);
		this.groupingAttributes = groupingAttributes;
		
		if (groupingAttributes != null)
			for (var a : groupingAttributes)
				outputSchema.attribute(a, input.outputSchema().attributeType(a));
		
		if (groupingAttributes != null)
			attributeDefinitions.removeAll(groupingAttributes);
		
		for (var a : attributeDefinitions) {
			var aggregateFunction = name2function.get(a.substring(0, a.indexOf('(')).trim());
			var inputAttributeName = a.substring(a.indexOf('(') + 1, a.indexOf(')')).trim();
			var outputAttributeName = a.split(" as ")[1].trim();
			aggregateFunctions.add(aggregateFunction);
			inputAttributeNames.add(inputAttributeName);
			outputSchema.attribute(outputAttributeName, aggregateFunction
					.apply(inputAttributeName, input.outputSchema().attributeType(inputAttributeName)).valueType());
		}
	}

	/**
	 * Returns the output schema of this {@code Aggregate}.
	 * 
	 * @return the output schema of this {@code Aggregate}
	 */
	@Override
	public RelationSchema outputSchema() {
		return outputSchema;
	}

	/**
	 * Returns the output {@code Stream<Tuple>} of this {@code Aggregate}.
	 * 
	 * @return the output {@code Stream<Tuple>} of this {@code Aggregate}
	 */
	@Override
	public Stream<Tuple> stream() {
		Map< List<Object>, List<AggregateFunction> > summary = new TreeMap<List<Object>, List<AggregateFunction>>(
				new Comparator<List<Object>>() {

					@SuppressWarnings("rawtypes")
					@Override
					public int compare(List<Object> o1, List<Object> o2) {
						for (int i = 0; i < o1.size(); i++)
							if (o1.get(i) instanceof Comparable) {
								@SuppressWarnings("unchecked")
								int c = ((Comparable) o1.get(i)).compareTo(o2.get(i));
								//System.out.println("o1 : " + o1.get(i).toString() + " o2: " + o2.get(i).toString());
								if (c != 0)
								//	System.out.println("c : "+c);
									return c;
							}
						return 0;
					}
				});
		input.stream().forEach(t -> update(summary, t));
		//check summary 
				for(Map.Entry<List<Object>, List<AggregateFunction>> entry : summary.entrySet()) {
					List<Object> key = entry.getKey(); 
					List<AggregateFunction> value = entry.getValue();
					//System.out.println("key: " + key + " value: " + value); 
				}
		return stream(summary);
	}

	/**
	 * Constructs a {@code Stream<Tuple>} from the specified {@code Map}.
	 * 
	 * @param summary
	 *            a {@code Map} that associates a list of grouping attribute values ({@code List<Object>}) with a list
	 *            of {@code AggregateFunction}s ({@code List<AggregateFunction>})
	 * @return a {@code Stream<Tuple>} constructed from the specified {@code Map}
	 */
	protected Stream<Tuple> stream(Map<List<Object>, List<AggregateFunction>> summary) {
		// list to store tuples for each entr of summary map 
		List<Tuple> list_tuple = new ArrayList<>(); 
		
		//iterate over the entries of this map and create a Tuple
		summary.entrySet().stream().forEach(e -> {
			List<Object> key = e.getKey(); 
			List<AggregateFunction> functions = e.getValue(); 
			List<Object> store_Attr_functions = new ArrayList<>(); // for each key we will create array list to store list of keys and list of aggrigate function. 
			 if (groupingAttributes!=null) {
		        	key = e.getKey(); // Use the actual key obtained from the summary map
		          //  System.out.println("Key:" + key);
		      } else {
		    	    //System.out.println("groupping attribute is null"); 
		        	 key = new ArrayList<>(); // create empty arraylist when attribute is null case we couldn't find common attribute.      
		      }
		     
			 for(int i=0; i <key.size(); i++) { //add each attribute from key into store_Attr_functions list 
				 store_Attr_functions.add(key.get(i));
				 //System.out.println(" add : " + key.get(i));
			 }
			// prepare to create tuple 
			if (!functions.isEmpty() || !store_Attr_functions.isEmpty()) {
				for(int i=0; i <functions.size(); i++) {
					var f = functions.get(i); 
					var value = f.value();
					//System.out.println(" value: " + value);
					store_Attr_functions.add(value); 
				}
			}
			//create tuple
		   Tuple tuple = new Tuple(outputSchema, store_Attr_functions.toArray()); // pass schema and linked list type as parameter
			//System.out.println("tuple: " + tuple);
		   list_tuple.add(tuple); // add to list 
		});
		return list_tuple.stream(); 
	}

	/**
	 * Updates the specified {@code Map} based on the specified {@code Tuple}
	 * 
	 * @param summary
	 *            a {@code Map} that maintains a {@code List} of {@code AggregateFunction}s for each group
	 * @param t
	 *            a {@code Tuple}
	 */
	private void update(Map<List<Object>, List<AggregateFunction>> summary, Tuple t) {
	// List.of("")check
		List<Object> key = groupingAttributes == null ? List.of("")
				: List.of(t.attributeValues(groupingAttributes.toArray(new String[0])));
		var functions = summary.get(key);
		
		if (functions == null) {
			functions = new ArrayList<AggregateFunction>();
			summary.put(key, functions);
			for (int i = 0; i < aggregateFunctions.size(); i++)
				functions.add(aggregateFunctions.get(i).apply(this.inputAttributeNames.get(i),
						input.outputSchema().attributeType(inputAttributeNames.get(i))));
		}
		
		for (int i = 0; i < aggregateFunctions.size(); i++)
			functions.get(i).update(t.attributeValue(inputAttributeNames.get(i)));
	}

	/**
	 * An {@code AggregateFunction} computes a summary value (e.g., maximum, minimum, count, and average) over a set of
	 * values.
	 * 
	 * @author Jeong-Hyon Hwang (jhh@cs.albany.edu)
	 */
	public static abstract class AggregateFunction {

		/**
		 * The name of the attribute used by this {@code AggregateFunction}.
		 */
		protected String attributeName;

		/**
		 * The type of the attribute used by this {@code AggregateFunction}.
		 */
		protected Class<?> attributeType;

		/**
		 * Constructs an {@code AggregateFunction}.
		 * 
		 * @param attributeName
		 *            the name of the attribute used by the {@code AggregateFunction}
		 * @param attributeType
		 *            the type of the attribute used by the {@code AggregateFunction}
		 */
		public AggregateFunction(String attributeName, Class<?> attributeType) {
			this.attributeName = attributeName;
			this.attributeType = attributeType;
		}

		/**
		 * Returns a string representation of this {@code AggregateFunction}.
		 */
		@Override
		public String toString() {
			return getClass().getSimpleName() + "(" + attributeName + ")";
		}

		/**
		 * Updates this {@code AggregateFunction} based on the specified value.
		 * 
		 * @param v
		 *            a value for updating this {@code AggregateFunction}
		 */
		public abstract void update(Object v);

		/**
		 * Returns the summary value from this {@code AggregateFunction}.
		 * 
		 * @return the summary value from this {@code AggregateFunction}
		 */
		public abstract Object value();

		/**
		 * Returns the type of the summary value from this {@code AggregateFunction}.
		 * 
		 * @return the type of the summary value from this {@code AggregateFunction}
		 */
		public abstract Class<?> valueType();

	}

	/**
	 * A {@code Sum} computes, given a collection of values, the sum of the values.
	 * 
	 * @author Jeong-Hyon Hwang (jhh@cs.albany.edu)
	 */
	public static class Sum extends AggregateFunction {

		/**
		 * The sum maintained by this {@code Sum}.
		 */
		Number sum = 0;

		/**
		 * Constructs a {@code Sum}.
		 * 
		 * @param attributeName
		 *            the name of the attribute used by the {@code Sum}
		 * @param attributeType
		 *            the type of the attribute used by the {@code Sum}
		 */
		public Sum(String attributeName, Class<?> attributeType) {
			super(attributeName, attributeType);
		}

		/**
		 * Returns a string representation of this {@code Sum}.
		 */
		@Override
		public String toString() {
			return super.toString() + ":" + sum;
		}

		/**
		 * Updates this {@code Sum} based on the specified value.
		 * 
		 * @param v
		 *            a value for updating this {@code Sum}
		 */
		@Override
		public void update(Object v) {
			if (v instanceof Integer)
				sum = ((Integer) v).intValue() + sum.intValue();
			else if (v instanceof Double)
				sum = ((Double) v).doubleValue() + sum.doubleValue();
			else
				throw new UnsupportedOperationException();
		}

		/**
		 * Returns the summary value (i.e., sum) from this {@code Sum}.
		 * 
		 * @return the summary value (i.e., sum) from this {@code Sum}
		 */
		@Override
		public Object value() {
			return sum;
		}

		/**
		 * Returns the type of the summary value (i.e., sum) from this {@code Sum}.
		 * 
		 * @return the type of the summary value (i.e., sum) from this {@code Sum}
		 */
		@Override
		public Class<?> valueType() {
			return attributeType;
		}

	}

	/**
	 * A {@code Average} computes, given a collection of values, the average of the values.
	 * 
	 * @author Jeong-Hyon Hwang (jhh@cs.albany.edu)
	 */
	public static class Average extends Sum {

		/**
		 * The count.
		 */
		int count = 0;

		/**
		 * Constructs a {@code Average}.
		 * 
		 * @param attributeName
		 *            the name of the attribute used by the {@code Average}
		 * @param attributeType
		 *            the type of the attribute used by the {@code Average}
		 */
		public Average(String attributeName, Class<?> attributeType) {
			super(attributeName, attributeType);
		}

		/**
		 * Returns a string representation of this {@code Average}.
		 */
		@Override
		public String toString() {
			return super.toString() + ":" + count;
		}

		/**
		 * Updates this {@code Average} based on the specified value.
		 * 
		 * @param v
		 *            a value for updating this {@code Average}
		 */
		@Override
		public void update(Object v) {
			super.update(v);
			count++;
		}

		/**
		 * Returns the summary value (i.e., average) from this {@code Average}.
		 * 
		 * @return the summary value (i.e., average) from this {@code Average}
		 */
		@Override
		public Object value() {
			if (attributeType.equals(Integer.class))
				return ((Integer) sum) / count;
			else if (attributeType.equals(Double.class))
				return ((Double) sum) / count;
			else
				throw new UnsupportedOperationException();
		}

	}

	/**
	 * A {@code Count} computes, given a collection of values, the count of the values.
	 * 
	 * @author Jeong-Hyon Hwang (jhh@cs.albany.edu)
	 */
	public static class Count extends AggregateFunction {

		/**
		 * The count managed by this {@code Count}.
		 */
		protected int count = 0;

		/**
		 * Constructs a {@code Count}.
		 * 
		 * @param attributeName
		 *            the name of the attribute used by the {@code Count}
		 * @param attributeType
		 *            the type of the attribute used by the {@code Count}
		 */
		public Count(String attributeName, Class<?> attributeType) {
			super(attributeName, attributeType);
		}

		/**
		 * Returns a string representation of this {@code Count}.
		 */
		@Override
		public String toString() {
			return super.toString() + ":" + count;
		}

		/**
		 * Updates this {@code Count} based on the specified value.
		 * 
		 * @param v
		 *            a value for updating this {@code Count}
		 */
		@Override
		public void update(Object v) {
			// task7a
			count++;
		}

		/**
		 * Returns the summary value (i.e., count) from this {@code Average}.
		 * 
		 * @return the summary value (i.e., count) from this {@code Average}
		 */
		@Override
		public Integer value() {
			return count;
		}

		/**
		 * Returns the type of the summary value (i.e., count) from this {@code Average}.
		 * 
		 * @return the type of the summary value (i.e., count) from this {@code Average}
		 */
		@Override
		public Class<?> valueType() {
			return Integer.class;
		}

	}

	/**
	 * A {@code Maximum} computes, given a collection of values, the minimum of the values.
	 * 
	 * @author Jeong-Hyon Hwang (jhh@cs.albany.edu)
	 */
	public static class Maximum extends AggregateFunction {

		/**
		 * The current maximum value.
		 */
		protected Object maximum = null;

		/**
		 * Constructs a {@code Maximum}.
		 * 
		 * @param attributeName
		 *            the name of the attribute used by the {@code Maximum}
		 * @param attributeType
		 *            the type of the attribute used by the {@code Maximum}
		 */
		public Maximum(String attributeName, Class<?> attributeType) {
			super(attributeName, attributeType);
		}

		/**
		 * Returns a string representation of this {@code Maximum}.
		 */
		@Override
		public String toString() {
			return super.toString() + ":" + maximum;
		}

		/**
		 * Updates this {@code Maximum} based on the specified value.
		 * 
		 * @param v
		 *            a value for updating this {@code Maximum}
		 */
		@SuppressWarnings("unchecked")
		@Override
		public void update(Object v) {
			if (maximum == null || ((Comparable<Object>) maximum).compareTo(v) < 0)
				maximum = v;
		}

		/**
		 * Returns the summary value (i.e., maximum) from this {@code Maximum}.
		 * 
		 * @return the summary value (i.e., maximum) from this {@code Maximum}
		 */
		@Override
		public Object value() {
			return maximum;
		}

		/**
		 * Returns the type of the summary value (i.e., maximum) from this {@code Maximum}.
		 * 
		 * @return the type of the summary value (i.e., maximum) from this {@code Maximum}
		 */
		@Override
		public Class<?> valueType() {
			return attributeType;
		}

	}

	/**
	 * A {@code Minimum} computes, given a collection of values, the minimum of the values.
	 * 
	 * @author Jeong-Hyon Hwang (jhh@cs.albany.edu)
	 */
	public static class Minimum extends AggregateFunction {

		protected Object minimum = null; //create min value ( same as Maximum class) 
		/**
		 * Constructs a {@code Minimum}.
		 * 
		 * @param attributeName
		 *            the name of the attribute used by the {@code Minimum}
		 * @param attributeType
		 *            the type of the attribute used by the {@code Minimum}
		 */
		public Minimum(String attributeName, Class<?> attributeType) {
			super(attributeName, attributeType);
		}

		/**
		 * Returns a string representation of this {@code minimum}.
		 */
		@Override
		public String toString() {
			return super.toString() + ":" + minimum; // same as Maximum class.
		}

		/**
		 * Updates this {@code Minimum} based on the specified value.
		 * 
		 * @param v
		 *            a value for updating this {@code Minimum}
		 */
		@Override
		public void update(Object v) {
			// fix smallest v and update it 
			if (minimum == null || ((Comparable<Object>) v).compareTo(minimum) < 0) {
	            minimum = v;
	        }
		}

		/**
		 * Returns the summary value (i.e., minimum) from this {@code Minimum}.
		 * 
		 * @return the summary value (i.e., minimum) from this {@code Minimum}
		 */
		@Override
		public Object value() {
			// return minimum 
			return minimum; 
		}

		/**
		 * Returns the type of the summary value (i.e., minimum) from this {@code Minimum}.
		 * 
		 * @return the type of the summary value (i.e., minimum) from this {@code Minimum}
		 */
		@Override
		public Class<?> valueType() {
			return attributeType;
		}

	}

}
