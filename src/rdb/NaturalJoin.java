package rdb;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Stream;

/**
 * A {@code NaturalJoin} finds, for each given {@code Tuple}, every matching {@code Tuple} from a {@code Relation} and
 * then produces a concatenation of these two {@code Tuple}s.
 * 
 * @author Jeong-Hyon Hwang (jhh@cs.albany.edu)
 */
public class NaturalJoin extends UnaryOperator {

	/**
	 * The common attributes between the two {@code RelationSchema}s involved in this {@code NaturalJoin}.
	 */
	Set<String> commonAttributes;

	/**
	 * The output {@code RelationSchema} of this {@code NaturalJoin}.
	 */
	RelationSchema outputSchema;

	/**
	 * The referenced {@code Relation} for this {@code NaturalJoin}.
	 */
	Relation referencedRelation;

	/**
	 * Constructs a {@code NaturalJoin}.
	 * 
	 * @param input
	 *            the input {@code Operator} for the {@code NaturalJoin}
	 * @param referencedRelation
	 *            the referenced {@code Relation} for the {@code NaturalJoin}
	 */
	public NaturalJoin(Operator input, Relation referencedRelation) {
		super(input);
		this.referencedRelation = referencedRelation;
		//find common attribute between input and referencedRelation
		this.commonAttributes = input.outputSchema().commonAttributeNames(referencedRelation.schema());
		this.outputSchema = new RelationSchema(input.outputSchema(), referencedRelation.schema());
	}

	@Override
	public RelationSchema outputSchema() {
		return outputSchema;
	}

	@Override
	public Stream<Tuple> stream() {
		//  access its input Stream of Tuples by calling input.stream().
		Stream<Tuple> inputs = input.stream();
		
		//For each Tuple from that input Stream, the method needs to find all matching Tuples 
		// from the relation named referencedRelation
		Stream<Tuple> outputs =  null; 
	     outputs = inputs.flatMap(t -> {
	         // Find all matching Tuples from referencedRelation
	         Stream<Tuple> tuples = referencedRelation.matchingTuples(t, commonAttributes).stream();
	         
	         // Concatenate matching Tuples and include in the output Stream
	         return tuples.map(t2 -> Tuple.concatenate(outputSchema, t, t2));
	     });
	     
	     // Return the output stream
	     return outputs;
	     
	}

}
