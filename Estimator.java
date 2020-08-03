package sjdb;

import java.util.List;
import java.util.Iterator;
import java.lang.*;

public class Estimator implements PlanVisitor {


	public Estimator() {
		// empty constructor
	}

	/* 
	 * Create output relation on Scan operator
	 *
	 * Example implementation of visit method for Scan operators.
	 */
	public void visit(Scan op) {
		Relation input = op.getRelation();
		Relation output = new Relation(input.getTupleCount());
		
		Iterator<Attribute> iter = input.getAttributes().iterator();
		while (iter.hasNext()) {
			output.addAttribute(new Attribute(iter.next()));
		}

		op.setOutput(output);
	}

	// Create output relation on Project operator
	public void visit(Project op) {
		// get input relation and create empty output relation of same size
		Relation input = op.getInput().getOutput();
		Relation output = new Relation(input.getTupleCount());

		List<Attribute> outputAttr = op.getAttributes();	// get output attributes
		// if input attribute is an output attribute, it is added to the output relation
		Iterator<Attribute> iter = input.getAttributes().iterator();
		while (iter.hasNext()) {
			Attribute attr = iter.next();
			if (outputAttr.contains(attr)) {
				output.addAttribute(new Attribute(attr));
			}
		}

		op.setOutput(output);
	}

	// Create output relation on Select operator
	public void visit(Select op) {
		// get input relation
		Relation input = op.getInput().getOutput();
		Relation output;

		// get predicate condition
		Predicate predicate = op.getPredicate();
		Iterator<Attribute> iter = input.getAttributes().iterator();

		if (predicate.equalsValue()){
			// Case 1: attr = val
			Attribute attr = input.getAttribute(predicate.getLeftAttribute());	// only get left attribute since right attribute is a constant value

			// In this case, output relation has size T(R)/V(R, attribute)
			output = new Relation((int) Math.ceil(input.getTupleCount() / attr.getValueCount()));
			while (iter.hasNext()) {
				Attribute attrTemp = iter.next();
				if (attrTemp.equals(attr)){
					// value count of the attribute in this particular case is 1
					output.addAttribute(new Attribute(attrTemp.getName(), 1));
				}else{
					output.addAttribute(attrTemp);
				}
			}
		}else{
			// Case 2: attr = attr
			Attribute left_attr = input.getAttribute(predicate.getLeftAttribute());		// get left attribute in predicate
			Attribute right_attr = input.getAttribute(predicate.getRightAttribute());    // get right attribute in predicate

			// In this case, output relation has size T(R)/max(V(R, left_attr), V(R, right_attr))
			Integer tupleCount = Math.max(left_attr.getValueCount(), right_attr.getValueCount());
			output = new Relation((int) Math.ceil(input.getTupleCount()/tupleCount));
			while (iter.hasNext()) {
				Attribute attrTemp = iter.next();
				if (attrTemp.equals(left_attr) || attrTemp.equals(right_attr)){
					// value count in case of both attributes is min(V(R, left_attr), V(R, right_attr))
					Integer valueCount = Math.min(left_attr.getValueCount(), right_attr.getValueCount());
					output.addAttribute(new Attribute(attrTemp.getName(), valueCount));
				}else{
					output.addAttribute(attrTemp);
				}
			}
		}

		op.setOutput(output);
	}
	
	public void visit(Product op) {
		// get relations to the left and right of the product operator (since it is a binary operator and takes 2 input relations)
		Relation left_input = op.getLeft().getOutput();
		Relation right_input = op.getRight().getOutput();

		// size of output relation will be the product of the sizes of the 2 relations
		Relation output = new Relation(left_input.getTupleCount() * right_input.getTupleCount());

		// get attributes of left relation
		Iterator<Attribute> left_iter = left_input.getAttributes().iterator();
		while (left_iter.hasNext()) {
			output.addAttribute(new Attribute(left_iter.next()));
		}

		// get attributes of right relation
		Iterator<Attribute> right_iter = right_input.getAttributes().iterator();
		while (right_iter.hasNext()) {
			output.addAttribute(new Attribute(right_iter.next()));
		}

		op.setOutput(output);
	}
	
	public void visit(Join op) {
		// get left and right inputs of binary operator join
		Relation left_input = op.getLeft().getOutput();
		Relation right_input = op.getRight().getOutput();
		Predicate predicate = op.getPredicate();

		// get attributes on which join is performed
		Attribute left_attr = left_input.getAttribute(predicate.getLeftAttribute());
		Attribute right_attr = right_input.getAttribute(predicate.getRightAttribute());

		// In this case, output relation has size T(R)*T(S)/max(V(R, left_attr), V(S, right_attr))
		Integer tupleCount = Math.max(left_attr.getValueCount(), right_attr.getValueCount());
		Relation output = new Relation((int) Math.ceil(left_input.getTupleCount() * right_input.getTupleCount()/tupleCount));

		// value count in case of both attributes is min(V(R, left_attr), V(S, right_attr))
		Integer valueCount = Math.min(left_attr.getValueCount(), right_attr.getValueCount());

		// get attributes of left relation and add the appropriate ones to the output
		Iterator<Attribute> left_iter = left_input.getAttributes().iterator();
		while (left_iter.hasNext()) {
			Attribute attr = left_iter.next();
			if (attr.equals(left_attr)){
				output.addAttribute(new Attribute(attr.getName(), valueCount));
			}else {
				// For an attribute that is not a join attribute the value count is the same
				output.addAttribute(attr);
			}
		}

		// get attributes of right relation and add the appropriate ones to the output
		Iterator<Attribute> right_iter = right_input.getAttributes().iterator();
		while (right_iter.hasNext()) {
			Attribute attr = right_iter.next();
			if (attr.equals(right_attr)){
				output.addAttribute(new Attribute(attr.getName(), valueCount));
			}else {
				// For an attribute that is not a join attribute the value count is the same
				output.addAttribute(attr);
			}
		}

		op.setOutput(output);
	}
}