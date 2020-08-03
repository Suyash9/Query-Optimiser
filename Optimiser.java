package sjdb;

import java.util.*;
import java.util.stream.Collectors;

public class Optimiser {

    /*
     Step 1: Move SELECT operators down
     Step 2: Reorder to put most restrictive SELECT first
     Step 3: Combine PRODUCT and SELECT operations to create JOIN
     Step 4: Move PROJECT operators down
    */

    private Catalogue catalogue;
    private LinkedList<Operator> reorderOperators = new LinkedList<>();     // LinkedList to store operators that need to be reordered
    private Set<Predicate> reorderPredicates = new HashSet<>();     // HashSet to store predicates required for join reordering

    // Constructor
    public Optimiser(Catalogue catalogue){
        this.catalogue = catalogue;
    }

    // Optimise function that takes the query plan as input, performs the 4 optimisation steps and returns the new, optimised query plan
    public Operator optimise(Operator plan) {
        plan = optimiseSelects(plan);       // Step 1: Move SELECT operators down
        plan = reorderJoins(plan);        // Step 2: Reorder to put most restrictive SELECT first
        plan = createJoins(plan);         // Step 3: Combine PRODUCT and SELECT operations to create JOIN
        plan = pushProjectsDown(plan, new HashSet<>());   // Step 4: Move PROJECT operators down
        return acceptOperator(plan);
    }

    // Method to move all selects down by searching down the query plan using recursion
    private Operator optimiseSelects(Operator operator){
        String operatorName = getOperatorType(operator);
        if (operatorName.equals("Select")) {
            Select select = (Select) operator;
            Operator newOperator = pushSelectDown(select, select.getInput());
            if(getOperatorType(newOperator).equals("Select")){
                Select newSelect = (Select) newOperator;
                return acceptOperator(new Select(optimiseSelects(newSelect.getInput()),newSelect.getPredicate()));
            }else{
                return acceptOperator(optimiseSelects(newOperator));
            }
        }else if(operatorName.equals("Project")){
            Project project = (Project) operator;
            return acceptOperator(new Project(optimiseSelects(project.getInput()), project.getAttributes()));
        }else if(operatorName.equals("Product")){
            Product product = (Product) operator;
            return acceptOperator(new Product(optimiseSelects(product.getLeft()),optimiseSelects(product.getRight())));
        }else if (operatorName.equals("Scan")) {
            Scan scan = (Scan) operator;
            return acceptOperator(new Scan((NamedRelation) scan.getRelation()));
        }
        return acceptOperator(operator);
    }

    // Method to push a specific select down the query plan
    private Operator pushSelectDown(Select select, Operator operator){
        Predicate predicate = select.getPredicate();
        if (predicate.equalsValue()) {
            // select operator with predicate of type attr=val is pushed down to just above the operator that contains attr
            Attribute leftAttr = predicate.getLeftAttribute();
            if(getOperatorType(operator).equals("Product")){
                Product product = (Product) operator;
                return acceptOperator(new Product(pushSelectDown(select, product.getLeft()), pushSelectDown(select,product.getRight())));
            }else if(getOperatorType(operator).equals("Project")){
                Project project = (Project) operator;
                return acceptOperator(new Project(pushSelectDown(select, project.getInput()), project.getAttributes()));
            }else if(getOperatorType(operator).equals("Select")){
                Select selectOp = (Select) operator;
                return acceptOperator(new Select(pushSelectDown(select, selectOp.getInput()),selectOp.getPredicate()));
            }else {
                // If the scan operator contains the attr in predicate, select is pushed down to just above the scan operator
                if (operator.getOutput().getAttributes().contains(leftAttr)) {
                    Scan scan = new Scan((NamedRelation) ((Scan) operator).getRelation());
                    return acceptOperator(new Select(scan, predicate));
                } else {
                    return acceptOperator(new Scan((NamedRelation) ((Scan) operator).getRelation()));
                }
            }
        }else{
            // select operator with predicate of type attr=attr is pushed down to just above the operator that contains both attrs
            Attribute leftAttr = predicate.getLeftAttribute();
            Attribute rightAttr = predicate.getRightAttribute();
            if(getOperatorType(operator).equals("Product")) {
                Product product = (Product) operator;
                Operator leftOp = product.getLeft();
                Operator rightOp = product.getRight();

                // If the left operator of the product contains both attributes, the select is pushed down to just above the left operator
                List<Attribute> leftAttributes = leftOp.getOutput().getAttributes();
                if (leftAttributes.contains(leftAttr) && leftAttributes.contains(rightAttr)) {
                    if(getOperatorType(leftOp).equals("Scan")) {
                        Scan scan = new Scan((NamedRelation) ((Scan) leftOp).getRelation());
                        Select newSelect = new Select(scan, predicate);
                        return acceptOperator(new Product(newSelect, rightOp));
                    }else
                        return acceptOperator(new Product(pushSelectDown(select, leftOp), rightOp));
                }

                // If the right operator of the product contains both attributes, the select is pushed down to just above the right operator
                List<Attribute> rightAttributes = rightOp.getOutput().getAttributes();
                if (rightAttributes.contains(leftAttr) && rightAttributes.contains(rightAttr)) {
                    if(getOperatorType(rightOp).equals("Scan")) {
                        Scan scan = new Scan((NamedRelation) ((Scan) rightOp).getRelation());
                        Select newSelect = new Select(scan, predicate);
                        return acceptOperator(new Product(leftOp, newSelect));
                    }else
                        return acceptOperator(new Product(leftOp, pushSelectDown(select, rightOp)));
                }

                // If the product operator contains both attributes, the select is pushed down to just above the product operator
                List<Attribute> productAttributes = operator.getOutput().getAttributes();
                if (productAttributes.contains(leftAttr) && productAttributes.contains(rightAttr)){
                    return acceptOperator(new Select(new Product(product.getLeft(), product.getRight()), predicate));
                }else {
                    return acceptOperator(new Product(pushSelectDown(select, product.getLeft()), pushSelectDown(select, product.getRight())));
                }
            }else if(getOperatorType(operator).equals("Project")){
                Project project = (Project) operator;
                return acceptOperator(new Project(pushSelectDown(select, project.getInput()), project.getAttributes()));
            }else if(getOperatorType(operator).equals("Select")){
                Select selectOp = (Select) operator;
                return acceptOperator(new Select(pushSelectDown(select, selectOp.getInput()),selectOp.getPredicate()));
            }else if (getOperatorType(operator).equals("Scan")){
                // If the scan operator contains both attrs in predicate, select is pushed down to just above the scan operator
                List<Attribute> scanAttributes = operator.getOutput().getAttributes();
                if (scanAttributes.contains(leftAttr) && scanAttributes.contains(rightAttr)) {
                    Scan scan = new Scan((NamedRelation) ((Scan) operator).getRelation());
                    return acceptOperator(new Select(scan, predicate));
                } else {
                    return acceptOperator(new Scan((NamedRelation) ((Scan) operator).getRelation()));
                }
            }
        }
        return acceptOperator(select);
    }

    // Method to perform reordering of operators in order to put operators with smallest tuple count first
    private Operator reorderJoins(Operator operator){
        getReorderOperators(operator);      // Populate linked list of operators and predicates that need to be reordered
        if(reorderOperators.isEmpty()) {
            return acceptOperator(operator);
        }else{
            removeRedundantScans();     // Remove unnecessary scans from linked list of operators to be reordered
            // Sorting linked list of operators to be reordered by tuple count
            Collections.sort(reorderOperators, Comparator.comparing((Operator o) -> o.getOutput().getTupleCount()));
            if(getOperatorType(operator).equals("Project")){
                Project project = (Project) operator;
                return acceptOperator(new Project(reorderOperators(),project.getAttributes()));
            }else {
                return acceptOperator(reorderOperators());
            }
        }
    }

    //  Recursive method that creates a new, reordered query plan by polling operators at the end of the linked list of operators
    private Operator reorderOperators(){
        Operator operator = acceptOperator(reorderOperators.pollLast());
        if(!reorderOperators.isEmpty()){
            Product newProduct = new Product(reorderOperators(),operator);
            Operator select = createSelect(newProduct, new LinkedList<>(reorderPredicates));
            return acceptOperator(select);
        }
        return acceptOperator(operator);
    }

    // Recursive method to create selects above reordered operators if the attributes of the predicate are in the output of the operator
    private Operator createSelect(Operator operator, LinkedList<Predicate> selectPredicates){
        if(selectPredicates.size()>0){
            List<Attribute> operatorAttributes = acceptOperator(operator).getOutput().getAttributes();
            Predicate predicate = selectPredicates.pollFirst();
            if(operatorAttributes.contains(predicate.getLeftAttribute()) && operatorAttributes.contains(predicate.getRightAttribute())){
                Select select = new Select(operator, predicate);
                reorderPredicates.remove(predicate);
                return acceptOperator(createSelect(select, selectPredicates));
            }else{
                return acceptOperator(createSelect(operator, selectPredicates));
            }
        }
        return acceptOperator(operator);
    }

    // Method that populates the reorderOperators linked list and the reorderPredicates list recursively
    private void getReorderOperators(Operator operator){
        List<Operator> inputs = operator.getInputs();
        if(getOperatorType(operator).equals("Select")){
            Select select = (Select) operator;
            Predicate predicate = select.getPredicate();
            Operator input = select.getInput();
            if(getOperatorType(input).equals("Scan")){
                reorderOperators.add(select);
            }
            if(!predicate.equalsValue()) {
                reorderPredicates.add(predicate);
            }
        }
        for (Operator o : inputs) {
            if(o != null){
                if(getOperatorType(o).equals("Scan")){
                    Scan scan = (Scan) o;
                    reorderOperators.add(scan);
                }
                if(o.getInputs() != null){
                    getReorderOperators(o);
                }
            }
        }
    }

    // Method that removes scans from the reorderOperators linked list if the scan is an input for a select operator
    private void removeRedundantScans(){
        List<Operator> removeScans = new ArrayList<>();
        for(Operator o:reorderOperators){
            if(getOperatorType(o).equals("Select")){
                Select select = (Select) o;
                Operator input = select.getInput();
                if(reorderOperators.contains(input) && getOperatorType(input).equals("Scan")){
                    removeScans.add(input);
                }
            }
        }
        reorderOperators.removeAll(removeScans);
    }

    // Method to combine selects and the products below them to create joins in a query plan using recursion
    private Operator createJoins(Operator operator){
        String operatorName = getOperatorType(operator);
        if (operatorName.equals("Select")) {
            Select select = (Select) operator;
            Operator input = select.getInput();
            // If a product operator is the input for a select operator, the two operators are combined to create a join
            if (getOperatorType(input).equals("Product")){
                Product product = (Product) input;
                return acceptOperator(createJoin(select, product));
            }else{
                return acceptOperator(new Select(select.getInput(),select.getPredicate()));
            }
        }else if(operatorName.equals("Project")){
            Project project = (Project) operator;
            return acceptOperator(new Project(createJoins(project.getInput()), project.getAttributes()));
        }else if(operatorName.equals("Product")){
            Product product = (Product) operator;
            return acceptOperator(new Product(createJoins(product.getLeft()),createJoins(product.getRight())));
        }else if (operatorName.equals("Scan")) {
            Scan scan = (Scan) operator;
            return acceptOperator(new Scan((NamedRelation) scan.getRelation()));
        }
        return acceptOperator(operator);
    }

    // Method that combines a specific select and product operator to create a join
    private Operator createJoin(Select select, Product product){
        Predicate predicate = select.getPredicate();
        Join join = new Join(product.getLeft(), product.getRight(), predicate);
        return acceptOperator(join);
    }

    /*  Method to reduce the number of attributes of intermediate relations by pushing projects down the query plan using recursion.
        Attributes that need to be projected are added to the set recursively.
        When an operator is encountered, the attributes common to the set and the output of the operator are obtained.
        If all attributes of an operator are required, no project operator is pushed down to just above the operator.
        Otherwise, the required attributes that need to be projected are pushed down to just above the operator.
     */
    private Operator pushProjectsDown(Operator operator, Set<Attribute> allProjectAttributes){
        String operatorName = getOperatorType(operator);
        if (operatorName.equals("Select")) {
            Select select = (Select) operator;
            Predicate predicate = select.getPredicate();
            List<Attribute> selectAttrs = select.getOutput().getAttributes();
            List<Attribute> newProjectAttrs = selectAttrs.stream().filter(allProjectAttributes::contains).collect(Collectors.toList());
            boolean equalLists = newProjectAttrs.containsAll(selectAttrs) && selectAttrs.containsAll(newProjectAttrs);
            allProjectAttributes.add(predicate.getLeftAttribute());
            if(!predicate.equalsValue()){
                allProjectAttributes.add(predicate.getRightAttribute());
            }
            if(newProjectAttrs.size()>0 && !equalLists){
                if(selectAttrs.containsAll(newProjectAttrs)) {
                    Select newSelect = new Select(pushProjectsDown(select.getInput(),allProjectAttributes),select.getPredicate());
                    return acceptOperator(new Project(newSelect, newProjectAttrs));
                }
            }
            return acceptOperator(new Select(pushProjectsDown(select.getInput(),allProjectAttributes), select.getPredicate()));
        }else if(operatorName.equals("Project")){
            Project project = (Project) operator;
            allProjectAttributes.addAll(project.getAttributes());
            Operator input = project.getInput();
            if(getOperatorType(input).equals("Product")){
                return acceptOperator(new Project(pushProjectsDown(input,allProjectAttributes),project.getAttributes()));
            }else {
                return acceptOperator(pushProjectsDown(project.getInput(), allProjectAttributes));
            }
        }else if(operatorName.equals("Product")){
            Product product = (Product) operator;
            List<Attribute> productAttrs = product.getOutput().getAttributes();
            List<Attribute> newProjectAttrs = productAttrs.stream().filter(allProjectAttributes::contains).collect(Collectors.toList());
            boolean equalLists = newProjectAttrs.containsAll(productAttrs) && productAttrs.containsAll(newProjectAttrs);
            if(newProjectAttrs.size()>0 && !equalLists){
                if(productAttrs.containsAll(newProjectAttrs)) {
                    Product newProduct = new Product(pushProjectsDown(product.getLeft(),allProjectAttributes), pushProjectsDown(product.getRight(),allProjectAttributes));
                    return acceptOperator(new Project(newProduct, newProjectAttrs));
                }
            }
            return acceptOperator(new Product(pushProjectsDown(product.getLeft(),allProjectAttributes), pushProjectsDown(product.getRight(),allProjectAttributes)));
        }else if(operatorName.equals("Join")){
            Join join = (Join) operator;
            Predicate predicate = join.getPredicate();
            List<Attribute> joinAttrs = join.getOutput().getAttributes();
            List<Attribute> newProjectAttrs = joinAttrs.stream().filter(allProjectAttributes::contains).collect(Collectors.toList());
            boolean equalLists = newProjectAttrs.containsAll(joinAttrs) && joinAttrs.containsAll(newProjectAttrs);
            allProjectAttributes.add(predicate.getLeftAttribute());
            allProjectAttributes.add(predicate.getRightAttribute());
            if(newProjectAttrs.size()>0 && !equalLists){
                if(joinAttrs.containsAll(newProjectAttrs)) {
                    Join newJoin = new Join(pushProjectsDown(join.getLeft(),allProjectAttributes), pushProjectsDown(join.getRight(),allProjectAttributes), join.getPredicate());
                    return acceptOperator(new Project(newJoin, newProjectAttrs));
                }
            }
            return acceptOperator(new Join(pushProjectsDown(join.getLeft(),allProjectAttributes), pushProjectsDown(join.getRight(),allProjectAttributes), join.getPredicate()));
        }else if (operatorName.equals("Scan")) {
            Scan scan = (Scan) operator;
            List<Attribute> scanAttrs = operator.getOutput().getAttributes();
            List<Attribute> newProjectAttrs = scanAttrs.stream().filter(allProjectAttributes::contains).collect(Collectors.toList());
            boolean equalLists = newProjectAttrs.containsAll(scanAttrs) && scanAttrs.containsAll(newProjectAttrs);
            if(newProjectAttrs.size()>0 && !equalLists) {
                Scan newScan = new Scan((NamedRelation) scan.getRelation());
                return acceptOperator(new Project(newScan, newProjectAttrs));
            }
            return acceptOperator(new Scan((NamedRelation) scan.getRelation()));
        }
        return acceptOperator(operator);
    }

    // Method that accepts a newly created operator so that its output attributes can be used
    private Operator acceptOperator(Operator operator) {
        Estimator estimator = new Estimator();
        operator.accept(estimator);
        return operator;
    }

    // Method that returns the type of an operator
    private String getOperatorType(Operator operator){
        return operator.getClass().getName().split("\\.")[1];
    }
}