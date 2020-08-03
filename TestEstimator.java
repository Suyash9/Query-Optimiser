package sjdb;

import java.util.Arrays;

public class TestEstimator {
    private static Catalogue cat;
    private static Estimator est;

    public static void main (String[] args) throws DatabaseException {
        est = new Estimator();
        cat = new Catalogue();

        System.out.println("========  creating catalogue  ========");

        System.out.println("=== creating relations ===");

        cat.createRelation("A", 1000);
        cat.createRelation("B", 2000);
        cat.createRelation("C", 3000);

        System.out.println("=== creating attributes ===");

        cat.createAttribute("A", "a", 100);
        cat.createAttribute("A", "b", 200);
        cat.createAttribute("A", "c", 300);
        cat.createAttribute("B", "d", 300);
        cat.createAttribute("B", "e", 200);
        cat.createAttribute("B", "f", 100);
        cat.createAttribute("C", "g", 400);
        cat.createAttribute("C", "h", 500);
        cat.createAttribute("C", "i", 600);

        System.out.println("========  printing catalogue  ========\n");
        System.out.println(cat.getRelation("A").render());
        System.out.println(cat.getRelation("B").render());
        System.out.println(cat.getRelation("C").render());

        System.out.println("\n===========  testing select attr = val  ===========\n");

            // prepare operators
            Predicate pred = new Predicate(cat.getAttribute("a"), "a_1");
            Scan input = new Scan(cat.getRelation("A"));
            Select select_A_a_val = new Select(input, pred);

            // print info
            System.out.println("Input ===> " + input.getRelation().render());
            System.out.println("Pred ===> " + select_A_a_val.getPredicate());
            System.out.println("\n");

            // run estimator
            select_A_a_val.accept(est);

    }
}
