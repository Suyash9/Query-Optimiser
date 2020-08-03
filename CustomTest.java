package sjdb;

import java.io.*;
import java.util.ArrayList;

import com.sun.jdi.Value;
import sjdb.DatabaseException;

public class CustomTest {
    private Catalogue catalogue;

    public static void main(String[] args) throws Exception {
        Catalogue catalogue = createCatalogue();
        Inspector inspector = new Inspector();
        Estimator estimator = new Estimator();

        Operator plan = query(catalogue);
        plan.accept(estimator);
        //plan.accept(inspector);

        Optimiser optimiser = new Optimiser(catalogue);
        Operator planopt = optimiser.optimise(plan);
        System.out.println(planopt);
        //planopt.accept(estimator);
        //planopt.accept(inspector);

    }

    public static Catalogue createCatalogue() {
        Catalogue cat = new Catalogue();
        cat.createRelation("A", 200);
        cat.createAttribute("A", "a1", 100);
        cat.createAttribute("A", "a2", 15);
        cat.createAttribute("A", "a3", 5);
        cat.createRelation("B", 250);
        cat.createAttribute("B", "b1", 150);
        cat.createAttribute("B", "b2", 100);
        cat.createRelation("C", 100);
        cat.createAttribute("C", "c1", 150);
        cat.createAttribute("C", "c2", 100);

        return cat;
    }

    public static Operator query(Catalogue cat) throws Exception {
        Scan a = new Scan(cat.getRelation("A"));
        Scan b = new Scan(cat.getRelation("B"));
        Product p1 = new Product(a, b);
        Select s1 = new Select(p1, new Predicate(new Attribute("a2"), "5"));
        Select s2 = new Select(s1, new Predicate(new Attribute("a1"), new Attribute("b2")));
        ArrayList<Attribute> atts = new ArrayList<Attribute>();
        atts.add(new Attribute("a2"));
        Project p = new Project(s2, atts);

        return p;

        /*
        Scan a = new Scan(cat.getRelation("A"));
        Scan b = new Scan(cat.getRelation("B"));

        Product p1 = new Product(a, b);

        Select s1 = new Select(p1, new Predicate(new Attribute("a2"), "5"));

        ArrayList<Attribute> atts = new ArrayList<Attribute>();
        atts.add(new Attribute("a2"));
        //atts.add(new Attribute("b1"));

        Project plan = new Project(s1, atts);

        return plan;

        // Lecture example
        after join reordering
        Scan a = new Scan(cat.getRelation("A"));
        Scan b = new Scan(cat.getRelation("B"));
        Scan c = new Scan(cat.getRelation("C"));

        ArrayList<Attribute> atts = new ArrayList<>();
        //atts.add(new Attribute("a1"));
        atts.add(new Attribute("a3"));
        Product p1 = new Product(a, b);
        Select s1 = new Select(c, new Predicate(new Attribute("c2"), "5"));
        Select s2 = new Select(a, new Predicate(new Attribute("a2"), "5"));

        Join join = new Join(s1,b,new Predicate(new Attribute("c1"), new Attribute("b2")));
        Join join2 = new Join(join,s2,new Predicate(new Attribute("b1"), new Attribute("a1")));
        Project plan = new Project(join2, atts);
        return plan;

        // Lecture example for join reordering without select

        Scan a = new Scan(cat.getRelation("A"));
        Scan b = new Scan(cat.getRelation("B"));
        Scan c = new Scan(cat.getRelation("C"));
        Product p1 = new Product(a,b);
        Product p2 = new Product(p1, c);

        Select s1 = new Select(p1, new Predicate(new Attribute("a2"), new Attribute("b1")));

        ArrayList<Attribute> atts = new ArrayList<Attribute>();
        atts.add(new Attribute("a2"));
        atts.add(new Attribute("b1"));

        Project plan = new Project(p2, atts);
        return p2;

        // Lecture example for join reordering with select

        Scan a = new Scan(cat.getRelation("A"));
        Scan b = new Scan(cat.getRelation("B"));
        Scan c = new Scan(cat.getRelation("C"));
        Select s1 = new Select(a, new Predicate(new Attribute("a2"), "5"));
        Product p1 = new Product(s1,b);

        Select s2 = new Select(p1, new Predicate(new Attribute("a1"), new Attribute("b1")));
        Select s3 = new Select(c, new Predicate(new Attribute("c2"), "5"));
        Product p2 = new Product(s2, s3);

        Select s4 = new Select(p2, new Predicate(new Attribute("c1"), new Attribute("b2")));
        ArrayList<Attribute> atts = new ArrayList<Attribute>();
        atts.add(new Attribute("a2"));
        atts.add(new Attribute("b1"));

        Project plan = new Project(p2, atts);
        return s4;
         */
    }
}


