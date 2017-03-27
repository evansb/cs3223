package qp.operators;

import qp.utils.Attribute;
import qp.utils.Batch;

import java.util.ArrayList;
import java.util.List;
import java.util.Vector;

public class OrderBy extends Operator {

    private final List<Attribute> attributes;
    private final boolean isAscending;
    private Operator base;

    public OrderBy(Operator base, Vector as, boolean isAscending, int type) {
        super(type);
        this.attributes = new ArrayList<>(as);
        this.base = base;
        this.isAscending = isAscending;
    }

    public boolean open() {
        System.out.println("Open ORDER BY");
        System.out.println("Ascending = " + this.isAscending);
        this.attributes.stream().forEach(att -> {
            System.out.println(att.getTabName() + "." + att.getColName());
        });
        return this.base.open();
    }

    public Batch next() {
        return this.base.next();
    }

    public boolean close() {
        return true;
    }

    public Operator getBase() {
        return this.base;
    }

    public void setBase(Operator base) {
        this.base = base;
    }
}
