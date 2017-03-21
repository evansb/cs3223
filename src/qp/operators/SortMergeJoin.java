package qp.operators;

import qp.utils.Batch;
import qp.utils.Tuple;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by michaellimantara on 21/3/17.
 */
public class SortMergeJoin extends Join {

    public SortMergeJoin(Join join) {
        super(join.getLeft(), join.getRight(), join.getCondition(), join.getOpType());
    }

    @Override
    public boolean open() {
        List<Order> sortOrders = Arrays.asList(new Order(getCondition().getLhs(), Order.OrderType.ASC));
        ExternalSort sort = new ExternalSort(left, sortOrders, numBuff);
        if (sort.open()) {
            Batch batch = sort.next();
            ArrayList<Batch> results = new ArrayList<>();
            while (batch != null) {
                results.add(batch);
                batch = sort.next();
            }

            for (Batch b: results) {
                for (int i = 0; i < b.size(); i++) {
                    Tuple t = b.elementAt(i);
                    System.out.println(t.dataAt(0) + ", " + t.dataAt(1));
                }
            }
        }

        return true;
    }

    @Override
    public Batch next() {
        return super.next();
    }

    @Override
    public boolean close() {
        return super.close();
    }
}
