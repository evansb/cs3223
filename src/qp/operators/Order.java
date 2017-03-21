package qp.operators;

import qp.utils.Attribute;

/**
 * Created by michaellimantara on 21/3/17.
 */
public class Order {

    public enum OrderType {
        ASC, DESC
    }

    private Attribute attribute;
    private OrderType orderType;


    public Order(Attribute attribute, OrderType orderType) {
        this.attribute = attribute;
        this.orderType = orderType;
    }

    public Attribute getAttribute() {
        return attribute;
    }

    public OrderType getOrderType() {
        return orderType;
    }
}
