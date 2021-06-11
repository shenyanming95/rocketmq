package org.apache.rocketmq.common.filter.impl;

public abstract class Op {

    private String symbol;

    protected Op(String symbol) {
        this.symbol = symbol;
    }

    public String getSymbol() {
        return symbol;
    }

    public String toString() {
        return symbol;
    }
}
