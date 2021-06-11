package org.apache.rocketmq.filter.expression;

import org.apache.rocketmq.filter.constant.UnaryType;

import java.util.Collection;

/**
 * In expression.
 */
abstract public class UnaryInExpression extends UnaryExpression implements BooleanExpression {

    private boolean not;

    private Collection inList;

    public UnaryInExpression(Expression left, UnaryType unaryType,
                             Collection inList, boolean not) {
        super(left, unaryType);
        this.setInList(inList);
        this.setNot(not);

    }

    public boolean matches(EvaluationContext context) throws Exception {
        Object object = evaluate(context);
        return object != null && object == Boolean.TRUE;
    }

    public boolean isNot() {
        return not;
    }

    public void setNot(boolean not) {
        this.not = not;
    }

    public Collection getInList() {
        return inList;
    }

    public void setInList(Collection inList) {
        this.inList = inList;
    }
}
