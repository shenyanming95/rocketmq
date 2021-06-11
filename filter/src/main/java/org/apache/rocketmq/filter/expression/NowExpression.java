package org.apache.rocketmq.filter.expression;

/**
 * Current time expression.Just for test.
 */
public class NowExpression extends ConstantExpression {
    public NowExpression() {
        super("now");
    }

    @Override
    public Object evaluate(EvaluationContext context) throws Exception {
        return new Long(System.currentTimeMillis());
    }

    public Object getValue() {
        return new Long(System.currentTimeMillis());
    }
}
