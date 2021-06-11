package org.apache.rocketmq.filter.expression;

/**
 * A filter performing a comparison of two objects
 * <p>
 * This class was taken from ActiveMQ org.apache.activemq.filter.LogicExpression,
 * </p>
 */
public abstract class LogicExpression extends BinaryExpression implements BooleanExpression {

    /**
     * @param left
     * @param right
     */
    public LogicExpression(BooleanExpression left, BooleanExpression right) {
        super(left, right);
    }

    public static BooleanExpression createOR(BooleanExpression lvalue, BooleanExpression rvalue) {
        return new LogicExpression(lvalue, rvalue) {

            public Object evaluate(EvaluationContext context) throws Exception {

                Boolean lv = (Boolean) left.evaluate(context);
                if (lv != null && lv.booleanValue()) {
                    return Boolean.TRUE;
                }
                Boolean rv = (Boolean) right.evaluate(context);
                if (rv != null && rv.booleanValue()) {
                    return Boolean.TRUE;
                }
                if (lv == null || rv == null) {
                    return null;
                }
                return Boolean.FALSE;
            }

            public String getExpressionSymbol() {
                return "||";
            }
        };
    }

    public static BooleanExpression createAND(BooleanExpression lvalue, BooleanExpression rvalue) {
        return new LogicExpression(lvalue, rvalue) {

            public Object evaluate(EvaluationContext context) throws Exception {

                Boolean lv = (Boolean) left.evaluate(context);

                if (lv != null && !lv.booleanValue()) {
                    return Boolean.FALSE;
                }
                Boolean rv = (Boolean) right.evaluate(context);
                if (rv != null && !rv.booleanValue()) {
                    return Boolean.FALSE;
                }
                if (lv == null || rv == null) {
                    return null;
                }
                return Boolean.TRUE;
            }

            public String getExpressionSymbol() {
                return "&&";
            }
        };
    }

    public abstract Object evaluate(EvaluationContext context) throws Exception;

    public boolean matches(EvaluationContext context) throws Exception {
        Object object = evaluate(context);
        return object != null && object == Boolean.TRUE;
    }

}
