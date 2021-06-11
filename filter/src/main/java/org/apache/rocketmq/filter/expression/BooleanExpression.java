package org.apache.rocketmq.filter.expression;

/**
 * A BooleanExpression is an expression that always
 * produces a Boolean result.
 * <p>
 * This class was taken from ActiveMQ org.apache.activemq.filter.BooleanExpression,
 * but the parameter is changed to an interface.
 * </p>
 *
 * @see org.apache.rocketmq.filter.expression.EvaluationContext
 */
public interface BooleanExpression extends Expression {

    /**
     * @return true if the expression evaluates to Boolean.TRUE.
     */
    boolean matches(EvaluationContext context) throws Exception;

}
