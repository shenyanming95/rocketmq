package org.apache.rocketmq.filter.expression;

/**
 * Represents a property expression
 * <p>
 * This class was taken from ActiveMQ org.apache.activemq.filter.PropertyExpression,
 * but more simple and no transfer between expression and message property.
 * </p>
 */
public class PropertyExpression implements Expression {
    private final String name;

    public PropertyExpression(String name) {
        this.name = name;
    }

    @Override
    public Object evaluate(EvaluationContext context) throws Exception {
        return context.get(name);
    }

    public String getName() {
        return name;
    }

    /**
     * @see Object#toString()
     */
    @Override
    public String toString() {
        return name;
    }

    /**
     * @see Object#hashCode()
     */
    @Override
    public int hashCode() {
        return name.hashCode();
    }

    /**
     * @see Object#equals(Object)
     */
    @Override
    public boolean equals(Object o) {

        if (o == null || !this.getClass().equals(o.getClass())) {
            return false;
        }
        return name.equals(((PropertyExpression) o).name);
    }
}
