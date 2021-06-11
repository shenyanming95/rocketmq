package org.apache.rocketmq.filter.expression;

/**
 * An expression which performs an operation on two expression values.
 * <p>
 * This class was taken from ActiveMQ org.apache.activemq.filter.BinaryExpression,
 * </p>
 */
public abstract class BinaryExpression implements Expression {
    protected Expression left;
    protected Expression right;

    public BinaryExpression(Expression left, Expression right) {
        this.left = left;
        this.right = right;
    }

    public Expression getLeft() {
        return left;
    }

    /**
     * @param expression
     */
    public void setLeft(Expression expression) {
        left = expression;
    }

    public Expression getRight() {
        return right;
    }

    /**
     * @param expression
     */
    public void setRight(Expression expression) {
        right = expression;
    }

    /**
     * @see Object#toString()
     */
    public String toString() {
        return "(" + left.toString() + " " + getExpressionSymbol() + " " + right.toString() + ")";
    }

    /**
     * @see Object#hashCode()
     */
    public int hashCode() {
        return toString().hashCode();
    }

    /**
     * @see Object#equals(Object)
     */
    public boolean equals(Object o) {

        if (o == null || !this.getClass().equals(o.getClass())) {
            return false;
        }
        return toString().equals(o.toString());

    }

    /**
     * Returns the symbol that represents this binary expression.  For example, addition is
     * represented by "+"
     */
    public abstract String getExpressionSymbol();

}
