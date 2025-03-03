package org.apache.rocketmq.filter.expression;

import org.apache.rocketmq.filter.constant.UnaryType;

import java.math.BigDecimal;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

/**
 * An expression which performs an operation on two expression values
 * <p>
 * This class was taken from ActiveMQ org.apache.activemq.filter.UnaryExpression,
 * but:
 * 1. remove XPath and XQuery expression;
 * 2. Add constant UnaryType to distinguish different unary expression;
 * 3. Extract UnaryInExpression to an independent class.
 * </p>
 */
public abstract class UnaryExpression implements Expression {

    private static final BigDecimal BD_LONG_MIN_VALUE = BigDecimal.valueOf(Long.MIN_VALUE);
    public UnaryType unaryType;
    protected Expression right;

    public UnaryExpression(Expression left) {
        this.right = left;
    }

    public UnaryExpression(Expression left, UnaryType unaryType) {
        this.setUnaryType(unaryType);
        this.right = left;
    }

    public static Expression createNegate(Expression left) {
        return new UnaryExpression(left, UnaryType.NEGATE) {
            public Object evaluate(EvaluationContext context) throws Exception {
                Object rvalue = right.evaluate(context);
                if (rvalue == null) {
                    return null;
                }
                if (rvalue instanceof Number) {
                    return negate((Number) rvalue);
                }
                return null;
            }

            public String getExpressionSymbol() {
                return "-";
            }
        };
    }

    public static BooleanExpression createInExpression(PropertyExpression right, List<Object> elements, final boolean not) {

        // Use a HashSet if there are many elements.
        Collection<Object> t;
        if (elements.size() == 0) {
            t = null;
        } else if (elements.size() < 5) {
            t = elements;
        } else {
            t = new HashSet<Object>(elements);
        }
        final Collection inList = t;

        return new UnaryInExpression(right, UnaryType.IN, inList, not) {
            public Object evaluate(EvaluationContext context) throws Exception {

                Object rvalue = right.evaluate(context);
                if (rvalue == null) {
                    return null;
                }
                if (rvalue.getClass() != String.class) {
                    return null;
                }

                if ((inList != null && inList.contains(rvalue)) ^ not) {
                    return Boolean.TRUE;
                } else {
                    return Boolean.FALSE;
                }

            }

            public String toString() {
                StringBuffer answer = new StringBuffer();
                answer.append(right);
                answer.append(" ");
                answer.append(getExpressionSymbol());
                answer.append(" ( ");

                int count = 0;
                for (Iterator i = inList.iterator(); i.hasNext(); ) {
                    Object o = (Object) i.next();
                    if (count != 0) {
                        answer.append(", ");
                    }
                    answer.append(o);
                    count++;
                }

                answer.append(" )");
                return answer.toString();
            }

            public String getExpressionSymbol() {
                if (not) {
                    return "NOT IN";
                } else {
                    return "IN";
                }
            }
        };
    }

    public static BooleanExpression createNOT(BooleanExpression left) {
        return new BooleanUnaryExpression(left, UnaryType.NOT) {
            public Object evaluate(EvaluationContext context) throws Exception {
                Boolean lvalue = (Boolean) right.evaluate(context);
                if (lvalue == null) {
                    return null;
                }
                return lvalue.booleanValue() ? Boolean.FALSE : Boolean.TRUE;
            }

            public String getExpressionSymbol() {
                return "NOT";
            }
        };
    }

    public static BooleanExpression createBooleanCast(Expression left) {
        return new BooleanUnaryExpression(left, UnaryType.BOOLEANCAST) {
            public Object evaluate(EvaluationContext context) throws Exception {
                Object rvalue = right.evaluate(context);
                if (rvalue == null) {
                    return null;
                }
                if (!rvalue.getClass().equals(Boolean.class)) {
                    return Boolean.FALSE;
                }
                return ((Boolean) rvalue).booleanValue() ? Boolean.TRUE : Boolean.FALSE;
            }

            public String toString() {
                return right.toString();
            }

            public String getExpressionSymbol() {
                return "";
            }
        };
    }

    private static Number negate(Number left) {
        Class clazz = left.getClass();
        if (clazz == Integer.class) {
            return new Integer(-left.intValue());
        } else if (clazz == Long.class) {
            return new Long(-left.longValue());
        } else if (clazz == Float.class) {
            return new Float(-left.floatValue());
        } else if (clazz == Double.class) {
            return new Double(-left.doubleValue());
        } else if (clazz == BigDecimal.class) {
            // We ussually get a big deciamal when we have Long.MIN_VALUE
            // constant in the
            // Selector. Long.MIN_VALUE is too big to store in a Long as a
            // positive so we store it
            // as a Big decimal. But it gets Negated right away.. to here we try
            // to covert it back
            // to a Long.
            BigDecimal bd = (BigDecimal) left;
            bd = bd.negate();

            if (BD_LONG_MIN_VALUE.compareTo(bd) == 0) {
                return Long.valueOf(Long.MIN_VALUE);
            }
            return bd;
        } else {
            throw new RuntimeException("Don't know how to negate: " + left);
        }
    }

    public Expression getRight() {
        return right;
    }

    public void setRight(Expression expression) {
        right = expression;
    }

    public UnaryType getUnaryType() {
        return unaryType;
    }

    public void setUnaryType(UnaryType unaryType) {
        this.unaryType = unaryType;
    }

    /**
     * @see Object#toString()
     */
    public String toString() {
        return "(" + getExpressionSymbol() + " " + right.toString() + ")";
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
     * Returns the symbol that represents this binary expression. For example,
     * addition is represented by "+"
     */
    public abstract String getExpressionSymbol();

    abstract static class BooleanUnaryExpression extends UnaryExpression implements BooleanExpression {
        public BooleanUnaryExpression(Expression left, UnaryType unaryType) {
            super(left, unaryType);
        }

        public boolean matches(EvaluationContext context) throws Exception {
            Object object = evaluate(context);
            return object != null && object == Boolean.TRUE;
        }
    }

}
