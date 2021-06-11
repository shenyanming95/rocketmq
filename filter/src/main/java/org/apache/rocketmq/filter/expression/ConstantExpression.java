package org.apache.rocketmq.filter.expression;

/**
 * Represents a constant expression
 * <p>
 * This class was taken from ActiveMQ org.apache.activemq.filter.ConstantExpression,
 * but:
 * 1. For long type constant, the range bound by java Long type;
 * 2. For float type constant, the range bound by java Double type;
 * 3. Remove Hex and Octal expression;
 * 4. Add now expression to support to get current time.
 * </p>
 */
public class ConstantExpression implements Expression {

    private Object value;

    public ConstantExpression(Object value) {
        this.value = value;
    }

    public static ConstantExpression createFromDecimal(String text) {

        // Strip off the 'l' or 'L' if needed.
        if (text.endsWith("l") || text.endsWith("L")) {
            text = text.substring(0, text.length() - 1);
        }

        // only support Long.MIN_VALUE ~ Long.MAX_VALUE
        Number value = new Long(text);

        long l = value.longValue();
        if (Integer.MIN_VALUE <= l && l <= Integer.MAX_VALUE) {
            value = value.intValue();
        }
        return new ConstantExpression(value);
    }

    public static ConstantExpression createFloat(String text) {
        Double value = new Double(text);
        if (value > Double.MAX_VALUE) {
            throw new RuntimeException(text + " is greater than " + Double.MAX_VALUE);
        }
        if (value < Double.MIN_VALUE) {
            throw new RuntimeException(text + " is less than " + Double.MIN_VALUE);
        }
        return new ConstantExpression(value);
    }

    public static ConstantExpression createNow() {
        return new NowExpression();
    }

    /**
     * Encodes the value of string so that it looks like it would look like when
     * it was provided in a selector.
     */
    public static String encodeString(String s) {

        StringBuilder builder = new StringBuilder();

        builder.append('\'');
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '\'') {
                builder.append(c);
            }
            builder.append(c);
        }
        builder.append('\'');
        return builder.toString();
    }

    public Object evaluate(EvaluationContext context) throws Exception {
        return value;
    }

    public Object getValue() {
        return value;
    }

    /**
     * @see Object#toString()
     */
    public String toString() {
        Object value = getValue();
        if (value == null) {
            return "NULL";
        }
        if (value instanceof Boolean) {
            return (Boolean) value ? "TRUE" : "FALSE";
        }
        if (value instanceof String) {
            return encodeString((String) value);
        }
        return value.toString();
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

}
