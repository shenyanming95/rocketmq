package org.apache.rocketmq.filter;

import org.apache.rocketmq.common.filter.ExpressionType;
import org.apache.rocketmq.filter.expression.EmptyEvaluationContext;
import org.apache.rocketmq.filter.expression.EvaluationContext;
import org.apache.rocketmq.filter.expression.Expression;
import org.apache.rocketmq.filter.expression.MQFilterException;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class FilterSpiTest {

    @Test
    public void testRegister() {
        FilterFactory.INSTANCE.register(new NothingFilter());

        Expression expr = null;
        try {
            expr = FilterFactory.INSTANCE.get("Nothing").compile("abc");
        } catch (MQFilterException e) {
            e.printStackTrace();
            assertThat(Boolean.FALSE).isTrue();
        }

        assertThat(expr).isNotNull();

        try {
            assertThat((Boolean) expr.evaluate(new EmptyEvaluationContext())).isTrue();
        } catch (Exception e) {
            e.printStackTrace();
            assertThat(Boolean.FALSE).isTrue();
        }
    }

    @Test
    public void testGet() {
        try {
            assertThat((Boolean) FilterFactory.INSTANCE.get(ExpressionType.SQL92).compile("a is not null and a > 0").evaluate(new EmptyEvaluationContext())).isFalse();
        } catch (Exception e) {
            e.printStackTrace();
            assertThat(Boolean.FALSE).isTrue();
        }
    }

    static class NothingExpression implements Expression {

        @Override
        public Object evaluate(final EvaluationContext context) throws Exception {
            return Boolean.TRUE;
        }
    }

    static class NothingFilter implements FilterSpi {
        @Override
        public Expression compile(final String expr) throws MQFilterException {
            return new NothingExpression();
        }

        @Override
        public String ofType() {
            return "Nothing";
        }
    }
}
