package org.apache.rocketmq.filter.expression;

import java.util.Map;

/**
 * Empty context.
 */
public class EmptyEvaluationContext implements EvaluationContext {
    @Override
    public Object get(String name) {
        return null;
    }

    @Override
    public Map<String, Object> keyValues() {
        return null;
    }
}
