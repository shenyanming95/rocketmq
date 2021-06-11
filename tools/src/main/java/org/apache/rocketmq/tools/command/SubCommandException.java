package org.apache.rocketmq.tools.command;

public class SubCommandException extends Exception {
    private static final long serialVersionUID = 0L;

    /**
     * @param msg Message.
     */
    public SubCommandException(String msg) {
        super(msg);
    }

    /**
     * @param msg   Message.
     * @param cause Cause.
     */
    public SubCommandException(String msg, Throwable cause) {
        super(msg, cause);
    }
}
