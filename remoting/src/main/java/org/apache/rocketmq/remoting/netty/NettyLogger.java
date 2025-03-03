package org.apache.rocketmq.remoting.netty;


import io.netty.util.internal.logging.InternalLogLevel;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;

public class NettyLogger {

    private static AtomicBoolean nettyLoggerSeted = new AtomicBoolean(false);

    private static InternalLogLevel nettyLogLevel = InternalLogLevel.ERROR;

    public static void initNettyLogger() {
        if (!nettyLoggerSeted.get()) {
            try {
                io.netty.util.internal.logging.InternalLoggerFactory.setDefaultFactory(new NettyBridgeLoggerFactory());
            } catch (Throwable e) {
                //ignore
            }
            nettyLoggerSeted.set(true);
        }
    }

    private static class NettyBridgeLoggerFactory extends io.netty.util.internal.logging.InternalLoggerFactory {
        @Override
        protected io.netty.util.internal.logging.InternalLogger newInstance(String s) {
            return new NettyBridgeLogger(s);
        }
    }

    private static class NettyBridgeLogger implements io.netty.util.internal.logging.InternalLogger {

        private InternalLogger logger = null;

        public NettyBridgeLogger(String name) {
            logger = InternalLoggerFactory.getLogger(name);
        }

        @Override
        public String name() {
            return logger.getName();
        }

        @Override
        public boolean isEnabled(InternalLogLevel internalLogLevel) {
            return nettyLogLevel.ordinal() <= internalLogLevel.ordinal();
        }

        @Override
        public void log(InternalLogLevel internalLogLevel, String s) {
            if (internalLogLevel.equals(InternalLogLevel.DEBUG)) {
                logger.debug(s);
            }
            if (internalLogLevel.equals(InternalLogLevel.TRACE)) {
                logger.info(s);
            }
            if (internalLogLevel.equals(InternalLogLevel.INFO)) {
                logger.info(s);
            }
            if (internalLogLevel.equals(InternalLogLevel.WARN)) {
                logger.warn(s);
            }
            if (internalLogLevel.equals(InternalLogLevel.ERROR)) {
                logger.error(s);
            }
        }

        @Override
        public void log(InternalLogLevel internalLogLevel, String s, Object o) {
            if (internalLogLevel.equals(InternalLogLevel.DEBUG)) {
                logger.debug(s, o);
            }
            if (internalLogLevel.equals(InternalLogLevel.TRACE)) {
                logger.info(s, o);
            }
            if (internalLogLevel.equals(InternalLogLevel.INFO)) {
                logger.info(s, o);
            }
            if (internalLogLevel.equals(InternalLogLevel.WARN)) {
                logger.warn(s, o);
            }
            if (internalLogLevel.equals(InternalLogLevel.ERROR)) {
                logger.error(s, o);
            }
        }

        @Override
        public void log(InternalLogLevel internalLogLevel, String s, Object o, Object o1) {
            if (internalLogLevel.equals(InternalLogLevel.DEBUG)) {
                logger.debug(s, o, o1);
            }
            if (internalLogLevel.equals(InternalLogLevel.TRACE)) {
                logger.info(s, o, o1);
            }
            if (internalLogLevel.equals(InternalLogLevel.INFO)) {
                logger.info(s, o, o1);
            }
            if (internalLogLevel.equals(InternalLogLevel.WARN)) {
                logger.warn(s, o, o1);
            }
            if (internalLogLevel.equals(InternalLogLevel.ERROR)) {
                logger.error(s, o, o1);
            }
        }

        @Override
        public void log(InternalLogLevel internalLogLevel, String s, Object... objects) {
            if (internalLogLevel.equals(InternalLogLevel.DEBUG)) {
                logger.debug(s, objects);
            }
            if (internalLogLevel.equals(InternalLogLevel.TRACE)) {
                logger.info(s, objects);
            }
            if (internalLogLevel.equals(InternalLogLevel.INFO)) {
                logger.info(s, objects);
            }
            if (internalLogLevel.equals(InternalLogLevel.WARN)) {
                logger.warn(s, objects);
            }
            if (internalLogLevel.equals(InternalLogLevel.ERROR)) {
                logger.error(s, objects);
            }
        }

        @Override
        public void log(InternalLogLevel internalLogLevel, String s, Throwable throwable) {
            if (internalLogLevel.equals(InternalLogLevel.DEBUG)) {
                logger.debug(s, throwable);
            }
            if (internalLogLevel.equals(InternalLogLevel.TRACE)) {
                logger.info(s, throwable);
            }
            if (internalLogLevel.equals(InternalLogLevel.INFO)) {
                logger.info(s, throwable);
            }
            if (internalLogLevel.equals(InternalLogLevel.WARN)) {
                logger.warn(s, throwable);
            }
            if (internalLogLevel.equals(InternalLogLevel.ERROR)) {
                logger.error(s, throwable);
            }
        }

        @Override
        public boolean isTraceEnabled() {
            return isEnabled(InternalLogLevel.TRACE);
        }

        @Override
        public void trace(String var1) {
            logger.info(var1);
        }

        @Override
        public void trace(String var1, Object var2) {
            logger.info(var1, var2);
        }

        @Override
        public void trace(String var1, Object var2, Object var3) {
            logger.info(var1, var2, var3);
        }

        @Override
        public void trace(String var1, Object... var2) {
            logger.info(var1, var2);
        }

        @Override
        public void trace(String var1, Throwable var2) {
            logger.info(var1, var2);
        }

        @Override
        public boolean isDebugEnabled() {
            return isEnabled(InternalLogLevel.DEBUG);
        }

        @Override
        public void debug(String var1) {
            logger.debug(var1);
        }

        @Override
        public void debug(String var1, Object var2) {
            logger.debug(var1, var2);
        }

        @Override
        public void debug(String var1, Object var2, Object var3) {
            logger.debug(var1, var2, var3);
        }

        @Override
        public void debug(String var1, Object... var2) {
            logger.debug(var1, var2);
        }

        @Override
        public void debug(String var1, Throwable var2) {
            logger.debug(var1, var2);
        }

        @Override
        public boolean isInfoEnabled() {
            return isEnabled(InternalLogLevel.INFO);
        }

        @Override
        public void info(String var1) {
            logger.info(var1);
        }

        @Override
        public void info(String var1, Object var2) {
            logger.info(var1, var2);
        }

        @Override
        public void info(String var1, Object var2, Object var3) {
            logger.info(var1, var2, var3);
        }

        @Override
        public void info(String var1, Object... var2) {
            logger.info(var1, var2);
        }

        @Override
        public void info(String var1, Throwable var2) {
            logger.info(var1, var2);
        }

        @Override
        public boolean isWarnEnabled() {
            return isEnabled(InternalLogLevel.WARN);
        }

        @Override
        public void warn(String var1) {
            logger.warn(var1);
        }

        @Override
        public void warn(String var1, Object var2) {
            logger.warn(var1, var2);
        }

        @Override
        public void warn(String var1, Object... var2) {
            logger.warn(var1, var2);
        }

        @Override
        public void warn(String var1, Object var2, Object var3) {
            logger.warn(var1, var2, var3);
        }

        @Override
        public void warn(String var1, Throwable var2) {
            logger.warn(var1, var2);
        }

        @Override
        public boolean isErrorEnabled() {
            return isEnabled(InternalLogLevel.ERROR);
        }

        @Override
        public void error(String var1) {
            logger.error(var1);
        }

        @Override
        public void error(String var1, Object var2) {
            logger.error(var1, var2);
        }

        @Override
        public void error(String var1, Object var2, Object var3) {
            logger.error(var1, var2, var3);
        }

        @Override
        public void error(String var1, Object... var2) {
            logger.error(var1, var2);
        }

        @Override
        public void error(String var1, Throwable var2) {
            logger.error(var1, var2);
        }
    }

}
