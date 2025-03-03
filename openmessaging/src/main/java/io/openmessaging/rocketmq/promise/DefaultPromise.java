package io.openmessaging.rocketmq.promise;

import io.openmessaging.FutureListener;
import io.openmessaging.Promise;
import io.openmessaging.exception.OMSRuntimeException;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class DefaultPromise<V> implements Promise<V> {
    private static final InternalLogger LOG = InternalLoggerFactory.getLogger(DefaultPromise.class);
    private final Object lock = new Object();
    private volatile FutureState state = FutureState.DOING;
    private V result = null;
    private long timeout;
    private long createTime;
    private Throwable exception = null;
    private List<FutureListener<V>> promiseListenerList;

    public DefaultPromise() {
        createTime = System.currentTimeMillis();
        promiseListenerList = new ArrayList<>();
        timeout = 5000;
    }

    @Override
    public boolean cancel(final boolean mayInterruptIfRunning) {
        return false;
    }

    @Override
    public boolean isCancelled() {
        return state.isCancelledState();
    }

    @Override
    public boolean isDone() {
        return state.isDoneState();
    }

    @Override
    public V get() {
        return result;
    }

    @Override
    public V get(final long timeout) {
        synchronized (lock) {
            if (!isDoing()) {
                return getValueOrThrowable();
            }

            if (timeout <= 0) {
                try {
                    lock.wait();
                } catch (Exception e) {
                    cancel(e);
                }
                return getValueOrThrowable();
            } else {
                long waitTime = timeout - (System.currentTimeMillis() - createTime);
                if (waitTime > 0) {
                    for (; ; ) {
                        try {
                            lock.wait(waitTime);
                        } catch (InterruptedException e) {
                            LOG.error("promise get value interrupted,excepiton:{}", e.getMessage());
                        }

                        if (!isDoing()) {
                            break;
                        } else {
                            waitTime = timeout - (System.currentTimeMillis() - createTime);
                            if (waitTime <= 0) {
                                break;
                            }
                        }
                    }
                }

                if (isDoing()) {
                    timeoutSoCancel();
                }
            }
            return getValueOrThrowable();
        }
    }

    @Override
    public boolean set(final V value) {
        if (value == null) return false;
        this.result = value;
        return done();
    }

    @Override
    public boolean setFailure(final Throwable cause) {
        if (cause == null) return false;
        this.exception = cause;
        return done();
    }

    @Override
    public void addListener(final FutureListener<V> listener) {
        if (listener == null) {
            throw new NullPointerException("FutureListener is null");
        }

        boolean notifyNow = false;
        synchronized (lock) {
            if (!isDoing()) {
                notifyNow = true;
            } else {
                if (promiseListenerList == null) {
                    promiseListenerList = new ArrayList<>();
                }
                promiseListenerList.add(listener);
            }
        }

        if (notifyNow) {
            notifyListener(listener);
        }
    }

    @Override
    public Throwable getThrowable() {
        return exception;
    }

    private void notifyListeners() {
        if (promiseListenerList != null) {
            for (FutureListener<V> listener : promiseListenerList) {
                notifyListener(listener);
            }
        }
    }

    private boolean isSuccess() {
        return isDone() && (exception == null);
    }

    private void timeoutSoCancel() {
        synchronized (lock) {
            if (!isDoing()) {
                return;
            }
            state = FutureState.CANCELLED;
            exception = new RuntimeException("Get request result is timeout or interrupted");
            lock.notifyAll();
        }
        notifyListeners();
    }

    private V getValueOrThrowable() {
        if (exception != null) {
            Throwable e = exception.getCause() != null ? exception.getCause() : exception;
            throw new OMSRuntimeException("-1", e);
        }
        notifyListeners();
        return result;
    }

    private boolean isDoing() {
        return state.isDoingState();
    }

    private boolean done() {
        synchronized (lock) {
            if (!isDoing()) {
                return false;
            }

            state = FutureState.DONE;
            lock.notifyAll();
        }

        notifyListeners();
        return true;
    }

    private void notifyListener(final FutureListener<V> listener) {
        try {
            listener.operationComplete(this);
        } catch (Throwable t) {
            LOG.error("notifyListener {} Error:{}", listener.getClass().getSimpleName(), t);
        }
    }

    private boolean cancel(Exception e) {
        synchronized (lock) {
            if (!isDoing()) {
                return false;
            }

            state = FutureState.CANCELLED;
            exception = e;
            lock.notifyAll();
        }

        notifyListeners();
        return true;
    }
}

