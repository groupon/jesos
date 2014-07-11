/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.groupon.mesos.util;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.Closeable;
import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Decorates {@link ExecutorService} and {@link ScheduledExecutorService} with Closeable
 * to allow throwing them into a {@link com.google.common.io.Closer}.
 */
public final class CloseableExecutors
{
    private CloseableExecutors()
    {
        throw new AssertionError("do not instantiate");
    }

    public static CloseableExecutorServiceDecorator decorate(final ExecutorService service)
    {
        return new CloseableExecutorServiceDecorator(service);
    }

    public static CloseableScheduledExecutorServiceDecorator decorate(final ScheduledExecutorService service)
    {
        return new CloseableScheduledExecutorServiceDecorator(service);
    }

    private static class CloseableExecutorServiceDecorator
        extends AbstractExecutorService
        implements Closeable
    {
        private final ExecutorService delegate;

        CloseableExecutorServiceDecorator(final ExecutorService delegate)
        {
            this.delegate = checkNotNull(delegate, "delegate is null");
        }

        @Override
        public void close()
        {
            delegate.shutdown();
        }

        @Override
        public boolean awaitTermination(final long timeout, final TimeUnit unit)
            throws InterruptedException
        {
            return delegate.awaitTermination(timeout, unit);
        }

        @Override
        public boolean isShutdown()
        {
            return delegate.isShutdown();
        }

        @Override
        public boolean isTerminated()
        {
            return delegate.isTerminated();
        }

        @Override
        public void shutdown()
        {
            delegate.shutdown();
        }

        @Override
        public List<Runnable> shutdownNow()
        {
            return delegate.shutdownNow();
        }

        @Override
        public void execute(final Runnable command)
        {
            delegate.execute(command);
        }
    }

    private static class CloseableScheduledExecutorServiceDecorator
        extends CloseableExecutorServiceDecorator
        implements ScheduledExecutorService
    {
        private final ScheduledExecutorService delegate;

        CloseableScheduledExecutorServiceDecorator(final ScheduledExecutorService delegate)
        {
            super(delegate);
            this.delegate = delegate;
        }

        @Override
        public void close()
        {
            delegate.shutdown();
        }

        @Override
        public ScheduledFuture<?> schedule(final Runnable command, final long delay, final TimeUnit unit)
        {
            return delegate.schedule(command, delay, unit);
        }

        @Override
        public <V> ScheduledFuture<V> schedule(final Callable<V> callable, final long delay, final TimeUnit unit)
        {
            return delegate.schedule(callable, delay, unit);
        }

        @Override
        public ScheduledFuture<?> scheduleAtFixedRate(final Runnable command, final long initialDelay, final long period, final TimeUnit unit)
        {
            return delegate.scheduleAtFixedRate(command, initialDelay, period, unit);
        }

        @Override
        public ScheduledFuture<?> scheduleWithFixedDelay(final Runnable command, final long initialDelay, final long delay, final TimeUnit unit)
        {
            return delegate.scheduleWithFixedDelay(command, initialDelay, delay, unit);
        }
    }

}
