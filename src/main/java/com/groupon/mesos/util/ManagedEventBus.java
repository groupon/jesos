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
import static com.google.common.base.Preconditions.checkState;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.eventbus.AsyncEventBus;
import com.google.common.eventbus.Subscribe;
import com.google.common.eventbus.SubscriberExceptionContext;
import com.google.common.eventbus.SubscriberExceptionHandler;
import com.google.common.util.concurrent.SettableFuture;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * As the event bus does not allow controlled shutdown, add the ability to "poison" the event bus and
 * wait for the pill to pass through it. It is assumed that the pill is the last event that the bus
 * processes and therefore when it is received, no additional events can be processed.
 */
public class ManagedEventBus implements Closeable
{
    private final AsyncEventBus eventBus;
    private AtomicBoolean finished = new AtomicBoolean(false);
    private AtomicReference<PoisonPill> pillHolder = new AtomicReference<>(new PoisonPill());

    private final ExecutorService executor;

    public ManagedEventBus(String name)
    {
        checkNotNull(name, "name is null");
        this.executor = Executors.newScheduledThreadPool(10, new ThreadFactoryBuilder().setDaemon(true).setNameFormat(name + "-%d").build());
        this.eventBus = new AsyncEventBus(executor, new EventBusExceptionHandler(name));
    }

    public void register(Object listener)
    {
        eventBus.register(listener);
    }

    public void post(Object event)
    {
        checkState(!finished.get(), "event bus is shut down");
        eventBus.post(event);
    }

    @Override
    public void close() throws IOException
    {
        if (finished.getAndSet(true)) {
            eventBus.register(this);
            eventBus.post(pillHolder.get());
        }
    }

    public void awaitTermination()
        throws InterruptedException
    {
        checkState(finished.get(), "event bus already finished");
        PoisonPill pill = pillHolder.getAndSet(null);
        if (pill != null) {
            pill.awaitTermination(1, TimeUnit.DAYS);
        }
        executor.awaitTermination(1, TimeUnit.DAYS);
    }

    @Subscribe
    public void receivePoisonPill(PoisonPill poisonPill)
    {
        // The poison pill made it through the event bus, so
        // all events that were present before are either delivered
        // or in flight. Shut down the executor now.
        executor.shutdown();
        poisonPill.trigger();
    }

    public static class PoisonPill
    {
        private final SettableFuture<Void> future = SettableFuture.create();

        public void trigger()
        {
            future.set(null);
        }

        public void awaitTermination(long timeout, TimeUnit unit)
            throws InterruptedException
        {
            try {
                future.get(timeout, unit);
            }
            catch (TimeoutException | ExecutionException e) {
                return; // do nothing.
            }
        }
    }

    /**
     * Simple exception handler that, unlike the default handler, does not swallow
     * the exception causing the error.
     */
    public static class EventBusExceptionHandler implements SubscriberExceptionHandler
    {
        public static final Log LOG = Log.getLog(EventBusExceptionHandler.class);

        private final String name;

        public EventBusExceptionHandler(String name)
        {
            this.name = checkNotNull(name, "name is null");
        }

        @Override
        public void handleException(Throwable e, SubscriberExceptionContext context)
        {
            LOG.error(e, "Could not call %s/%s on bus %s", context.getSubscriber().getClass().getSimpleName(), context.getSubscriberMethod().getName(), name);
        }
    }

}
