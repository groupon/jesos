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
package com.groupon.mesos.executor;

import static java.lang.String.format;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import static org.apache.mesos.Protos.Status.DRIVER_ABORTED;
import static org.apache.mesos.Protos.Status.DRIVER_NOT_STARTED;
import static org.apache.mesos.Protos.Status.DRIVER_RUNNING;
import static org.apache.mesos.Protos.Status.DRIVER_STOPPED;
import static org.apache.mesos.Protos.TaskState.TASK_STAGING;

import java.io.Closeable;
import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;

import com.google.common.base.Throwables;
import com.google.common.eventbus.Subscribe;
import com.google.common.io.Closer;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import com.groupon.mesos.executor.ExecutorMessageEnvelope.RemoteMessageEnvelope;
import com.groupon.mesos.util.CloseableExecutors;
import com.groupon.mesos.util.HttpProtocolReceiver;
import com.groupon.mesos.util.HttpProtocolSender;
import com.groupon.mesos.util.Log;
import com.groupon.mesos.util.ManagedEventBus;
import com.groupon.mesos.util.NetworkUtil;
import com.groupon.mesos.util.TimeUtil;
import com.groupon.mesos.util.UPID;
import com.groupon.mesos.util.UUIDUtil;

import org.apache.mesos.Executor;
import org.apache.mesos.ExecutorDriver;
import org.apache.mesos.Protos.ExecutorID;
import org.apache.mesos.Protos.FrameworkID;
import org.apache.mesos.Protos.SlaveID;
import org.apache.mesos.Protos.Status;
import org.apache.mesos.Protos.TaskStatus;

import mesos.internal.Messages.ExecutorToFrameworkMessage;
import mesos.internal.Messages.RegisterExecutorMessage;
import mesos.internal.Messages.StatusUpdate;
import mesos.internal.Messages.StatusUpdateMessage;

public abstract class InternalExecutorDriver
    implements ExecutorDriver, Closeable
{
    private static final Log LOG = Log.getLog(InternalExecutorDriver.class);

    private final Executor executor;

    private final HttpProtocolReceiver receiver;
    private final HttpProtocolSender sender;

    private final ScheduledExecutorService callbackExecutor;

    private final ManagedEventBus eventBus;

    private final LocalExecutorMessageProcessor localMessageProcessor;

    private final Closer closer = Closer.create();

    private final ExecutorDriverContext context;

    protected InternalExecutorDriver(final Executor executor,
                                     final UPID slaveUpid,
                                     final SlaveID slaveId,
                                     final FrameworkID frameworkId,
                                     final ExecutorID executorId) throws IOException
    {
        this.executor = checkNotNull(executor, "executor is null");

        checkNotNull(slaveUpid, "slaveUpid is null");
        checkNotNull(slaveId, "slaveId is null");
        checkNotNull(frameworkId, "frameworkId is null");
        checkNotNull(executorId, "executorId is null");

        LOG.debug("Slave UPID:       %s", slaveUpid.asString());
        LOG.debug("Slave ID:         %s", slaveId.getValue());
        LOG.debug("Framework ID:     %s", frameworkId.getValue());
        LOG.debug("Executor ID:      %s", executorId.getValue());

        // Enforce using of the IP, when using the hostname this might "flap" between IPs which in turn
        // confuses the heck out of Mesos.
        final String hostName = NetworkUtil.findPublicIp();

        LOG.debug("Host name:        %s", hostName);

        this.context = new ExecutorDriverContext(hostName, slaveUpid, slaveId, frameworkId, executorId);

        this.eventBus = new ManagedEventBus("executor");

        this.localMessageProcessor = new LocalExecutorMessageProcessor(context, eventBus);

        // Closer closes in reverse registration order.

        // Close the callback executor last, so that everything that was still scheduled to be delivered to the framework still has a chance.
        this.callbackExecutor = closer.register(CloseableExecutors.decorate(Executors.newScheduledThreadPool(5, new ThreadFactoryBuilder().setDaemon(true).setNameFormat("executor-callback-%d").build())));

        this.receiver = closer.register(new HttpProtocolReceiver(context.getDriverUPID(), ExecutorMessageEnvelope.class, eventBus));

        // The sender is closed before the receiver, so that possible responses are still caught
        this.sender = closer.register(new HttpProtocolSender(context.getDriverUPID()));

        // Make sure that the event bus is drained first at shutdown.
        closer.register(eventBus);
    }

    @Override
    public void close() throws IOException
    {
        stop();
    }

    private void driverStart()
    {
        eventBus.register(this);
        eventBus.register(localMessageProcessor);

        this.receiver.start();
    }

    //
    // ========================================================================
    //
    // Mesos ExecutorDriver API
    //
    // ========================================================================
    //

    @Override
    public Status start()
    {
        if (!context.isStateMachine(DRIVER_NOT_STARTED)) {
            return context.getStateMachine();
        }

        try {
            driverStart();

            //
            // Register with Mesos Slave
            //
            final RegisterExecutorMessage message = RegisterExecutorMessage.newBuilder()
                .setFrameworkId(context.getFrameworkId())
                .setExecutorId(context.getExecutorId())
                .build();

            eventBus.post(new RemoteMessageEnvelope(context.getDriverUPID(), context.getSlaveUPID(), message));

            context.setStateMachine(DRIVER_RUNNING);
        }
        catch (final Exception e) {
            context.setStateMachine(DRIVER_ABORTED);
            LOG.error(e, "Failed to create executor process for '%s'", context.getSlaveUPID());

            eventBus.post(new ExecutorCallback() {
                @Override
                public Runnable getCallback(final Executor executor, final ExecutorDriver executorDriver)
                {
                    return new Runnable() {
                        @Override
                        public void run()
                        {
                            final String message = format("Failed to create scheduler process for '%s': %s", context.getSlaveUPID(), e.getMessage());
                            LOG.debug("calling error(driver, %s)", message);
                            executor.error(executorDriver, message);
                        }
                    };
                }
            });
        }

        return context.getStateMachine();
    }

    @Override
    public Status stop()
    {
        Status status = context.getStateMachine();

        if (!context.isStateMachine(DRIVER_RUNNING, DRIVER_ABORTED)) {
            return status;
        }

        try {
            closer.close();
        }
        catch (final IOException e) {
            LOG.warn(e, "While stopping");
        }

        context.setStateMachine(DRIVER_STOPPED);

        // If the driver was aborted, preserve that
        // state on the return.
        if (status != DRIVER_ABORTED) {
            status = DRIVER_STOPPED;
        }

        return status;
    }

    @Override
    public Status abort()
    {
        if (!context.isStateMachine(DRIVER_RUNNING)) {
            return context.getStateMachine();
        }

        context.setStateMachine(DRIVER_ABORTED);

        return context.getStateMachine();
    }

    @Override
    @SuppressWarnings("PMD.PreserveStackTrace")
    public Status join()
    {
        if (!context.isStateMachine(DRIVER_RUNNING)) {
            return context.getStateMachine();
        }

        final Future<Status> statusFuture = context.waitForStateChange(DRIVER_RUNNING);
        try {
            return statusFuture.get();
        }
        catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        catch (final ExecutionException e) {
            final Throwable t = e.getCause();
            throw Throwables.propagate(t);
        }

        return context.getStateMachine();
    }

    @Override
    public Status run()
    {
        start();

        if (context.isStateMachine(DRIVER_RUNNING)) {
            join();
        }

        return context.getStateMachine();
    }

    @Override
    public Status sendStatusUpdate(final TaskStatus taskStatus)
    {
        checkNotNull(taskStatus, "status is null");

        if (!context.isStateMachine(DRIVER_RUNNING)) {
            return context.getStateMachine();
        }

        if (taskStatus.getState() == TASK_STAGING) {
            LOG.error("Executor is not allowed to send TASK_STAGING status update. Aborting!");

            eventBus.post(new ExecutorCallback() {
                @Override
                public Runnable getCallback(final Executor executor, final ExecutorDriver executorDriver)
                {
                    return new Runnable() {
                        @Override
                        public void run()
                        {
                            executorDriver.abort();

                            final String message = "Executor is not allowed to send TASK_STAGING status update. Aborting!";
                            LOG.debug("calling error(driver, %s)", message);
                            executor.error(executorDriver, message);
                        }
                    };
                }
            });

            return context.getStateMachine();
        }

        final UUID uuid = UUID.randomUUID();

        final long now = TimeUtil.currentTime();
        final StatusUpdateMessage message = StatusUpdateMessage.newBuilder()
            .setPid(context.getDriverUPID().asString())
            .setUpdate(StatusUpdate.newBuilder()
                .setFrameworkId(context.getFrameworkId())
                .setExecutorId(context.getExecutorId())
                .setSlaveId(context.getSlaveId())
                .setStatus(TaskStatus.newBuilder(taskStatus).setTimestamp(now))
                .setTimestamp(now)
                .setUuid(UUIDUtil.uuidBytes(UUID.randomUUID())))
            .build();

        context.addUpdate(uuid, message.getUpdate());

        eventBus.post(new RemoteMessageEnvelope(context.getDriverUPID(), context.getSlaveUPID(), message));

        return context.getStateMachine();
    }

    @Override
    public Status sendFrameworkMessage(final byte[] data)
    {
        checkNotNull(data, "data is null");

        if (!context.isStateMachine(DRIVER_RUNNING)) {
            return context.getStateMachine();
        }

        final ExecutorToFrameworkMessage message = ExecutorToFrameworkMessage.newBuilder()
            .setSlaveId(context.getSlaveId())
            .setFrameworkId(context.getFrameworkId())
            .setExecutorId(context.getExecutorId())
            .setData(ByteString.copyFrom(data))
            .build();

        eventBus.post(new RemoteMessageEnvelope(context.getDriverUPID(), context.getSlaveUPID(), message));

        return context.getStateMachine();
    }

    /**
     * Remote message delivery. Called by the event bus on an event bus thread to transfer a message
     * to another mesos host. This must <b>not</b> called directly to ensure the thread separation between
     * caller threads, event bus threads and executor threads.
     */
    @Subscribe
    public void sendMessage(final RemoteMessageEnvelope envelope) throws Exception
    {
        final Message message = envelope.getMessage();
        final UPID recipient = envelope.getRecipient();

        checkState(!recipient.equals(context.getDriverUPID()), "Received a message with local recipient! (%s)", message);

        sender.sendHttpMessage(recipient, message);
    }

    //
    // Executor callback delivery.
    //

    @Subscribe
    public void processExecutorCallback(final ExecutorCallback callback)
    {
        callbackExecutor.submit(callback.getCallback(executor, this));
    }
}
