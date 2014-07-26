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

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import static org.apache.mesos.Protos.Status.DRIVER_ABORTED;

import java.util.concurrent.ConcurrentMap;

import com.google.common.collect.Maps;
import com.google.common.eventbus.Subscribe;
import com.groupon.mesos.executor.ExecutorMessageEnvelope.ExecutorRegisteredMessageEnvelope;
import com.groupon.mesos.executor.ExecutorMessageEnvelope.ExecutorReregisteredMessageEnvelope;
import com.groupon.mesos.executor.ExecutorMessageEnvelope.FrameworkToExecutorMessageEnvelope;
import com.groupon.mesos.executor.ExecutorMessageEnvelope.KillTaskMessageEnvelope;
import com.groupon.mesos.executor.ExecutorMessageEnvelope.ReconnectExecutorMessageEnvelope;
import com.groupon.mesos.executor.ExecutorMessageEnvelope.RemoteMessageEnvelope;
import com.groupon.mesos.executor.ExecutorMessageEnvelope.RunTaskMessageEnvelope;
import com.groupon.mesos.executor.ExecutorMessageEnvelope.ShutdownExecutorMessageEnvelope;
import com.groupon.mesos.executor.ExecutorMessageEnvelope.StatusUpdateAcknowledgementMessageEnvelope;
import com.groupon.mesos.util.Log;
import com.groupon.mesos.util.ManagedEventBus;
import com.groupon.mesos.util.UUIDUtil;

import org.apache.mesos.Executor;
import org.apache.mesos.ExecutorDriver;
import org.apache.mesos.Protos.TaskID;
import org.apache.mesos.Protos.TaskInfo;

import mesos.internal.Messages.ExecutorRegisteredMessage;
import mesos.internal.Messages.ExecutorReregisteredMessage;
import mesos.internal.Messages.FrameworkToExecutorMessage;
import mesos.internal.Messages.KillTaskMessage;
import mesos.internal.Messages.ReconnectExecutorMessage;
import mesos.internal.Messages.ReregisterExecutorMessage;
import mesos.internal.Messages.RunTaskMessage;
import mesos.internal.Messages.StatusUpdateAcknowledgementMessage;

/**
 * Local Message processing. Accepts messages from the outside and deliver them to the local executor.
 */
class LocalExecutorMessageProcessor
{
    private static final Log LOG = Log.getLog(LocalExecutorMessageProcessor.class);

    private final ConcurrentMap<TaskID, TaskInfo> tasks = Maps.newConcurrentMap();

    private final ExecutorDriverContext context;
    private final ManagedEventBus eventBus;

    LocalExecutorMessageProcessor(final ExecutorDriverContext context,
                                  final ManagedEventBus eventBus)
    {
        this.context = checkNotNull(context, "context is null");
        this.eventBus = checkNotNull(eventBus, "eventBus is null");
    }

    @Subscribe
    public void executorRegistered(final ExecutorRegisteredMessageEnvelope envelope)
    {
        checkState(envelope.getRecipient().equals(context.getDriverUPID()), "Received a remote message for local delivery");

        if (context.isStateMachine(DRIVER_ABORTED)) {
            LOG.warn("driver is aborted!");
            return;
        }

        final ExecutorRegisteredMessage message = envelope.getMessage();

        eventBus.post(new ExecutorCallback() {
            @Override
            public Runnable getCallback(final Executor executor, final ExecutorDriver executorDriver)
            {
                return new Runnable() {
                    @Override
                    public void run()
                    {
                        executor.registered(executorDriver, message.getExecutorInfo(), message.getFrameworkInfo(), message.getSlaveInfo());
                    }

                    @Override
                    public String toString()
                    {
                        return "callback for registered()";
                    }
                };
            }
        });
    }

    @Subscribe
    public void executorReregistered(final ExecutorReregisteredMessageEnvelope envelope)
    {
        checkState(envelope.getRecipient().equals(context.getDriverUPID()), "Received a remote message for local delivery");

        if (context.isStateMachine(DRIVER_ABORTED)) {
            LOG.warn("driver is aborted!");
            return;
        }

        final ExecutorReregisteredMessage message = envelope.getMessage();

        eventBus.post(new ExecutorCallback() {
            @Override
            public Runnable getCallback(final Executor executor, final ExecutorDriver executorDriver)
            {
                return new Runnable() {
                    @Override
                    public void run()
                    {
                        executor.reregistered(executorDriver, message.getSlaveInfo());
                    }

                    @Override
                    public String toString()
                    {
                        return "callback for reregistered()";
                    }
                };
            }
        });
    }

    @Subscribe
    public void reconnectExecutor(final ReconnectExecutorMessageEnvelope envelope)
    {
        checkState(envelope.getRecipient().equals(context.getDriverUPID()), "Received a remote message for local delivery");

        if (context.isStateMachine(DRIVER_ABORTED)) {
            LOG.warn("driver is aborted!");
            return;
        }

        final ReconnectExecutorMessage message = envelope.getMessage();

        checkState(message.getSlaveId().equals(context.getSlaveId()), "Received reconnect from slave %s (expected %s)", message.getSlaveId().getValue(), context.getSlaveId().getValue());

        final ReregisterExecutorMessage.Builder builder = ReregisterExecutorMessage.newBuilder()
            .setExecutorId(context.getExecutorId())
            .setFrameworkId(context.getFrameworkId())
            .addAllUpdates(context.getUpdates())
            .addAllTasks(tasks.values());

        eventBus.post(new RemoteMessageEnvelope(context.getDriverUPID(), context.getSlaveUPID(), builder.build()));
    }

    @Subscribe
    public void runTask(final RunTaskMessageEnvelope envelope)
    {
        checkState(envelope.getRecipient().equals(context.getDriverUPID()), "Received a remote message for local delivery");

        if (context.isStateMachine(DRIVER_ABORTED)) {
            LOG.warn("driver is aborted!");
            return;
        }

        final RunTaskMessage message = envelope.getMessage();

        final TaskInfo task = message.getTask();

        checkState(!tasks.containsKey(task.getTaskId()), "Task %s already started!", task.getTaskId().getValue());

        tasks.put(task.getTaskId(), task);

        eventBus.post(new ExecutorCallback() {
            @Override
            public Runnable getCallback(final Executor executor, final ExecutorDriver executorDriver)
            {
                return new Runnable() {
                    @Override
                    public void run()
                    {
                        executor.launchTask(executorDriver, task);
                    }

                    @Override
                    public String toString()
                    {
                        return "callback for launchTask()";
                    }
                };
            }
        });
    }

    @Subscribe
    public void killTask(final KillTaskMessageEnvelope envelope)
    {
        checkState(envelope.getRecipient().equals(context.getDriverUPID()), "Received a remote message for local delivery");

        if (context.isStateMachine(DRIVER_ABORTED)) {
            LOG.warn("driver is aborted!");
            return;
        }

        final KillTaskMessage message = envelope.getMessage();

        eventBus.post(new ExecutorCallback() {
            @Override
            public Runnable getCallback(final Executor executor, final ExecutorDriver executorDriver)
            {
                return new Runnable() {
                    @Override
                    public void run()
                    {
                        executor.killTask(executorDriver, message.getTaskId());
                    }

                    @Override
                    public String toString()
                    {
                        return "callback for killTask()";
                    }
                };
            }
        });
    }

    @Subscribe
    public void statusUpdateAcknowledgement(final StatusUpdateAcknowledgementMessageEnvelope envelope)
    {
        checkState(envelope.getRecipient().equals(context.getDriverUPID()), "Received a remote message for local delivery");

        if (context.isStateMachine(DRIVER_ABORTED)) {
            LOG.warn("driver is aborted!");
            return;
        }

        final StatusUpdateAcknowledgementMessage message = envelope.getMessage();

        context.removeUpdate(UUIDUtil.bytesUuid(message.getUuid()));
        tasks.remove(message.getTaskId());
    }

    @Subscribe
    public void frameworkToExecutor(final FrameworkToExecutorMessageEnvelope envelope)
    {
        checkState(envelope.getRecipient().equals(context.getDriverUPID()), "Received a remote message for local delivery");

        if (context.isStateMachine(DRIVER_ABORTED)) {
            LOG.warn("driver is aborted!");
            return;
        }

        final FrameworkToExecutorMessage message = envelope.getMessage();

        eventBus.post(new ExecutorCallback() {
            @Override
            public Runnable getCallback(final Executor executor, final ExecutorDriver executorDriver)
            {
                return new Runnable() {
                    @Override
                    public void run()
                    {
                        executor.frameworkMessage(executorDriver, message.getData().toByteArray());
                    }

                    @Override
                    public String toString()
                    {
                        return "callback for frameworkMessage()";
                    }
                };
            }
        });
    }

    @Subscribe
    public void shutdownExecutor(final ShutdownExecutorMessageEnvelope envelope)
    {
        checkState(envelope.getRecipient().equals(context.getDriverUPID()), "Received a remote message for local delivery");

        if (context.isStateMachine(DRIVER_ABORTED)) {
            LOG.warn("driver is aborted!");
            return;
        }

        eventBus.post(new ExecutorCallback() {
            @Override
            public Runnable getCallback(final Executor executor, final ExecutorDriver executorDriver)
            {
                return new Runnable() {
                    @Override
                    public void run()
                    {
                        executorDriver.abort();
                        executor.shutdown(executorDriver);
                    }

                    @Override
                    public String toString()
                    {
                        return "callback for abort()";
                    }
                };
            }
        });
    }
}
