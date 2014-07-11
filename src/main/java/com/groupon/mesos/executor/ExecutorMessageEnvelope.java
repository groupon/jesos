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

import com.google.protobuf.Message;
import com.groupon.mesos.util.AbstractMessageEnvelope;
import com.groupon.mesos.util.UPID;

import mesos.internal.Messages.ExecutorRegisteredMessage;
import mesos.internal.Messages.ExecutorReregisteredMessage;
import mesos.internal.Messages.FrameworkToExecutorMessage;
import mesos.internal.Messages.KillTaskMessage;
import mesos.internal.Messages.ReconnectExecutorMessage;
import mesos.internal.Messages.RunTaskMessage;
import mesos.internal.Messages.ShutdownExecutorMessage;
import mesos.internal.Messages.StatusUpdateAcknowledgementMessage;

/**
 * Executor related messages.
 */
public abstract class ExecutorMessageEnvelope<T extends Message> extends AbstractMessageEnvelope<T>
{
    protected ExecutorMessageEnvelope(final UPID sender, final UPID recipient, final T message)
    {
        super(sender, recipient, message);
    }

    /**
     * Send a message to the Mesos framework.
     */
    public static class RemoteMessageEnvelope extends ExecutorMessageEnvelope<Message>
    {
        public RemoteMessageEnvelope(final UPID sender, final UPID recipient, final Message message)
        {
            super(sender, recipient, message);
        }
    }

    public static class ExecutorRegisteredMessageEnvelope extends ExecutorMessageEnvelope<ExecutorRegisteredMessage>
    {
        public ExecutorRegisteredMessageEnvelope(final UPID sender, final UPID recipient, final ExecutorRegisteredMessage message)
        {
            super(sender, recipient, message);
        }
    }

    public static class ExecutorReregisteredMessageEnvelope extends ExecutorMessageEnvelope<ExecutorReregisteredMessage>
    {
        public ExecutorReregisteredMessageEnvelope(final UPID sender, final UPID recipient, final ExecutorReregisteredMessage message)
        {
            super(sender, recipient, message);
        }
    }

    public static class ReconnectExecutorMessageEnvelope extends ExecutorMessageEnvelope<ReconnectExecutorMessage>
    {
        public ReconnectExecutorMessageEnvelope(final UPID sender, final UPID recipient, final ReconnectExecutorMessage message)
        {
            super(sender, recipient, message);
        }
    }

    public static class RunTaskMessageEnvelope extends ExecutorMessageEnvelope<RunTaskMessage>
    {
        public RunTaskMessageEnvelope(final UPID sender, final UPID recipient, final RunTaskMessage message)
        {
            super(sender, recipient, message);
        }
    }

    public static class KillTaskMessageEnvelope extends ExecutorMessageEnvelope<KillTaskMessage>
    {
        public KillTaskMessageEnvelope(final UPID sender, final UPID recipient, final KillTaskMessage message)
        {
            super(sender, recipient, message);
        }
    }

    public static class StatusUpdateAcknowledgementMessageEnvelope extends ExecutorMessageEnvelope<StatusUpdateAcknowledgementMessage>
    {
        public StatusUpdateAcknowledgementMessageEnvelope(final UPID sender, final UPID recipient, final StatusUpdateAcknowledgementMessage message)
        {
            super(sender, recipient, message);
        }
    }

    public static class FrameworkToExecutorMessageEnvelope extends ExecutorMessageEnvelope<FrameworkToExecutorMessage>
    {
        public FrameworkToExecutorMessageEnvelope(final UPID sender, final UPID recipient, final FrameworkToExecutorMessage message)
        {
            super(sender, recipient, message);
        }
    }

    public static class ShutdownExecutorMessageEnvelope extends ExecutorMessageEnvelope<ShutdownExecutorMessage>
    {
        public ShutdownExecutorMessageEnvelope(final UPID sender, final UPID recipient, final ShutdownExecutorMessage message)
        {
            super(sender, recipient, message);
        }
    }
}
