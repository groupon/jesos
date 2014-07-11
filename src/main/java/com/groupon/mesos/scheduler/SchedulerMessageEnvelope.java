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
package com.groupon.mesos.scheduler;

import com.google.protobuf.Message;
import com.groupon.mesos.util.AbstractMessageEnvelope;
import com.groupon.mesos.util.UPID;

import mesos.internal.Messages.ExecutorToFrameworkMessage;
import mesos.internal.Messages.FrameworkErrorMessage;
import mesos.internal.Messages.FrameworkRegisteredMessage;
import mesos.internal.Messages.FrameworkReregisteredMessage;
import mesos.internal.Messages.LostSlaveMessage;
import mesos.internal.Messages.RescindResourceOfferMessage;
import mesos.internal.Messages.ResourceOffersMessage;
import mesos.internal.Messages.StatusUpdateMessage;

/**
 * Scheduler related messages.
 */
public abstract class SchedulerMessageEnvelope<T extends Message> extends AbstractMessageEnvelope<T>
{
    protected SchedulerMessageEnvelope(final UPID sender, final UPID recipient, final T message)
    {
        super(sender, recipient, message);
    }

    /**
     * Send a message to the Mesos framework.
     */
    public static class RemoteMessageEnvelope extends SchedulerMessageEnvelope<Message>
    {
        public RemoteMessageEnvelope(final UPID sender, final UPID recipient, final Message message)
        {
            super(sender, recipient, message);
        }
    }

    /**
     * {@link mesos.internal.Messages.FrameworkRegisteredMessage} received from Mesos.
     */
    public static class FrameworkRegisteredMessageEnvelope extends SchedulerMessageEnvelope<FrameworkRegisteredMessage>
    {
        public FrameworkRegisteredMessageEnvelope(final UPID sender, final UPID recipient, final FrameworkRegisteredMessage message)
        {
            super(sender, recipient, message);
        }
    }

    /**
     * {@link mesos.internal.Messages.FrameworkReregisteredMessage} received from Mesos.
     */
    public static class FrameworkReregisteredMessageEnvelope extends SchedulerMessageEnvelope<FrameworkReregisteredMessage>
    {
        public FrameworkReregisteredMessageEnvelope(final UPID sender, final UPID recipient, final FrameworkReregisteredMessage message)
        {
            super(sender, recipient, message);
        }
    }

    /**
     * {@link mesos.internal.Messages.ResourceOffersMessage} received from Mesos.
     */
    public static class ResourceOffersMessageEnvelope extends SchedulerMessageEnvelope<ResourceOffersMessage>
    {
        public ResourceOffersMessageEnvelope(final UPID sender, final UPID recipient, final ResourceOffersMessage message)
        {
            super(sender, recipient, message);
        }
    }

    /**
     * {@link mesos.internal.Messages.FrameworkErrorMessage} received from Mesos.
     */
    public static class FrameworkErrorMessageEnvelope extends SchedulerMessageEnvelope<FrameworkErrorMessage>
    {
        public FrameworkErrorMessageEnvelope(final UPID sender, final UPID recipient, final FrameworkErrorMessage message)
        {
            super(sender, recipient, message);
        }
    }

    /**
     * {@link mesos.internal.Messages.ExecutorToFrameworkMessage} received from Mesos.
     */
    public static class ExecutorToFrameworkMessageEnvelope extends SchedulerMessageEnvelope<ExecutorToFrameworkMessage>
    {
        public ExecutorToFrameworkMessageEnvelope(final UPID sender, final UPID recipient, final ExecutorToFrameworkMessage message)
        {
            super(sender, recipient, message);
        }
    }

    /**
     * {@link mesos.internal.Messages.LostSlaveMessage} received from Mesos.
     */
    public static class LostSlaveMessageEnvelope extends SchedulerMessageEnvelope<LostSlaveMessage>
    {
        public LostSlaveMessageEnvelope(final UPID sender, final UPID recipient, final LostSlaveMessage message)
        {
            super(sender, recipient, message);
        }
    }

    /**
     * {@link mesos.internal.Messages.RescindResourceOfferMessage} received from Mesos.
     */
    public static class RescindResourceOfferMessageEnvelope extends SchedulerMessageEnvelope<RescindResourceOfferMessage>
    {
        public RescindResourceOfferMessageEnvelope(final UPID sender, final UPID recipient, final RescindResourceOfferMessage message)
        {
            super(sender, recipient, message);
        }
    }

    /**
     * {@link mesos.internal.Messages.StatusUpdateMessage} received from Mesos.
     */
    public static class StatusUpdateMessageEnvelope extends SchedulerMessageEnvelope<StatusUpdateMessage>
    {
        public StatusUpdateMessageEnvelope(final UPID sender, final UPID recipient, final StatusUpdateMessage message)
        {
            super(sender, recipient, message);
        }
    }
}
