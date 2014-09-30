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

import com.google.common.base.MoreObjects;
import com.google.protobuf.Message;

/**
 * Base class for all the Mesos specific messages that move across the event bus. They are all
 * shaped the same way (sender, receiver, protobuf message) so subclassing is used to have the
 * event bus dispatch to the correct receivers.
 */
public abstract class AbstractMessageEnvelope<T extends Message>
{
    private final UPID sender;
    private final UPID recipient;
    private final T message;

    protected AbstractMessageEnvelope(final UPID sender, final UPID recipient, final T message)
    {
        this.sender = checkNotNull(sender, "sender is null");
        this.recipient = checkNotNull(recipient, "recipient is null");
        this.message = checkNotNull(message, "message is null");
    }

    public UPID getSender()
    {
        return sender;
    }

    public UPID getRecipient()
    {
        return recipient;
    }

    public T getMessage()
    {
        return message;
    }

    @Override
    public String toString()
    {
        return MoreObjects.toStringHelper(this.getClass())
            .add("sender", sender)
            .add("recipient", recipient)
            .add("message", message)
            .toString();
    }
}
