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
package com.groupon.mesos.zookeeper;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.util.concurrent.SettableFuture;

import org.apache.mesos.Protos.MasterInfo;

/**
 * Sent to the message bus when a client wants to register for detection notification.
 */
public class DetectMessage
{
    private final SettableFuture<MasterInfo> future;
    private final MasterInfo previous;

    DetectMessage(final SettableFuture<MasterInfo> future, final MasterInfo previous)
    {
        this.future = checkNotNull(future, "future is null");
        this.previous = previous;
    }

    public SettableFuture<MasterInfo> getFuture()
    {
        return future;
    }

    public MasterInfo getPrevious()
    {
        return previous;
    }
}
