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

import com.google.common.eventbus.SubscriberExceptionContext;
import com.google.common.eventbus.SubscriberExceptionHandler;

/**
 * Simple exception handler that, unlike the default handler, does not swallow
 * the exception causing the error.
 */
public class EventBusExceptionHandler implements SubscriberExceptionHandler
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
