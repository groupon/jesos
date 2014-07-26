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
/*
 * Licensed under the7 Apache License, Version 2.0 (the "License");
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

import static com.google.common.base.Preconditions.checkState;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.undertow.Undertow;
import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.server.handlers.BlockingHandler;
import io.undertow.server.handlers.CanonicalPathHandler;
import io.undertow.server.handlers.GracefulShutdownHandler;
import io.undertow.server.handlers.PathHandler;
import mesos.internal.Messages;

/**
 * Receives messages from the Mesos cluster. A message is a Protobuf formatted byte stream sent over http. Every
 * message is unconditionally acknowledged with 202. The user agent contains the pid (id, host and port) that identifies
 * the sender.
 */
public class HttpProtocolReceiver
    implements HttpHandler, Closeable
{
    private static final Log LOG = Log.getLog(HttpProtocolReceiver.class);

    private final Undertow httpServer;
    private final GracefulShutdownHandler shutdownHandler;

    private final ManagedEventBus eventBus;
    private final UPID localAddress;

    private final Class<?> messageBaseClass;

    private final Set<String> typesSeen = Sets.newConcurrentHashSet();
    private final ConcurrentMap<String, Method> parseMethodMap = Maps.newConcurrentMap();
    private final ConcurrentMap<String, Constructor<?>> constructorMap = Maps.newConcurrentMap();

    public HttpProtocolReceiver(final UPID localAddress,
                                final Class<?> messageBaseClass,
                                final ManagedEventBus eventBus)
    {
        this.localAddress = localAddress;
        this.messageBaseClass = messageBaseClass;
        this.eventBus = eventBus;

        final PathHandler pathHandler = new PathHandler();
        pathHandler.addPrefixPath(localAddress.getId(), new CanonicalPathHandler(new BlockingHandler(this)));

        this.shutdownHandler = new GracefulShutdownHandler(pathHandler);

        this.httpServer = Undertow.builder()
            .addHttpListener(localAddress.getPort(), localAddress.getHost())
            .setHandler(shutdownHandler)
            .build();
    }

    @Override
    public void close()
        throws IOException
    {
        shutdownHandler.shutdown();
        try {
            shutdownHandler.awaitShutdown();
        }
        catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        httpServer.stop();
    }

    public void start()
    {
        httpServer.start();
    }

    @Override
    public void handleRequest(final HttpServerExchange exchange) throws Exception
    {
        final UPID sender;
        final String libprocessFrom = exchange.getRequestHeaders().getFirst("Libprocess-From");

        if (libprocessFrom != null) {
            sender = UPID.create(libprocessFrom);
        }
        else {
            final String userAgent = exchange.getRequestHeaders().getFirst("User-Agent");

            checkState(userAgent != null && userAgent.startsWith("libprocess/"), "No User-Agent or Libprocess-From header found! Not a valid message!");
            sender = UPID.create(userAgent.substring(11));
        }

        final int dotIndex = exchange.getRelativePath().lastIndexOf('.');
        final String name = exchange.getRelativePath().substring(dotIndex + 1);
        exchange.setResponseCode(202);

        // This is where it gets ugly. Protobuf and dynamic messages don't really mesh.
        final Method parseFromMethod;
        final Constructor<?> envelopeConstructor;

        if (typesSeen.contains(name)) {
            if (!parseMethodMap.containsKey(name)) {
                LOG.warn("Unparseable message type %s", name);
                return;
            }
            else {
                parseFromMethod = parseMethodMap.get(name);
                envelopeConstructor = constructorMap.get(name);
            }
        }
        else {
            try {
                final Class<?> clazz = Class.forName(Messages.class.getName() + "$" + name);
                parseFromMethod = clazz.getMethod("parseFrom", InputStream.class);
                // This implies that for all messages delivered, an Envelope class exists, which is an inner
                // class of the messageBaseClass and ends with 'Envelope'.
                final Class<?> envelopeClazz = Class.forName(messageBaseClass.getName() + "$" + name + "Envelope");
                envelopeConstructor = envelopeClazz.getConstructor(UPID.class, UPID.class, clazz);

                parseMethodMap.put(name, parseFromMethod);
                constructorMap.put(name, envelopeConstructor);
            }
            catch (ReflectiveOperationException | SecurityException e) {
                LOG.warn(e, "While constructing objects for message type %s", name);
                return;
            }
            finally {
                typesSeen.add(name);
            }
        }

        try {
            final Object o = parseFromMethod.invoke(null, exchange.getInputStream());
            // Local delivery of the message.
            eventBus.post(envelopeConstructor.newInstance(sender, localAddress, o));
            LOG.debug("Received from %s: %s", sender.asString(), o);
        }
        catch (final ReflectiveOperationException e) {
            LOG.warn(e, "Can not decode message type %s", name);
        }
    }
}
