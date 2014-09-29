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

import static java.lang.String.format;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import java.io.Closeable;
import java.io.IOException;
import java.net.URL;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.Message;
import com.squareup.okhttp.Callback;
import com.squareup.okhttp.MediaType;
import com.squareup.okhttp.OkHttpClient;
import com.squareup.okhttp.Protocol;
import com.squareup.okhttp.Request;
import com.squareup.okhttp.RequestBody;
import com.squareup.okhttp.Response;

/**
 * Sends out messages to the mesos master and slaves. A message is sent as a HTTP post with a
 * protobuf body. The Mesos 0.19+ framework will acknowledge messages that have no User-Agent: libprocess
 * header set with 202 (ACCEPTED). The message must contain a custom (Libprocess-From) header as the
 * sender.
 */
public class HttpProtocolSender
    implements Callback, Closeable
{
    private static final Log LOG = Log.getLog(HttpProtocolSender.class);

    private static final MediaType PROTOBUF_MEDIATYPE = MediaType.parse("application/x-protobuf");

    private final AtomicBoolean closed = new AtomicBoolean(false);

    private final OkHttpClient client;
    private final String sender;
    private final Map<UUID, SettableFuture<Void>> inFlight = new ConcurrentHashMap<>(16, 0.75f, 2);

    public HttpProtocolSender(final UPID sender)
    {
        this.client = new OkHttpClient();
        client.setProtocols(ImmutableList.of(Protocol.HTTP_1_1));

        this.sender = sender.asString();
    }

    @Override
    public void close() throws IOException
    {
        if (!closed.getAndSet(true)) {
            // Drain all outstanding requests.
            while (!inFlight.isEmpty()) {
                try {
                    // Wait for all outstanding futures to complete
                    Futures.allAsList(inFlight.values()).get();
                }
                catch (final ExecutionException e) {
                    LOG.warn(e.getCause(), "While waiting for in flight requests to drain");
                }
                catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
    }

    public void sendHttpMessage(final UPID recipient, final Message message) throws IOException
    {
        if (closed.get()) {
            return;
        }

        checkNotNull(recipient, "recipient is null");
        checkNotNull(message, "message is null");
        checkArgument(recipient.getHost() != null, "%s is not a valid recipient for %s", recipient, message);
        checkArgument(recipient.getPort() > 0, "%s is not a valid recipient for %s", recipient, message);
        final String path = format("/%s/%s", recipient.getId(), message.getDescriptorForType().getFullName());
        final URL url = new URL("http", recipient.getHost(), recipient.getPort(), path);

        final UUID tag = UUID.randomUUID();
        inFlight.put(tag, SettableFuture.<Void>create());

        final RequestBody body = RequestBody.create(PROTOBUF_MEDIATYPE, message.toByteArray());
        final Request request = new Request.Builder()
            .header("Libprocess-From", sender)
            .url(url)
            .post(body)
            .tag(tag)
            .build();

        LOG.debug("Sending from %s to URL %s: %s", sender, url, message);
        client.newCall(request).enqueue(this);
    }

    @Override
    public void onFailure(final Request request, final IOException e)
    {
        final Object tag = request.tag();
        checkState(tag != null, "saw a request with null tag");

        final SettableFuture<Void> future = inFlight.remove(tag);
        checkState(future != null, "Saw tag %s but not in in flight map", tag);
        future.setException(e);

        LOG.warn("While running %s %s: %s", request.method(), request.urlString(), e.getMessage());
    }

    @Override
    public void onResponse(final Response response) throws IOException
    {
        final Object tag = response.request().tag();
        checkState(tag != null, "saw a request with null tag");

        final SettableFuture<Void> future = inFlight.remove(tag);
        checkState(future != null, "Saw tag %s but not in in flight map", tag);
        future.set(null);

        LOG.debug("Response %s %s: %d", response.request().method(), response.request().urlString(), response.code());
    }
}
