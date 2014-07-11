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

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;

import javax.annotation.Nonnull;

import com.google.common.base.CharMatcher;
import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.net.HostAndPort;

/**
 * Represents a mesos internal process address. Consists of an id, the host and a port.
 */
public final class UPID
{
    private static final Log LOG = Log.getLog(UPID.class);

    private final String id;
    private final HostAndPort hostAndPort;
    private final Integer ip;

    private final String pidAsString;

    public static UPID create(final String master)
    {
        checkNotNull(master, "master is null");
        final List<String> parts = ImmutableList.copyOf(Splitter.on(CharMatcher.anyOf(":@")).limit(3).split(master));
        checkState(parts.size() == 3, "%s is not a valid master definition", master);

        Integer ip = null;
        try {
            ip = resolveIp(parts.get(1));
        }
        catch (final IOException e) {
            LOG.warn("Could not resolve %s: %s", parts.get(1), e.getMessage());
        }

        return new UPID(parts.get(0), HostAndPort.fromParts(parts.get(1), Integer.parseInt(parts.get(2))), ip);
    }

    public static UPID fromParts(final String id, final HostAndPort hostAndPort)
        throws IOException
    {
        checkNotNull(id, "id is null");
        checkNotNull(hostAndPort, "hostAndPort is null");

        return new UPID(id,
                        hostAndPort,
                        resolveIp(hostAndPort.getHostText()));
    }

    private static Integer resolveIp(final String hostname)
        throws IOException
    {
        final InetAddress addr = InetAddress.getByName(hostname);
        // Gah. Mesos can only deal with IPv4.

        if (addr == null) {
            return null;
        }

        if (addr.getAddress().length != 4) {
            LOG.warn("Received non-IPv4 address (%s), see https://issues.apache.org/jira/browse/MESOS-1562", hostname);
            return null;
        }

        final ByteBuffer b = ByteBuffer.wrap(addr.getAddress()).order(ByteOrder.BIG_ENDIAN);
        return b.getInt();
    }

    private UPID(final String id, final HostAndPort hostAndPort, final Integer ip)
    {
        this.id = id;
        this.hostAndPort = hostAndPort;
        this.ip = ip;

        this.pidAsString = format("%s@%s:%d", id, hostAndPort.getHostText(), hostAndPort.getPort());
    }

    public String getId()
    {
        return id;
    }

    public String getHost()
    {
        return hostAndPort.getHostText();
    }

    public int getPort()
    {
        return hostAndPort.getPort();
    }

    public HostAndPort getHostAndPort()
    {
        return hostAndPort;
    }

    public Integer getIp()
    {
        return ip;
    }

    public String asString()
    {
        return pidAsString;
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(id, hostAndPort, ip);
    }

    @Override
    public boolean equals(final Object other)
    {
        if (other == this) {
            return true;
        }
        if (other == null || other.getClass() != this.getClass()) {
            return false;
        }
        final UPID that = (UPID) other;

        return Objects.equal(this.id, that.id)
            && Objects.equal(this.hostAndPort, that.hostAndPort)
            && Objects.equal(this.ip, that.ip);
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this.getClass())
            .add("id", id)
            .add("hostAndPort", hostAndPort)
            .add("ip", ip)
            .toString();
    }

    public static Function<String, UPID> getCreateFunction()
    {
        return new Function<String, UPID>() {
            @Override
            public UPID apply(@Nonnull final String value)
            {
                try {
                    return UPID.create(value);
                }
                catch (final Throwable t) {
                    throw Throwables.propagate(t);
                }
            }
        };
    }
}
