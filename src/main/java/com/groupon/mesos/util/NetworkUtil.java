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

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.ServerSocket;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.List;

import com.google.common.collect.ImmutableList;

public final class NetworkUtil
{
    private NetworkUtil()
    {
        throw new AssertionError("do not instantiate");
    }

    /**
     * Find an unused port.
     */
    public static int findUnusedPort() throws IOException
    {
        int port;

        try (ServerSocket socket = new ServerSocket()) {
            socket.bind(new InetSocketAddress(0));
            port = socket.getLocalPort();
        }

        return port;
    }

    //
    // ========================================================================
    //
    // This code was taken from https://github.com/airlift/airlift/blob/master/node/src/main/java/io/airlift/node/NodeInfo.java
    //
    // ========================================================================
    //

    public static String findPublicIp()
        throws UnknownHostException
    {
        // Check if local host address is a good v4 address
        final InetAddress localAddress = InetAddress.getLocalHost();
        if (isV4Address(localAddress) && getGoodAddresses().contains(localAddress)) {
            return localAddress.getHostAddress();
        }

        // check all up network interfaces for a good v4 address
        for (final InetAddress address : getGoodAddresses()) {
            if (isV4Address(address)) {
                return address.getHostAddress();
            }
        }

        // just return the local host address
        // it is most likely that this is a disconnected developer machine
        return localAddress.getHostAddress();
    }

    private static List<InetAddress> getGoodAddresses()
    {
        final ImmutableList.Builder<InetAddress> list = ImmutableList.builder();
        for (final NetworkInterface networkInterface : getGoodNetworkInterfaces()) {
            for (final InetAddress address : Collections.list(networkInterface.getInetAddresses())) {
                if (isGoodAddress(address)) {
                    list.add(address);
                }
            }
        }
        return list.build();
    }

    private static List<NetworkInterface> getGoodNetworkInterfaces()
    {
        try {
            final ImmutableList.Builder<NetworkInterface> builder = ImmutableList.builder();
            for (final NetworkInterface networkInterface : Collections.list(NetworkInterface.getNetworkInterfaces())) {
                try {
                    if (!networkInterface.isLoopback() && networkInterface.isUp()) {
                        builder.add(networkInterface);
                    }
                }
                catch (final SocketException se) {
                    continue; // Ignore that network interface.
                }
            }
            return builder.build();
        }
        catch (final SocketException se) {
            // Return empty list.
            return ImmutableList.of();
        }
    }

    private static boolean isV4Address(final InetAddress address)
    {
        return address.getAddress().length == 4;
    }

    private static boolean isGoodAddress(final InetAddress address)
    {
        return !address.isAnyLocalAddress() &&
            !address.isLoopbackAddress() &&
            !address.isMulticastAddress();
    }
}
