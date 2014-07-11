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
import java.net.InetSocketAddress;
import java.net.ServerSocket;

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
}
