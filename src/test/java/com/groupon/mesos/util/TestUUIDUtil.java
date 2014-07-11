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

import static com.groupon.mesos.util.UUIDUtil.bytesUuid;
import static com.groupon.mesos.util.UUIDUtil.uuidBytes;

import static org.junit.Assert.assertEquals;

import java.util.UUID;

import com.google.protobuf.ByteString;

import org.junit.Test;

public class TestUUIDUtil
{
    @Test
    public void testUuid()
    {
        final UUID uuid = UUID.randomUUID();
        final ByteString str = uuidBytes(uuid);
        final UUID newUuid = bytesUuid(str);
        assertEquals(uuid, newUuid);

        final ByteString newStr = uuidBytes(newUuid);
        assertEquals(str, newStr);
    }
}
