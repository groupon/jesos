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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.UUID;

import com.google.protobuf.ByteString;

public final class UUIDUtil
{
    private UUIDUtil()
    {
        throw new AssertionError("do not instantiate");
    }

    public static ByteString uuidBytes(final UUID uuid)
    {
        final ByteBuffer longBuffer = ByteBuffer.allocate(16).order(ByteOrder.BIG_ENDIAN);
        longBuffer.mark();
        longBuffer.putLong(uuid.getMostSignificantBits());
        longBuffer.putLong(uuid.getLeastSignificantBits());
        longBuffer.reset();
        return ByteString.copyFrom(longBuffer);
    }

    public static UUID bytesUuid(final ByteString byteString)
    {
        final ByteBuffer uuidBuffer = byteString.asReadOnlyByteBuffer();
        // This always works (JLS 15.7.4)
        return new UUID(uuidBuffer.getLong(), uuidBuffer.getLong());
    }
}
