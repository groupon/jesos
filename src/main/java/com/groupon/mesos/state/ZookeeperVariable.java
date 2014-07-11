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
package com.groupon.mesos.state;

import com.google.protobuf.ByteString;

import org.apache.mesos.state.Variable;

import mesos.internal.state.State.Entry;

public class ZookeeperVariable
    extends JVariable
{
    private final Integer zookeeperVersion;

    ZookeeperVariable(final String name, final byte[] value)
    {
        this(name, value, null);
    }

    public ZookeeperVariable(final String name, final byte[] value, final Integer zookeeperVersion)
    {
        super(name, value);
        this.zookeeperVersion = zookeeperVersion;
    }

    ZookeeperVariable(final Entry entry, final Integer zookeeperVersion)
    {
        super(entry);
        this.zookeeperVersion = zookeeperVersion;
    }

    Integer getZookeeperVersion()
    {
        return zookeeperVersion;
    }

    byte[] asBytes()
    {
        return getEntry().toByteArray();
    }

    @Override
    public Variable mutate(final byte[] value)
    {
        return new ZookeeperVariable(Entry.newBuilder()
            .setName(getEntry().getName())
            .setValue(ByteString.copyFrom(value))
            .setUuid(getEntry().getUuid())
            .build(), zookeeperVersion);
    }
}
