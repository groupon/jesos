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

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;

import org.junit.Test;

import com.google.protobuf.ExtensionRegistry;
import com.groupon.jesos.TestProtos;
import com.groupon.jesos.TestProtos.Base;
import com.groupon.jesos.TestProtos.ExtBase;

public class TestProtobufRegistry
{
    @Test
    public void testRoundtrip() throws Exception {
        ExtensionRegistry e = ProtobufRegistry.INSTANCE.mutableExtensionRegistry();

        TestProtos.registerAllExtensions(e);

        ExtBase extBase =
                        ExtBase.newBuilder()
                        .setA("172.42.1.2")
                        .setB("br0")
                        .setD(24)
                        .setC("172.42.1.1")
                        .setE("00:11:22:33:44:55")
                        .build();

        Base base = Base.newBuilder()
                        .setExtension(ExtBase.type, extBase)
                        .setType(Base.Type.EXT_BASE)
                        .build();

        byte [] bytes = base.toByteArray();

        Base generated = Base.parseFrom(new ByteArrayInputStream(bytes), e);

        ExtBase extGenerated = generated.getExtension(ExtBase.type);

        assertEquals(extBase.getA(), extGenerated.getA());
        assertEquals(extBase.getB(), extGenerated.getB());
        assertEquals(extBase.getC(), extGenerated.getC());
        assertEquals(extBase.getD(), extGenerated.getD());
        assertEquals(extBase.getE(), extGenerated.getE());
    }
}
