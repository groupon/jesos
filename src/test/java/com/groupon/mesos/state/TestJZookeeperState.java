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

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.groupon.mesos.testutil.EmbeddedZookeeper;

import org.apache.mesos.state.State;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

public class TestJZookeeperState
    extends AbstractTestState
{
    private static EmbeddedZookeeper ZOOKEEPER;

    private JZookeeperState state;

    @BeforeClass
    public static void setUpZookeeper()
        throws Exception
    {
        ZOOKEEPER = new EmbeddedZookeeper();
        ZOOKEEPER.start();
    }

    @Before
    public void setUp()
        throws IOException
    {
        state = new JZookeeperState(ZOOKEEPER.getConnectString(), 30, TimeUnit.SECONDS, "test-" + UUID.randomUUID().toString());
    }

    @After
    public void tearDown()
        throws Exception
    {
        state.close();
    }

    @AfterClass
    public static void tearDownZookeeper()
    {
        ZOOKEEPER.close();
        ZOOKEEPER.cleanup();
    }

    @Override
    protected State getState()
    {
        return state;
    }
}
