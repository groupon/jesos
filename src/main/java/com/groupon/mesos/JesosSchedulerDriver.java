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
package com.groupon.mesos;

import java.io.Closeable;
import java.io.IOException;

import com.groupon.mesos.scheduler.InternalSchedulerDriver;

import org.apache.mesos.Protos.Credential;
import org.apache.mesos.Protos.FrameworkInfo;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;

public class JesosSchedulerDriver
    extends InternalSchedulerDriver
    implements SchedulerDriver, Closeable
{
    /**
     * Creates a new driver for the specified scheduler. The master
     * must be specified as
     *
     *     zk://host1:port1,host2:port2,.../path
     *     zk://username:password@host1:port1,host2:port2,.../path
     *
     * The driver will attempt to "failover" if the specified
     * FrameworkInfo includes a valid FrameworkID.
     */
    public JesosSchedulerDriver(final Scheduler scheduler,
                                final FrameworkInfo frameworkInfo,
                                final String master) throws IOException
    {
        super(scheduler, frameworkInfo, master, true, null);
    }

    public JesosSchedulerDriver(final Scheduler scheduler,
                                final FrameworkInfo frameworkInfo,
                                final String master,
                                final Credential credential)
                    throws IOException
    {
        super(scheduler, frameworkInfo, master, true, credential);
    }

    public JesosSchedulerDriver(final Scheduler scheduler,
                                final FrameworkInfo frameworkInfo,
                                final String master,
                                boolean implicitAcknowledges) throws IOException
    {
        super(scheduler, frameworkInfo, master, implicitAcknowledges, null);
    }

    public JesosSchedulerDriver(final Scheduler scheduler,
                                final FrameworkInfo frameworkInfo,
                                final String master,
                                boolean implicitAcknowledges,
                                final Credential credential)
                    throws IOException
    {
        super(scheduler, frameworkInfo, master, implicitAcknowledges, credential);
    }
}
