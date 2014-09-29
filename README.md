# Jesos - A pure Java implementation of the Apache Mesos APIs.

* Requires Apache Mesos 0.19.0 or later. Does not work with any
  earlier version of Apache Mesos. The code is built with Apache Mesos
  0.20.1 but it will work with any older version of Apache Mesos, as
  long as it supports the HTTP protocol (which is the 0.19.0 release
  and later).

* Only works with Apache Zookeeper state management. Does not support
  local master (master must be `zk:<host1:port1>.../<path>`).

* Does not do the SASL dance, Apache Mesos authentication is not
  implemented

* Does not do Apache Zookeeper authentication (needs ripping out
  zkclient to do that). There is an experimental pull request to use
  Apache Curator, but that is just that: experimental.

## Status

* SchedulerDriver - code completed, tested with various schedulers
* ExecutorDriver - code completed, lightly tested
* State Management - code completed, tests for leveldb and ZooKeeper.

## TODO

* More tests. Spin up mesos from tests and do end-to-end testing.
* Get war stories running Marathon, Aurora, Singularity etc. on top of jesos.

## Usage

* Install using `maven clean install`
* Replace usage of `org.apache.mesos.MesosSchedulerDriver` with `com.groupon.mesos.JesosSchedulerDriver`
* Replace usage of `org.apache.mesos.MesosExecutorDriver` with `com.groupon.mesos.JesosExecutorDriver`
* Replace usage of `org.apache.mesos.state.LevelDBState` with `com.groupon.mesos.JLevelDBState`
* Replace usage of `org.apache.mesos.state.ZooKeeperState` with `com.groupon.mesos.JZookeeperState`
* Profit


----
Copyright (C) 2014, Groupon, Inc.
Licensed under the Apache Software License V2 (see the LICENSE file in this folder).
