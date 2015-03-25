# Changelog

## Version 1.3

* 2015-03-24 - Add new Mesos 0.22.0 API call (acknowledgeStatusUpdate)

## Version 1.2

* 2015-03-16 - Add Extension registry to allow extension parsing for
               protobuf messages.

## Version 1.1

* 2014-12-01 - Upgrade Mesos dependencies to 0.21.0
* 2014-11-12 - Lock number of worker threads for the protocol receiver,
               otherwise, on a server with many cores, there will be hundreds
               of idle threads created.

## Version 1.0

* 2014-09-29 - First public release
