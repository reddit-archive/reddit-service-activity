reddit-service-activity
=======================

A service for real-time counting of visitors.

There are currently two components of this service: the main service itself
which talks Thrift, and the gateway which speaks HTTP. Ideally, the gateway
portion would be subsumed by an API gateway down the road and stop needing to
exist here.
