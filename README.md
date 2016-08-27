flume-influxdb-reporter
=======================

Apache Flume plugin for collecting and reporting metrics to InfluxDB

Installation
------------

1. Clone this repository and `mvn clean package` to build the artifact.
2. Then, tar xzf `target/flume-influxdb-reporter-{version}.tar.gz` into `$FLUME_HOME/plugins.d` directory.
   https://flume.apache.org/FlumeUserGuide.html#installing-third-party-plugins

Configuration
-------------

| Property Name | Default | Description |
|---------------|---------|-------------|
| **type** | - | The component type name, has to be `net.thisptr.flume.reporter.influxdb.InfluxDBReporter` |
| **servers** | - | Comma-separated list `of hostname:port` of InfluxDB servers |
| **database** | - | The name of the database to store metrics. The database is automatically created if not exists. |
| interval | 30 | Time, in seconds, between consecutive reporting to InfluxDB server |
| user | root | The user name to use when connecting to InfluxDB server |
| password | root | The password to use when connecting to InfluxDB server |
| tags.*&lt;key&gt;* | - | Additional tags to set on each measurements. For example, to add a `hostname` tag, configuration will look like `tags.hostname = foo.example.com` |
| retention | - | The name of the RetentionPolicy to write metrics to. If not specified, `DEFAULT` policy is used. |

#### Example configuration

```sh
JAVA_OPTS="$JAVA_OPTS -Dflume.monitoring.type=net.thisptr.flume.reporter.influxdb.InfluxDBReporter \
	-Dflume.monitoring.servers=influxdb.example.com \
	-Dflume.monitoring.database=flume \
	-Dflume.monitoring.interval=10 \
	-Dflume.monitoring.tags.host=`hostname` \
	-Dflume.monitoring.tags.env=production \
	-Dflume.monitoring.tags.version=1.6.0"
```

TODO
----

 - upload the artifact to the maven central
