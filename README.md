flume-influxdb-reporter
=======================

| Property Name | Default | Description |
|---------------------------------------|
| **type** | - | The component type name, has to be `net.thisptr.flume.reporter.influxdb.InfluxDBReporter` |
| **servers** | - | Comma-separated list `of hostname:port` of InfluxDB servers |
| **database** | - | The name of the database to store metrics. The database is automatically created if not exists. |
| interval | 30 | Time, in seconds, between consecutive reporting to InfluxDB server |
| user | root | The user name to use when connecting to InfluxDB server |
| password | root | The password to use when connecting to InfluxDB server |
| tags.*&lt;key&gt;* | - | Additional tags to set on each measurements. For example, to add a `hostname` tag, configuration will look like `tags.hostname = foo.example.com` |
| retention | - | The name of the RetentionPolicy to write metrics to. If not specified, `DEFAULT` policy is used. |
