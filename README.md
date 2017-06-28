# About the Hadoop Metrics HTTP Sink Plugin #

**`hadoop-metrics-http-sink`** is an implementation of Hadoop Metrics2 plugin to push metrics to a HTTP end point (e.g. a distributed RESTful search engine).
The sink is capable of collecting metrics of Hadoop applications that support Hadoop Metrics2 (e.g. hbase, kafka, etc.).

# Build hadoop-metrics-http-sink.jar #
```
git clone https://dlanza@gitlab.cern.ch/db/hadoop-metrics-http-sink.git
cd hadoop-metrics-http-sink
gradle clean build
```

# Configuration #

Add or create at /etc/hadoop/conf/hadoop-metrics2.properties

```
*.sink.http.class=ch.cern.hadoop.metrics2.sink.http.HTTPMetricsSink

datanode.sink.http.collector=<full URL>
datanode.sink.http.auth = <false|true>
datanode.sink.http.auth.username = <username>
datanode.sink.http.auth.password = <password>
datanode.sink.http.extraAttributes=<list of keys separated by space>
datanode.sink.http.extraAttributes.<key1>=<value>
datanode.sink.http.extraAttributes.<key1>=<value>

# Examples below
namenode.sink.http.collector=http://es-cluster.cern.ch:9200/index-name/type/
namenode.sink.http.extraAttributes=cluster hostgroup env
namenode.sink.http.extraAttributes.cluster=hadoopqa
namenode.sink.http.extraAttributes.hostgroup=hadoop/datanode
namenode.sink.http.extraAttributes.env=prod

secondarynamenode.sink.http.collector=http://es-cluster.cern.ch:9200/index-name/type/
secondarynamenode.sink.http.auth = true
secondarynamenode.sink.http.auth.username = itdb
secondarynamenode.sink.http.auth.password = itdb_password
secondarynamenode.sink.http.extraAttributes=cluster
secondarynamenode.sink.http.extraAttributes.cluster=hadoopqa

resourcemanager.sink.http.collector=http://es-cluster.cern.ch:9200/index-name/type/
nodemanager.sink.http.collector=http://es-cluster.cern.ch:9200/index-name/type/
mrappmaster.sink.http.collector=http://es-cluster.cern.ch:9200/index-name/type/
maptask.sink.http.collector=http://es-cluster.cern.ch:9200/index-name/type/
reducetask.sink.http.collector=http://es-cluster.cern.ch:9200/index-name/type/
```


