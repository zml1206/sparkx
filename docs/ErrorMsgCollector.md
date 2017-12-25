# ErrorMsgCollectorListener

### Usage

to enable message collector, firstly, your need add follow configuration to your maven project `pom.xml`

```xml
<dependency>
    <groupId>com.dxy.data</groupId>
    <artifactId>sparkx-monitor</artifactId>
    <version>1.0</version>
</dependency>
```

and then, you need add `spark.error.msg.collect.enable=true` and `spark.extraListeners=org.apache.spark.dxy.monitor.ErrorMsgCollectorListener` to your `spark-default.conf` or your app start script, and set subscript users.

here is an example:

```bash
${SPARK_HOME}/bin/spark-submit --master local[4] \
    --jars libs/sparkx-monitor-1.0.jar,libs/your-app.jar \
    --conf spark.error.msg.collect.enable=true \
    --conf spark.error.msg.collect.subusers=chenfu@dxy.cn \
    --conf spark.extraListeners=org.apache.spark.dxy.monitor.ErrorMsgCollectorListener \
    --class Test \
    libs/sparkx-examples-1.0.jar
```

| Property Name| Default | Meaning | 
| --- | --- | --- |
| spark.error.msg.collect.enable | false | enable spark job failed message collect.|
| spark.error.msg.collect.subusers| - | Comma-spearated list of email address to subscription error message.|
