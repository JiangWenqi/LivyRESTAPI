# Livy REST API 封装（Java）

![](https://img.shields.io/cocoapods/l/Alamofire.svg?style=flat)

参考文章如下：

https://blog.csdn.net/camel84/article/details/81990383

https://cloud.tencent.com/developer/article/1078857



# 前言

> Livy is an open source REST interface for interacting with [Apache Spark](http://spark.apache.org/) from anywhere. It supports executing snippets of code or programs in a Spark context that runs locally or in [Apache Hadoop YARN](http://hadoop.apache.org/docs/current/hadoop-yarn/hadoop-yarn-site/YARN.html).
>
> - Interactive Scala, Python and R shells
> - Batch submissions in Scala, Java, Python
> - Multiple users can share the same server (impersonation support)
> - Can be used for submitting jobs from anywhere with REST
> - Does not require any code change to your programs

以上是`Livy`的官方介绍，具体使用请参照这篇[文章](https://blog.csdn.net/camel84/article/details/81990383)。

大体思路是用 `Java` 模拟发送请求报文给 `Livy`，

---





# Get Started

## 第一步：上传 jar 包

上传测试所用的`jar`包到`hdfs`。

```bash
export HADOOP_USER_NAME=hdfs
${HADOOP_HOME}/bin/hdfs dfs -mkdir /testJars
${HADOOP_HOME}/bin/hdfs dfs -put /opt/cloudera/parcels/SPARK2-2.3.0.cloudera4-1.cdh5.13.3.p0.611179/lib/spark2/examples/jars/spark-examples_2.11-2.3.0.cloudera4.jar /testJars/
```

## 第二步：创建 Spark Job

```java
SparkJob job = new SparkJob();

job.setFile("hdfs://192.168.1.170:8020/testJars/spark-examples_2.11-2.3.0.cloudera4.jar");
job.setClassName("org.apache.spark.examples.SparkPi");
job.setName("SparkPi");
job.setExecutorCores(3);
```



## 第三步：执行任务，查询任务状态等操作

```java
int sparkJobID = livyService.startSparkJob(job);

if (sparkJobID > 0) {
    System.out.println("\n创建任务，任务ID为：\n" + sparkJobID);

    Map<String, Object> activeSparkJobs = livyService.getActiveSparkJobs();
    System.out.println("\n查询当前所有任务：\n" + activeSparkJobs.toString());

    Map<String, Object> info = livyService.getSparkJobInfo(sparkJobID);
    System.out.println("\n查询任务ID为" + sparkJobID + "的任务详情:\n" + info.toString());

    SparkJobState state = livyService.getSparkJobState(sparkJobID);
    System.out.println("\n查询任务ID为" + sparkJobID + "的任务状态:\n" + state);

    Map<String, Object> log = livyService.getSparkJoblog(sparkJobID);
    System.out.println("\n查询任务ID为" + sparkJobID + "的任务日志:\n" + log.toString());

    // Map<String, Object> del = livyService.deleteSparkJob(sparkJobID);
    // System.out.println("删除任务ID为" + sparkJobID + "\n" + del.toString());
}
// 执行任务，一直到任务结束
// System.out.println(runSparkJob(job));
```