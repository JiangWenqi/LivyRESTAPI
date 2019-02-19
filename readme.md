# Livy REST API 封装（Java）

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





# 项目依赖

```xml
<dependencies>
    <!-- https://mvnrepository.com/artifact/org.apache.livy/livy-core -->
    <dependency>
        <groupId>org.apache.livy</groupId>
        <artifactId>livy-core_2.11</artifactId>
        <version>0.5.0-incubating</version>
    </dependency>
    <!-- https://mvnrepository.com/artifact/org.apache.livy/livy-rsc -->
    <dependency>
        <groupId>org.apache.livy</groupId>
        <artifactId>livy-rsc</artifactId>
        <version>0.5.0-incubating</version>
    </dependency>

    <!-- https://mvnrepository.com/artifact/org.apache.spark/spark-core -->
    <dependency>
        <groupId>org.apache.spark</groupId>
        <artifactId>spark-core_2.12</artifactId>
        <version>2.4.0</version>
    </dependency>

    <!-- https://mvnrepository.com/artifact/log4j/log4j -->
    <dependency>
        <groupId>log4j</groupId>
        <artifactId>log4j</artifactId>
        <version>1.2.17</version>
    </dependency>
    
    <dependency>
        <groupId>net.sf.json-lib</groupId>
        <artifactId>json-lib</artifactId>
        <version>2.4</version>
        <classifier>jdk15</classifier>
    </dependency>
    <dependency>
        <groupId>org.apache.httpcomponents</groupId>
        <artifactId>httpclient</artifactId>
        <version>4.5.5</version>
    </dependency>
    <!-- https://mvnrepository.com/artifact/org.apache.ibatis/ibatis-core -->
    <dependency>
        <groupId>org.apache.ibatis</groupId>
        <artifactId>ibatis-core</artifactId>
        <version>3.0</version>
    </dependency>
    <dependency>
        <groupId>org.junit.jupiter</groupId>
        <artifactId>junit-jupiter-api</artifactId>
        <version>5.3.1</version>
        <scope>test</scope>
    </dependency>
</dependencies>

```

---







# Spark Job封装

| Name           | Description                                    | Type            |
| -------------- | ---------------------------------------------- | --------------- |
| file           | File containing the application to execute     | path (required) |
| proxyUser      | User to impersonate when running the job       | string          |
| className      | Application Java/Spark main class              | string          |
| args           | Command line arguments for the application     | list of strings |
| jars           | jars to be used in this session                | list of strings |
| pyFiles        | Python files to be used in this session        | list of strings |
| files          | files to be used in this session               | list of strings |
| driverMemory   | Amount of memory to use for the driver process | string          |
| driverCores    | Number of cores to use for the driver process  | int             |
| executorMemory | Amount of memory to use per executor process   | string          |
| executorCores  | Number of cores to use for each executor       | int             |
| numExecutors   | Number of executors to launch for this session | int             |
| archives       | Archives to be used in this session            | List of string  |
| queue          | The name of the YARN queue to which submitted  | string          |
| name           | The name of this session                       | string          |
| conf           | Spark configuration properties                 | Map of key=val  |

这是原本是需要自己根据所需要的参数拼接 `json`，作为`request body`上传到Livy服务器，执行相应任务。

但是为了方便起见，我对该`request body`进行了封装。

```java
package space.jwqwy.livy.entiy;

import java.util.List;
import java.util.Map;

/**
 * Livy REST API 封装
 *
 * @author Vinci
 * Create: 2019/02/18 16:37
 * Description: Request Body Livy 批处理任务 属性封装
 */

public class SparkJob {

    /**
     * 必须有
     * 包含需要执行应用的文件，主要是jar包
     */
    private String file;
    /**
     * User to impersonate when running the job
     */
    private String proxyUser;
    /**
     * Application Java/Spark main class
     * 主类
     */
    private String className;

    /**
     * Command line arguments for the application
     * 参数
     */
    private List<String> args;

    /**
     * jars to be used in this session
     * 这个任务里面用到的其他 jar 包
     */
    private List<String> jars;

    /**
     * Python files to be used in this session
     */
    private List<String> pyFiles;
    /**
     * files to be used in this session
     */
    private List<String> files;

    /**
     * Amount of memory to use for the driver process
     */
    private String driverMemory;

    /**
     * Number of cores to use for the driver process
     */
    private int driverCores;

    /**
     * Amount of memory to use per executor process
     */
    private String executorMemory;
    /**
     * Number of cores to use for each executor
     */
    private int executorCores;
    /**
     * Number of executors to launch for this session
     */
    private int numExecutors;
    /**
     * Archives to be used in this session
     */
    private List<String> archives;
    /**
     * The name of the YARN queue to which submitted
     */
    private String queue;
    /**
     * The name of this session
     * 任务名称
     */
    private String name;
    /**
     * Spark configuration properties
     * spark 配置文件
     */
    private Map<String, Object> conf;

    public String getFile() {
        return file;
    }

    public void setFile(String file) {
        this.file = file;
    }

    public String getProxyUser() {
        return proxyUser;
    }

    public void setProxyUser(String proxyUser) {
        this.proxyUser = proxyUser;
    }

    public String getClassName() {
        return className;
    }

    public void setClassName(String className) {
        this.className = className;
    }

    public List<String> getArgs() {
        return args;
    }

    public void setArgs(List<String> args) {
        this.args = args;
    }

    public List<String> getJars() {
        return jars;
    }

    public void setJars(List<String> jars) {
        this.jars = jars;
    }

    public List<String> getPyFiles() {
        return pyFiles;
    }

    public void setPyFiles(List<String> pyFiles) {
        this.pyFiles = pyFiles;
    }

    public List<String> getFiles() {
        return files;
    }

    public void setFiles(List<String> files) {
        this.files = files;
    }

    public String getDriverMemory() {
        return driverMemory;
    }

    public void setDriverMemory(String driverMemory) {
        this.driverMemory = driverMemory;
    }

    public int getDriverCores() {
        return driverCores;
    }

    public void setDriverCores(int driverCores) {
        this.driverCores = driverCores;
    }

    public String getExecutorMemory() {
        return executorMemory;
    }

    public void setExecutorMemory(String executorMemory) {
        this.executorMemory = executorMemory;
    }

    public int getExecutorCores() {
        return executorCores;
    }

    public void setExecutorCores(int executorCores) {
        this.executorCores = executorCores;
    }

    public int getNumExecutors() {
        return numExecutors;
    }

    public void setNumExecutors(int numExecutors) {
        this.numExecutors = numExecutors;
    }

    public List<String> getArchives() {
        return archives;
    }

    public void setArchives(List<String> archives) {
        this.archives = archives;
    }

    public String getQueue() {
        return queue;
    }

    public void setQueue(String queue) {
        this.queue = queue;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Map<String, Object> getConf() {
        return conf;
    }

    public void setConf(Map<String, Object> conf) {
        this.conf = conf;
    }
}

```

---







# Http Utils

`HttpUtils`是比较这个项目最为核心的工具类，用来模拟发送：**POST,GET,DELETE** 等报文请求。

```java
package space.jwqwy.livy.util;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.util.Map;

/**
 * Livy REST API 封装
 *
 * @author Vinci
 * Create: 2019/02/19 15:35
 * Description: Http 报文
 */

public class HttpUtils {
    /**
     * HttpGET请求
     *
     * @param url     链接
     * @param headers 报文头
     * @return 结果
     */
    public static String getAccess(String url, Map<String, String> headers) {
        String result = null;
        CloseableHttpClient httpClient = HttpClients.createDefault();
        HttpGet httpGet = new HttpGet(url);
        if (headers != null && headers.size() > 0) {
            headers.forEach(httpGet::addHeader);
        }
        try {
            HttpResponse response = httpClient.execute(httpGet);
            HttpEntity entity = response.getEntity();
            result = EntityUtils.toString(entity);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return result;
    }

    /**
     * HttpDelete请求
     *
     * @param url     链接
     * @param headers 报文头
     * @return 结果
     */
    public static String deleteAccess(String url, Map<String, String> headers) {
        String result = null;
        CloseableHttpClient httpClient = HttpClients.createDefault();
        HttpDelete httpDelete = new HttpDelete(url);
        if (headers != null && headers.size() > 0) {
            headers.forEach(httpDelete::addHeader);
        }
        try {
            HttpResponse response = httpClient.execute(httpDelete);
            HttpEntity entity = response.getEntity();
            result = EntityUtils.toString(entity);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return result;
    }

    /**
     * HttpPost请求
     *
     * @param url     url
     * @param headers 请求报文头
     * @param data    数据
     * @return 结果
     */
    public static String postAccess(String url, Map<String, String> headers, String data) {
        String result = null;

        CloseableHttpClient httpClient = HttpClients.createDefault();

        HttpPost post = new HttpPost(url);

        if (headers != null && headers.size() > 0) {
            headers.forEach(post::addHeader);
        }

        try {
            StringEntity entity = new StringEntity(data);
            entity.setContentEncoding("UTF-8");
            entity.setContentType("application/json");
            post.setEntity(entity);

            HttpResponse response = httpClient.execute(post);
            HttpEntity resultEntity = response.getEntity();
            result = EntityUtils.toString(resultEntity);

            return result;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }
}
```

---





# Livy Service及实现类

## Livy Service

封装了一共有八个方法，具体作用看代码注释

```java
package space.jwqwy.livy.service;

import space.jwqwy.livy.entiy.SparkJob;
import space.jwqwy.livy.eum.SparkJobState;

import java.util.Map;

/**
 * Livy REST API 封装
 *
 * @author Vinci
 * Create: 2019/02/19 15:01
 * Description: 如何通过Livy的RESTful API接口向CDH集群提交作业
 */
public interface LivyService {

    /**
     * 运行一个 SparkJob 一直等到他运行完成之后才会有返回值
     *
     * @param job SparkJob
     * @return 任务是否正确运行结束
     */
    boolean runSparkJob(SparkJob job);

    /**
     * 后台启动一个 SparkJob
     *
     * @param job SparkJob
     * @return SparkJob 的 batch session ID
     */
    int runSparkJobBackground(SparkJob job);

    /**
     * 启动一个 session 运行 SparkJob, 不需要等待是否运行成功
     *
     * @param job SparkJob
     * @return SparkJob 的 batch session ID
     */
    int startSparkJob(SparkJob job);

    /**
     * 查询所有的 活跃的 Spark Job
     *
     * @return 所有活跃的 Spark Job = batch session
     */
    Map<String, Object> getActiveSparkJobs();

    /**
     * 查询具体的且活跃的 Spark Job 信息
     *
     * @param sparkJobID SparkJob 的 ID（batch session ID）
     * @return Spark Job 信息 ，具体的 batch session 信息
     */
    Map<String, Object> getSparkJobInfo(int sparkJobID);

    /**
     * 查询具体的且活跃的 Spark Job 状态
     *
     * @param sparkJobID SparkJob 的 ID（batch session ID）
     * @return Spark Job 状态 ，具体的 batch session 状态
     */
    SparkJobState getSparkJobState(int sparkJobID);

    /**
     * 查询具体的且活跃的 Spark Job 日志
     *
     * @param sparkJobID SparkJob 的 ID（batch session ID）
     * @return Spark Job 日志 ，具体的 batch session 日志
     */
    Map<String, Object> getSparkJoblog(int sparkJobID);

    /**
     * Kills the Batch job.
     *
     * @param sparkJobID SparkJob 的 ID（batch session ID）
     * @return msg
     * {
     * "msg": "deleted"
     * }
     */
    Map<String, Object> deleteSparkJob(int sparkJobID);

}

```

---



## Livy Service实现类

```java
package space.jwqwy.livy.service.impl;

import net.sf.json.JSONObject;
import net.sf.json.JsonConfig;
import net.sf.json.util.PropertyFilter;
import org.apache.log4j.Logger;
import space.jwqwy.livy.common.Constants;
import space.jwqwy.livy.entiy.SparkJob;
import space.jwqwy.livy.eum.SparkJobState;
import space.jwqwy.livy.service.LivyService;
import space.jwqwy.livy.util.HttpUtils;
import space.jwqwy.livy.util.PropertiesUtil;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Livy REST API 封装
 *
 * @author Vinci
 * Create: 2019/02/19 15:12
 * Description: Livy Service实现类
 */

public class LivyServiceImpl implements LivyService {

    private static Logger logger = Logger.getLogger(LivyServiceImpl.class);
    private static String LIVY_URL = "";

    public LivyServiceImpl() {
        try {
            Properties properties = PropertiesUtil.getProperties("properties/livy.properties");
            LIVY_URL = String.valueOf(properties.get("LIVY_URL"));
        } catch (IOException e) {
            logger.error("请检查配置文件，找不到 Livy URL");
            e.printStackTrace();
        }
    }

    @Override
    public boolean runSparkJob(SparkJob sparkJob) {
        int sparkJobID = startSparkJob(sparkJob);
        while (true) {
            SparkJobState sparkJobState = getSparkJobState(sparkJobID);
            switch (sparkJobState) {
                case SHUTTING_DOWN:
                    return false;
                case ERROR:
                    return false;
                case DEAD:
                    return false;
                case SUCCESS:
                    return true;
                default:
            }
            try {
                // 休眠3s
                Thread.sleep(3000);
            } catch (Exception ex) {
                logger.error(ex.getMessage());
            }
        }
    }

    @Override
    public int runSparkJobBackground(SparkJob sparkJob) {
        return startSparkJob(sparkJob);
    }

    @Override
    public int startSparkJob(SparkJob sparkJob) {
        int sparkJobID = -1;
        JSONObject batchSession = createBatch(parse2Json(sparkJob));
        String state = batchSession.getString(Constants.LIVY_SESSION_STATE);

        // 如果 session 状态为 不为 dead 或者 error ，则返回 session id
        SparkJobState sparkJobState = SparkJobState.fromDescription(state);
        if (sparkJobState != SparkJobState.DEAD && sparkJobState != SparkJobState.ERROR) {
            sparkJobID = (int) batchSession.get(Constants.LIVY_SESSION_ID);
        } else {
            logger.error("================ 创建Spark 任务失败=======================\n");
            logger.error("=====================失败原因:==========================\n" + batchSession.toString());
        }

        return sparkJobID;
    }

    @Override
    public Map<String, Object> getActiveSparkJobs() {
        return getBatchSessions();
    }

    @Override
    public Map<String, Object> getSparkJobInfo(int sparkJobID) {
        return getBatchSession(sparkJobID);
    }

    @Override
    public SparkJobState getSparkJobState(int sparkJobID) {
        return getBatchSessionState(sparkJobID);
    }

    @Override
    public Map<String, Object> getSparkJoblog(int sparkJobID) {
        return getBatchSessionLog(sparkJobID);
    }

    @Override
    public Map<String, Object> deleteSparkJob(int sparkJobID) {
        return deleteBatchSession(sparkJobID);
    }

    /**
     * 过滤器，把默认值的参数剔除掉
     *
     * @param job Livy任务
     * @return jobJson
     */
    private JSONObject parse2Json(SparkJob job) {
        // 过滤器，把默认值的参数剔除掉
        PropertyFilter filter = (source, name, value) -> {
            // 如果为数字则判断是否为0（默认值），如果为0，则为 true
            if (value instanceof Number && (int) value == 0) {
                return true;
            } else {
                return null == value;
            }
        };
        JsonConfig jsonConfig = new JsonConfig();
        jsonConfig.setJsonPropertyFilter(filter);
        return JSONObject.fromObject(job, jsonConfig);
    }

    /**
     * 创建一个 Batch Session 执行 SparkJob
     *
     * @param sparkJobJson sparkJob json 形式
     * @return 该 job 的 batch session 信息
     */
    private JSONObject createBatch(JSONObject sparkJobJson) {
        // 将 Map 转为字符串
        String sparkJobJsonStr = JSONObject.fromObject(sparkJobJson).toString();
        return createBatch(sparkJobJsonStr);
    }

    /**
     * 创建一个 Batch Session 执行 SparkJob
     *
     * @param sparkJobJsonStr sparkJob 字符串形式
     * @return 该 job 的 batch session 信息
     */
    private JSONObject createBatch(String sparkJobJsonStr) {
        JSONObject resultJson = null;
        Map<String, String> headers = new HashMap<>(4);
        headers.put("Accept", "application/json");
        headers.put("Content-Type", "application/json");
        headers.put("Accept-Charset", "utf-8");

        String result = HttpUtils.postAccess(LIVY_URL + "/batches", headers, sparkJobJsonStr);
        if (result != null) {
            resultJson = JSONObject.fromObject(result);
        } else {
            logger.error("\n==============Livy 提交批任务失败==================\n");
        }
        return resultJson;
    }

    private Map<String, Object> getBatchSessions() {
        JSONObject resultJson = null;
        String result = HttpUtils.getAccess(LIVY_URL + "/batches", null);
        if (result != null) {
            resultJson = JSONObject.fromObject(result);
        } else {
            logger.error("\n==============Livy 查询批任务失败==================\n");
        }
        return resultJson;
    }

    private Map<String, Object> getBatchSession(int batchID) {
        JSONObject resultJson = null;
        String result = HttpUtils.getAccess(LIVY_URL + "/batches/" + batchID, null);
        if (result != null) {
            resultJson = JSONObject.fromObject(result);
        } else {
            logger.error("\n==============Livy 查询具体任务失败，任务编号为：\n" + batchID + "\n");
        }
        return resultJson;
    }

    private SparkJobState getBatchSessionState(int batchID) {
        SparkJobState sparkJobState = null;
        String result = HttpUtils.getAccess(LIVY_URL + "/batches/" + batchID + "/state", null);
        if (result != null) {
            JSONObject resultJson = JSONObject.fromObject(result);
            String state = resultJson.getString("state");
            sparkJobState = SparkJobState.fromDescription(state);
        } else {
            logger.error("\n==============Livy 查询具体任务状态失败，任务编号为：\n" + batchID);
        }
        return sparkJobState;
    }

    private Map<String, Object> getBatchSessionLog(int batchID) {
        JSONObject resultJson = null;
        String result = HttpUtils.getAccess(LIVY_URL + "/batches/" + batchID + "/log", null);
        if (result != null) {
            resultJson = JSONObject.fromObject(result);
        } else {
            logger.error("\n==============Livy 查询具体任务日志失败，任务编号为：\n" + batchID + "\n");
        }
        return resultJson;
    }

    private Map<String, Object> deleteBatchSession(int batchID) {
        JSONObject resultJson = null;
        String result = HttpUtils.deleteAccess(LIVY_URL + "/batches/" + batchID, null);
        if (result != null) {
            resultJson = JSONObject.fromObject(result);
        } else {
            logger.error("\n==============Livy 删除具体任务失败，任务编号为：\n" + batchID + "\n");
        }
        return resultJson;
    }
}

```





---





# Livy Service 测试

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



## 第三部：执行任务，查询任务状态等操作

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