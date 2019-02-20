import org.junit.jupiter.api.Test;
import space.jwqwy.livy.entiy.SparkJob;
import space.jwqwy.livy.eum.SparkJobState;
import space.jwqwy.livy.service.LivyService;
import space.jwqwy.livy.service.impl.LivyServiceImpl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Livy REST API 封装
 *
 * @author Vinci
 * Create: 2019/02/19 15:47
 * Description: Livy Service的测试类
 */

class LivyServiceTest {

    @Test
    void submitJob() {

        LivyService livyService = new LivyServiceImpl();

        SparkJob job = new SparkJob();

        // job.setFile("hdfs://192.168.1.170:8020/jar/spark-examples_2.11-2.3.0.cloudera4.jar");
        // job.setClassName("org.apache.spark.examples.SparkPi");
        // job.setName("SparkPi");

        job.setName("FP_Growth");
        job.setFile("hdfs://192.168.1.170:8020/taskJars/FP_Growth_Analysis-23.017-SNAPSHOT.jar");
        // job.setFile("hdfs://192.168.1.170:8020/taskJars/sparkAnalysis-1.0-SNAPSHOT.jar");
        job.setClassName("com.webstudio.sparkAnalaysis.FP_Growth");
        job.setExecutorCores(3);

        List<String> jars = new ArrayList<>(1);
        jars.add("hdfs://192.168.1.170:8020/lib/hive-hbase-handler-1.1.0-cdh5.14.2.jar");
        job.setJars(jars);

        int sparkJobID = livyService.startSparkJob(job);

        if (sparkJobID > 0) {
            System.out.println("\n创建任务，任务ID为：\n" + sparkJobID);

            Map<String, Object> activeSparkJobs = livyService.getActiveSparkJobs();
            System.out.println("\n查询当前所有任务：\n" + activeSparkJobs.toString());

            SparkJobState state = livyService.getSparkJobState(sparkJobID);
            System.out.println("\n查询任务ID为" + sparkJobID + "的任务状态:\n" + state);

            Map<String, Object> info = livyService.getSparkJobInfo(sparkJobID);
            System.out.println("\n查询任务ID为" + sparkJobID + "的任务详情:\n" + info.toString());

            String log = livyService.getSparkJobLog(sparkJobID);
            System.out.println("\n查询任务ID为" + sparkJobID + "的任务日志:\n" + log);

            // Map<String, Object> del = livyService.deleteSparkJob(sparkJobID);
            // System.out.println("删除任务ID为" + sparkJobID + "\n" + del.toString());
        }
        // 执行任务，一直到任务结束
        // System.out.println(runSparkJob(job));
    }

    /**
     * Livy查询 Spark 任务失败停用
     */
    @Test
    void failJobTest() {
        LivyService livyService = new LivyServiceImpl();
        SparkJob job = new SparkJob();
        job.setExecutorCores(3);
        List<String> jars = new ArrayList<>(1);
        jars.add("hdfs://192.168.1.170:8020/lib/hive-hbase-handler-1.1.0-cdh5.14.2.jar");
        job.setJars(jars);
        job.setName("FP_Growth");
        job.setFile("hdfs://192.168.1.170:8020/taskJars/sparkAnalysis-1.0-SNAPSHOT.jar");
        job.setClassName("com.webstudio.sparkAnalaysis.FP_Growth");

        int sparkJobID = livyService.runSparkJobBackground(job);

        while (true) {
            try {
                // 休眠3s
                Thread.sleep(4000);
            } catch (Exception ex) {
                ex.getMessage();
            }

            SparkJobState sparkJobState = livyService.getSparkJobState(sparkJobID);
            String log = livyService.getSparkJobLog(sparkJobID);

            System.out.println(log);

            switch (sparkJobState) {
                case SHUTTING_DOWN:
                    livyService.deleteSparkJob(sparkJobID);
                    return;
                case ERROR:
                    livyService.deleteSparkJob(sparkJobID);
                    return;
                case DEAD:
                    livyService.deleteSparkJob(sparkJobID);
                    return;
                case SUCCESS:
                    return;
                default:
            }

        }
    }

    @Test
    void sparkAnalysisTest() {
        LivyService livyService = new LivyServiceImpl();
        SparkJob job = new SparkJob();
        job.setName("FP_Growth");
        job.setFile("hdfs://192.168.1.170:8020/testJars/SparkAnalysis-23.018-SNAPSHOT.jar");
        job.setClassName("com.webstudio.sparkAnalaysis.FP_Growth");
        job.setExecutorCores(3);

        List<String> jars = new ArrayList<>(1);
        jars.add("hdfs://192.168.1.170:8020/lib/hive-hbase-handler-1.1.0-cdh5.14.2.jar");
        job.setJars(jars);

        livyService.runSparkJob(job);
    }

}
