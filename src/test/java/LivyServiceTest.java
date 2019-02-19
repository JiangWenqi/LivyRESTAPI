import org.junit.jupiter.api.Test;
import space.jwqwy.livy.entiy.SparkJob;
import space.jwqwy.livy.eum.SparkJobState;
import space.jwqwy.livy.service.LivyService;
import space.jwqwy.livy.service.impl.LivyServiceImpl;

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
    void test() {
        LivyService livyService = new LivyServiceImpl();

        SparkJob job = new SparkJob();

        job.setFile("hdfs://192.168.1.170:8020/testJars/spark-examples_2.11-2.3.0.cloudera4.jar");
        job.setClassName("org.apache.spark.examples.SparkPi");
        job.setName("SparkPi");
        job.setExecutorCores(3);

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
    }

}