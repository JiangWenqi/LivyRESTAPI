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
