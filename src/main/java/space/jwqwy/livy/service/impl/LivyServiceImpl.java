package space.jwqwy.livy.service.impl;

import net.sf.json.JSONArray;
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
import java.util.*;

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

        Map<String, List<String>> oldLog = new HashMap<>(0);
        while (true) {

            Map<String, List<String>> nowLog = getBatchSessionLog(sparkJobID);

            Map<String, List<String>> newLog = getNewLog(oldLog, nowLog);

            printSessionLog("info", newLog);

            oldLog = nowLog;

            SparkJobState sparkJobState = getBatchSessionState(sparkJobID);
            switch (sparkJobState) {
                case SHUTTING_DOWN:
                    logger.error("\n================================job关闭==================================\n");
                    return false;
                case ERROR:
                    logger.error("\n================================job错误==================================\n");
                    return false;
                case DEAD:
                    logger.error("\n================================job死亡==================================\n");
                    return false;
                case SUCCESS:
                    logger.info("\n================================job执行成功==================================\n");
                    return true;
                default:
            }

            try {
                // 休眠3s，每隔3s 中执行一次查询
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
    public String getSparkJobLog(int sparkJobID) {
        StringBuilder logs = new StringBuilder();

        Map<String, List<String>> batchSessionLog = getBatchSessionLog(sparkJobID);
        if (batchSessionLog.size() > 0) {
            List<String> stdout = batchSessionLog.get("stdout");
            List<String> stderr = batchSessionLog.get("stderr");
            for (String log : stdout) {
                logs.append(log).append("\n");
            }
            for (String log : stderr) {
                logs.append(log).append("\n");
            }
        }

        return logs.toString();
    }

    @Override
    public Map<String, List<String>> getSparkJobNewLog(int sparkJobID, Map<String, List<String>> oldLog) {
        Map<String, List<String>> nowLog = getBatchSessionLog(sparkJobID);

        return getNewLog(oldLog, nowLog);
    }

    @Override
    public Map<String, Object> deleteSparkJob(int sparkJobID) {
        return deleteBatchSession(sparkJobID);
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

    /**
     * 解析 Session 日志
     *
     * @param batchID job id
     * @return 日志
     */
    private Map<String, List<String>> getBatchSessionLog(int batchID) {
        // Session 日志分两个部分，一个是：stdout, 一个是：stderr。且默认各只输出两百行日志
        Map<String, List<String>> logMap = new HashMap<>(402);
        // stdout 标准日志输出字符串链表
        List<String> stdoutLog = new ArrayList<>(200);
        // stdout 错误日志输出字符串链表
        List<String> stderrLog = new ArrayList<>(200);

        String result = HttpUtils.getAccess(LIVY_URL + "/batches/" + batchID + "/log", null);
        if (result != null) {
            JSONObject responseData = JSONObject.fromObject(result);
            // 解析出这202行日志（字符串数组）
            JSONArray logJsonArray = responseData.getJSONArray("log");
            // 有时候关键字 stdout 不会出现，则stdoutIndex=-1,一般情况下=0
            int stdoutIndex = logJsonArray.indexOf("stdout: ");
            int stderrIndex = logJsonArray.indexOf("\nstderr: ");
            Object[] logs = logJsonArray.toArray();

            for (int i = stdoutIndex + 1; i < stderrIndex; i++) {
                stdoutLog.add((String) logs[i]);
            }
            for (int i = stderrIndex + 1; i < logs.length; i++) {
                stderrLog.add((String) logs[i]);
            }
            logMap.put("stdout", stdoutLog);
            logMap.put("stderr", stderrLog);

        } else {
            logger.error("\n==============Livy 查询具体任务日志失败，任务编号为：\n" + batchID + "\n");
        }

        return logMap;
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

    /**
     * 每次请求 Batch Session 的 log，都是当前所有的 log，所以重复请求，会有重复的 log
     * 于是这个方法就是把重复 log 去掉，只显示新增加 log
     *
     * @param oldLog 上一次请求的 log
     * @param nowLog 这一次请求的 log
     * @return 新增加的 log
     */
    private Map<String, List<String>> getNewLog(Map<String, List<String>> oldLog, Map<String, List<String>> nowLog) {
        Map<String, List<String>> newLog = new HashMap<>(400);
        // stdout 标准日志输出字符串链表
        List<String> newStdoutLog = new ArrayList<>(200);
        // stdout 错误日志输出字符串链表
        List<String> newStderrLog = new ArrayList<>(200);

        if ((oldLog.size() == 0)) {
            return nowLog;
        }

        List<String> oldStdoutLog = oldLog.get("stdout");
        List<String> oldStderrLog = oldLog.get("stderr");

        if ((oldStdoutLog.size() == 0) && (oldStderrLog.size() == 0)) {
            return nowLog;
        }

        List<String> nowStdoutLog = nowLog.get("stdout");
        List<String> nowStderrLog = nowLog.get("stderr");

        getNewLog(newStdoutLog, oldStdoutLog, nowStdoutLog);
        newLog.put("stdout", newStdoutLog);
        getNewLog(newStderrLog, oldStderrLog, nowStderrLog);
        newLog.put("stderr", newStderrLog);
        return newLog;
    }

    /**
     * 去重拼接日志
     *
     * @param newStdLog 新的日志
     * @param oldStdLog 旧的日志
     * @param nowStdLog 现在的日志
     */
    private void getNewLog(List<String> newStdLog, List<String> oldStdLog, List<String> nowStdLog) {
        if (oldStdLog.size() > 0 && nowStdLog.size() > 0) {
            // 定位最后一行
            String oldStdLastLog = oldStdLog.get(oldStdLog.size() - 1);
            int newStdLogIndex = nowStdLog.lastIndexOf(oldStdLastLog);
            // 从最新行开始往后复制
            for (int i = newStdLogIndex + 1; i < nowStdLog.size(); i++) {
                newStdLog.add(nowStdLog.get(i));
            }
        }
    }

    /**
     * 打印 Session 日志
     *
     * @param logType 日志类型
     * @param logs    日志
     */
    private void printSessionLog(String logType, Map<String, List<String>> logs) {

        // 如果日志为空则不打印
        if ((logs.size() == 0)) {
            return;
        }
        List<String> stdoutLog = logs.get("stdout");
        List<String> stderrLog = logs.get("stderr");

        if ((stdoutLog.size() == 0) && (stderrLog.size() == 0)) {
            return;
        }

        StringBuilder stdout = new StringBuilder();

        for (String log : logs.get(Constants.LIVY_SESSION_LOG_STDOUT)) {
            stdout.append(log).append("\n");
        }
        StringBuilder stderr = new StringBuilder();
        for (String log : logs.get(Constants.LIVY_SESSION_LOG_STDERR)) {
            stderr.append(log).append("\n");
        }

        switch (logType) {
            case "info":
                logger.info("\nstdout:\n" + stdout + "\nstderr:\n" + stderr);
                break;
            case "error":
                logger.error("\nstdout:\n" + stdout + "\nstderr:\n" + stderr);
                break;
            case "debug":
                logger.debug("\nstdout:\n" + stdout + "\nstderr:\n" + stderr);
                break;
            default:
                logger.info("\nstdout:\n" + stdout + "\nstderr:\n" + stderr);
                break;
        }

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
}
