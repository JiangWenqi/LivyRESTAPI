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
