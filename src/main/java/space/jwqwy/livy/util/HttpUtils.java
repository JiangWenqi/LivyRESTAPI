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
