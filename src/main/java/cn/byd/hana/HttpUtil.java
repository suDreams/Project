package cn.byd.hana;

import com.google.gson.JsonObject;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

/**
 * Created by huang.hai7 on 2021/6/28.
 */
public class HttpUtil {

    private static final String DEFAULT_CHARSET = "UTF-8";
    private static final int CONNECTION_TIME_OUT = 60 * 1000;
    private static final String PROXY_URL = "xa-sg.byd.com";
    private static final int PROXY_PORT = 8080;


    public static String httpPostWithJSON(String url, JsonObject param) {
        CloseableHttpClient client = null;
        try {
            if (url == null || url.trim().length() == 0) {
                throw new Exception("URL is null");
            }

            HttpHost proxy = new HttpHost(PROXY_URL, PROXY_PORT);

            RequestConfig defaultRequestConfig = RequestConfig.custom()
                    .setConnectTimeout(CONNECTION_TIME_OUT).setSocketTimeout(CONNECTION_TIME_OUT)
                    .setProxy(proxy).build();
            client = HttpClients.custom().setDefaultRequestConfig(defaultRequestConfig).build();

            HttpPost httpPost = new HttpPost(url);
            if (param != null) {
                StringEntity entity = new StringEntity(param.toString(), DEFAULT_CHARSET);
                entity.setContentEncoding(DEFAULT_CHARSET);
                entity.setContentType("application/json");
                httpPost.setEntity(entity);
            }
            HttpResponse resp = client.execute(httpPost);
            if (resp.getStatusLine().getStatusCode() == 200) {
                return EntityUtils.toString(resp.getEntity(), DEFAULT_CHARSET);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            close(client);
        }
        return null;
    }

    /**
     * Close HTTP
     *
     * @param client
     */
    private static void close(CloseableHttpClient client) {
        if (client == null) {
            return;
        }
        try {
            client.close();
        } catch (Exception e) {
        }
    }
}
