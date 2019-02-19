package space.jwqwy.livy.util;

/**
 * Livy REST API 封装
 *
 * @author Vinci
 * Create: 2019/02/19 15:41
 * Description: TODO
 */

import org.apache.ibatis.io.Resources;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
public final class PropertiesUtil {
    private PropertiesUtil() {
    }

    public static Properties getProperties(String path) throws IOException {
        Properties properties = new Properties();
        try {
            InputStream in = Resources.getResourceAsStream(path);
            properties.load(in);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return properties;
    }
}