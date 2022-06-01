package com.mc.utils;

import com.google.common.collect.Maps;
import org.apache.flink.shaded.jackson2.org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.FileInputStream;
import java.util.Iterator;
import java.util.Map;

/**
 * @author macheng
 * @date 2022/3/7 18:28
 */
public class YmlUtil {
    private volatile static Map<String, Object> ymlMap = null;

    public synchronized static void init(String config)  {
        if (ymlMap!=null){
            return;
        }
        ymlMap = Maps.newConcurrentMap();
        getApplicationYml(config);
    }

    /**
     *
     */
    public YmlUtil() {
        super();
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> getApplicationYml(String config) {
        try {
            Yaml yaml = new Yaml();
//            InputStream resourceAsStream = YmlUtil.class.getResourceAsStream(config);
//            if (resourceAsStream != null) {
//                Map<String, Object> map = yaml.loadAs(resourceAsStream, Map.class);
//                switchToMap(null, map);
//            }
            File file = new File("/root/application.yml");
            FileInputStream fileInputStream = new FileInputStream(file);
            if (fileInputStream != null) {
                Map<String, Object> map = yaml.loadAs(fileInputStream, Map.class);
                switchToMap(null, map);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return ymlMap;
    }

    @SuppressWarnings("unchecked")
    private static void switchToMap(String myKey, Map<String, Object> map) {
        Iterator<String> it = map.keySet().iterator();
        myKey = myKey == null ? "" : myKey;
        String tmpkey = myKey;
        while (it.hasNext()) {
            String key = it.next();
            myKey = tmpkey + key;
            Object value = map.get(key);
            if (value instanceof Map) {
                switchToMap(myKey.concat("."), (Map<String, Object>) value);
            } else {
                if (null != value) {
                    ymlMap.put(myKey, value);
                }
//                System.out.println(myKey+"->"+map.get(key));
            }
        }
    }

    @SuppressWarnings("unchecked")
    public static <T> T get(String key) {
        if (ymlMap==null){
            throw new RuntimeException("YML CONFIG SHOULD BE INIT FIRST");
        }
        return (T) ymlMap.get(key);
    }

    public static String getStr(String key) {
        if (ymlMap==null){
            throw new RuntimeException("YML CONFIG SHOULD BE INIT FIRST");
        }
        return String.valueOf(ymlMap.get(key));
    }

}
