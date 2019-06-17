package simpledb;

import java.io.*;
import java.util.Properties;

public class Config {

    private static Properties p = new Properties();

    static {
        try {
            InputStream inputStream = Config.class.getClassLoader().getResourceAsStream("config.properties");
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));

            p = new Properties();
            p.load(bufferedReader);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public static void main(String[] args) throws IOException {
        System.out.println(debugTransaction());
        System.out.println(debugPageWrite());
        System.out.println(debugPageRead());
    }

    public static boolean getBoolProperty(String key) {
        return Boolean.parseBoolean(p.getProperty(key));
    }

    public static boolean debugTransaction() {
        return getBoolProperty("debugTransaction");
    }

    public static boolean debugPageWrite() {
        return getBoolProperty("debugPageWrite");
    }

    public static boolean debugPageRead() {
        return false;
    }
}


