package simpledb;

import java.io.*;
import java.util.Properties;

public class Config {

    private static Properties p = new Properties();

    static {
        InputStream inputStream = Config.class.getClassLoader().getResourceAsStream("config.properties");
        Reader reader = new InputStreamReader(inputStream);

        Properties p = new Properties();

        try {
            p.load(reader);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public static void main(String[] args) throws IOException {
        System.out.println(p.getProperty("debugTransaction"));
        System.out.println(p.getProperty("debugPageWrite"));
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


