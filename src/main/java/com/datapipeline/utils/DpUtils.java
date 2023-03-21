package com.datapipeline.utils;

import static com.datapipeline.utils.ObjectConvert.getObjectMapper;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.commons.lang3.SystemUtils;
import org.apache.commons.lang3.time.DateFormatUtils;

public class DpUtils {

  public static void main(String[] args) {
    System.out.println(".......");
  }

  public static void test() {
    Date dNow = new Date(); // 当前时间
    Calendar calendar = Calendar.getInstance(); // 得到日历
    calendar.setTime(dNow); // 把当前时间赋给日历
    calendar.add(Calendar.MONTH, -3); // 设置为前3月
    Date dBefore = calendar.getTime(); // 得到前3月的时间
    System.out.println("dBefore: " + dBefore);
    System.out.println("前三个月的时间戳为：" + dBefore.getTime());
    long randomTime = ThreadLocalRandom.current().nextLong(dBefore.getTime(), new Date().getTime());
    System.out.println(DateFormatUtils.format(new Date(randomTime), "yyyy-MM-dd HH:mm:ss"));
    //    System.out.println("当前时间减去前三个月的时间差");
    //    System.out.println(dNow.getTime()-dBefore.getTime());
    //    long strarTm = 1678797636000L;
    //    if ((dNow.getTime()-strarTm)>dNow.getTime()-dBefore.getTime()){
    //      System.out.println("数据不是前三个月的数据");
    //    }else{
    //      //可放入相对应的数据
    //      System.out.println("数据是前三个月的数据");
    //    }
  }

  private static final String LOCALFILE_CONFIG_PATH_ROOT =
      SystemUtils.getUserHome().getAbsolutePath();

  // 获取n个月前的时间戳
  public static long getPriorTime(int month) {
    Date dNow = new Date(); // 当前时间
    Calendar calendar = Calendar.getInstance(); // 得到日历
    calendar.setTime(dNow); // 把当前时间赋给日历
    calendar.add(Calendar.MONTH, -month); // 设置为前3月
    Date dBefore = calendar.getTime(); // 得到前3月的时间
    return dBefore.getTime();
  }

  public static <T> void writeConfig(String configFile, T config) throws IOException {
    String configPath = LOCALFILE_CONFIG_PATH_ROOT + configFile;
    try {
      Path path = Paths.get(configPath);
      Files.write(
          path,
          getObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsBytes(config),
          StandardOpenOption.WRITE,
          StandardOpenOption.TRUNCATE_EXISTING);
    } catch (Exception e) {
      throw e;
    }
  }

  public static long parseTime(String time) {
    // 换个写法
    long temp = 0;
    try {
      SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
      temp = simpleDateFormat.parse(time).getTime();
    } catch (Exception exception) {
      System.out.println(exception);
    }
    return temp;
  }

  public static Properties getPropertiesFromLocal(String path) {
    File file = new File(path);
    boolean exists = file.exists();
    if (!exists) {
      return null;
    }
    Properties prop = new Properties();
    FileInputStream fis = null;
    try {
      File f = new File(path);
      fis = new FileInputStream(f);
      prop.load(fis);
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      if (null != fis) {
        try {
          fis.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }
    return prop;
  }
}
