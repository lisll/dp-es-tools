package com.datapipeline.utils;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import org.apache.commons.lang3.StringUtils;

public class DateUtils {

  public static void main(String[] args) {
    System.out.println(StringUtils.substringBeforeLast(null,"_"));
    System.out.println(StringUtils.substringAfterLast("2023-04_456","_"));
  }

  public static long dateStringTransMilli(String date) {
    return dateStringTransMilliOfPattern(date, "yyyy-MM-dd HH:mm:ss");
  }

  public static long dateStringTransMilliOfPattern(String date, String pattern) {
    LocalDateTime parse = LocalDateTime.parse(date, DateTimeFormatter.ofPattern(pattern));
    Instant instant = parse.toInstant(ZoneOffset.of("+8"));
    return instant.toEpochMilli();
  }
}
