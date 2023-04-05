package com.datapipeline.utils;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

public class DateUtils {

  public static long dateStringTransMilli(String date) {
    return dateStringTransMilliOfPattern(date, "yyyy-MM-dd HH:mm:ss");
  }

  public static long dateStringTransMilliOfPattern(String date, String pattern) {
    LocalDateTime parse = LocalDateTime.parse(date, DateTimeFormatter.ofPattern(pattern));
    Instant instant = parse.toInstant(ZoneOffset.of("+8"));
    return instant.toEpochMilli();
  }
}
