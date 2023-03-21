package com.datapipeline.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class ParameterTool implements Serializable, Cloneable {
  private static final long serialVersionUID = 100L;
  protected final Map<String, String> data;

  private ParameterTool(Map<Object, Object> data) {
    this.data = Collections.unmodifiableMap(new HashMap(data));
  }

  public String get(String key) {
    return (String) this.data.get(key);
  }

  public boolean has(String key) {
    return this.data.containsKey(key);
  }

  public static ParameterTool fromPropertiesFile(String path) throws IOException {
    File propertiesFile = new File(path);
    return fromPropertiesFile(propertiesFile);
  }

  public static ParameterTool fromPropertiesFile(File file) throws IOException {
    if (!file.exists()) {
      throw new FileNotFoundException(
          "Properties file " + file.getAbsolutePath() + " does not exist");
    } else {
      FileInputStream fis = new FileInputStream(file);
      Throwable var2 = null;

      ParameterTool var3;
      try {
        var3 = fromPropertiesFile((InputStream) fis);
      } catch (Throwable var12) {
        var2 = var12;
        throw var12;
      } finally {
        if (fis != null) {
          if (var2 != null) {
            try {
              fis.close();
            } catch (Throwable var11) {
              var2.addSuppressed(var11);
            }
          } else {
            fis.close();
          }
        }
      }

      return var3;
    }
  }

  public static ParameterTool fromPropertiesFile(InputStream inputStream) throws IOException {
    Properties props = new Properties();
    props.load(inputStream);
    return fromMap(props);
  }

  public static ParameterTool fromMap(Map<Object, Object> map) {
    if (map == null) {
      throw new NullPointerException("Unable to initialize from empty map");
    }
    return new ParameterTool(map);
  }
}
