package com.datapipeline.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import java.beans.BeanInfo;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Method;
import java.util.LinkedHashMap;
import java.util.Map;

public class ObjectConvert {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  static {
    OBJECT_MAPPER.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
    OBJECT_MAPPER.configure(DeserializationFeature.FAIL_ON_INVALID_SUBTYPE, false);
    OBJECT_MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
  }

  public static <T> T getObject(Object obj, Class<T> clazz) {
    try {
      if (obj instanceof String) {
        return OBJECT_MAPPER.readValue((String) obj, clazz);
      }
    } catch (Throwable t) {
      throw new IllegalArgumentException(
          obj + " cannot convert to object: " + clazz.getName() + " reason:" + t.getMessage());
    }
    return OBJECT_MAPPER.convertValue(obj, clazz);
  }

  public static <T> T getObject(Object obj, TypeReference<T> typeReference) {
    try {
      if (obj instanceof String) {
        return OBJECT_MAPPER.readValue((String) obj, typeReference);
      }
    } catch (Throwable t) {
      throw new IllegalArgumentException(
          obj
              + " cannot convert to object: "
              + typeReference.getType()
              + " reason:"
              + t.getMessage());
    }
    return OBJECT_MAPPER.convertValue(obj, typeReference);
  }

  public static ObjectMapper getObjectMapper() {
    return OBJECT_MAPPER;
  }

  public static byte[] getJsonBytes(Object obj) {
    try {
      return OBJECT_MAPPER.writeValueAsBytes(obj);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  public static String getJsonString(Object obj) {
    try {
      return OBJECT_MAPPER.writeValueAsString(obj);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  public static Map<String, Object> beanToMap(Object bean) throws Exception {
    Map<String, Object> map = new LinkedHashMap<>();
    BeanInfo b = Introspector.getBeanInfo(bean.getClass(), Object.class);
    PropertyDescriptor[] pds = b.getPropertyDescriptors();
    for (PropertyDescriptor pd : pds) {
      String propertyName = pd.getName();
      Method m = pd.getReadMethod();
      Object properValue = m.invoke(bean);
      map.put(propertyName, properValue);
    }
    return map;
  }

  public static <T> T mapToBean(Map<String, Object> map, Class<T> clz) throws Exception {
    T obj = clz.newInstance();
    BeanInfo b = Introspector.getBeanInfo(clz, Object.class);
    PropertyDescriptor[] pds = b.getPropertyDescriptors();
    for (PropertyDescriptor pd : pds) {
      Method setter = pd.getWriteMethod();
      setter.invoke(obj, map.get(pd.getName()));
    }
    return obj;
  }
}
