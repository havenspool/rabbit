package com.havenspool.rabbit;

import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Primitives;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by havens on 15-8-10.
 */
public class DBObject extends JSONObject implements Serializable{

    public String getTableName(){
        Class clazz = getClass();
        String tname = DBObjectManager.getTableNameByObject(clazz);
        if (tname == null) {
            try {
                Field field = clazz.getField("table_name");
                while (field == null && clazz.getSuperclass() != null) {
                    clazz = clazz.getSuperclass();
                    field = clazz.getField("table_name");
                }
                if (field == null) {
                    tname = clazz.getSimpleName() + "s";
                } else
                    tname = (String) field.get(this);
            } catch (Exception e) {
                tname = clazz.getSimpleName() + "s";
            }
            DBObjectManager.setTableNameByClass(clazz, tname);
        }
        return tname;
    }

    public String toString() {
        return __getValueToString();
    }

    private String __getValueToString() {
        StringBuilder sb = new StringBuilder();
        try {
            Map<String, Field> allFields = getAllFields();
            for (Field field : allFields.values()) {
                Class type = field.getType();
                if (Boolean.TYPE == type || Integer.TYPE == type
                        || Long.TYPE == type || Double.TYPE == type
                        || type.equals(String.class)) {
                    String name = field.getName();
                    Object o = "";
                    try {
                        o = field.get(this);
                        if (o == null)
                            o = "";
                    } catch (Exception ignored) {
                    }
                    sb.append(name).append(",").append(o).append(";");
                }
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return sb.toString();
    }

    private Map<String, Field> _allFields = null;
    public Map<String, Field> getAllFields() {
        if (_allFields == null) {
            _allFields = getClazzField(this.getClass());
        }
        return _allFields;
    }

    private static Map<Class, Map<String, Field>> ObjectFieldCache = new ConcurrentHashMap<Class, Map<String, Field>>();
    public static Map<String, Field> getClazzField(final Class clazz) {
        Map<String, Field> result = ObjectFieldCache.get(clazz);
        if (result == null) {
            registerTableClassField(clazz);
            result = ObjectFieldCache.get(clazz);
        }
        return result;
    }
    private static void registerTableClassField(Class clazz) {
        if (ObjectFieldCache.get(clazz) == null) {
            ImmutableMap.Builder<String, Field> classMap = ImmutableMap.builder();
            Class uper = clazz;
            while (uper != null) {
                for (Field field : uper.getDeclaredFields()) {
                    if (Modifier.isFinal(field.getModifiers())
                            || Modifier.isStatic(field.getModifiers())
                            || !Modifier.isPublic(field.getModifiers())
                            || field.getType().isArray())
                        continue;
                    classMap.put(field.getName(), field);
                }
                uper = uper.getSuperclass();
            }
            ObjectFieldCache.put(clazz, classMap.build());
        }
    }

    public Object getValueByField(String name) {
        Field field = getAllFields().get(name);
        if (field != null) {
            try {
                return field.get(this);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return null;
    }

    public void JsonToObj(JSONObject json) {
        Map<String, Field> allFields = getAllFields();
        for (Object attr : json.keySet()) {
            Object value = json.opt((String) attr);
            Field field = allFields.get(attr);
            if (field != null
                    && value != null) {
                Class type = field.getType();
                __setValueToObj(value, field, type);
            }
        }
    }

    public void MapToObj(Map<String, Object> map) {
        Map<String, Field> allFields = getAllFields();
        for (String fieldName : map.keySet()) {
            Field field = allFields.get(fieldName);
            Object value = map.get(fieldName);
            if (field != null) {
                Class type = field.getType();
                __setValueToObj(value, field, type);
            }
        }
    }

    private void __setValueToObj(final Object value, final Field field, final Class type) {
        try {
            if (Boolean.TYPE == type) {
                if (value instanceof Boolean) {
                    field.setBoolean(this, (Boolean) value);
                } else {
                    int ivalue = value instanceof Number ? ((Number) value).intValue()
                            : Integer.parseInt((String) value);
                    field.setBoolean(this, ivalue == 1);
                }
            } else if (type.isPrimitive() || type.equals(String.class) || Primitives.isWrapperType(type)) {
                field.set(this, value);
            } else if (List.class.isAssignableFrom(type)) {
                if (value instanceof JSONArray) {
                    ParameterizedType listType = (ParameterizedType) field.getGenericType();
                    Class<?> listClass = (Class<?>) listType.getActualTypeArguments()[0];
                    if (DBObject.class.isAssignableFrom(listClass)) {
                        List tmp = new ArrayList();
                        JSONArray jsonArray = (JSONArray) value;
                        for (int i = 0; i < jsonArray.length(); i++) {
                            Object o = listClass.newInstance();
                            ((DBObject) o).JsonToObj(jsonArray.getJSONObject(i));
                            tmp.add(o);
                        }
                        field.set(this, tmp);
                    }
                } else if (value instanceof List) {
                    ParameterizedType listType = (ParameterizedType) field.getGenericType();
                    Class<?> listClass = (Class<?>) listType.getActualTypeArguments()[0];
                    List tmp = new ArrayList();
                    if (DBObject.class.isAssignableFrom(listClass)) {
                        List mapArray = (List) value;
                        for (Object mapData : mapArray) {
                            Object o = listClass.newInstance();
                            ((DBObject) o).MapToObj((Map) mapData);
                            tmp.add(o);
                        }
                    }
                    field.set(this, tmp);
                } else if (value instanceof Map[]) {
                    ParameterizedType listType = (ParameterizedType) field.getGenericType();
                    Class<?> listClass = (Class<?>) listType.getActualTypeArguments()[0];
                    List tmp = new ArrayList();
                    if (DBObject.class.isAssignableFrom(listClass)) {
                        Map[] mapArray = (Map[]) value;
                        for (int i = 0; i < mapArray.length; i++) {
                            Object o = listClass.newInstance();
                            ((DBObject) o).MapToObj(mapArray[i]);
                            tmp.add(o);
                        }
                    }
                    field.set(this, tmp);
                }
                //  for mongodb
                //
//                    else if (value instanceof BasicDBObject) {
//                        Object o = type.newInstance();
//                        ((DBObject) o).MapToObj((BasicDBObject) value);
//                        field.set(obj, o);
//                    }

            } else if (DBObject.class.isAssignableFrom(type)) {
                if (value instanceof JSONObject) {
                    Object o = type.newInstance();
                    ((DBObject) o).JsonToObj((JSONObject) value);
                    field.set(this, o);
                } else if (value instanceof String) {
                    Object o = type.newInstance();
                    ((DBObject) o).JsonToObj(new JSONObject((String) value));
                    field.set(this, o);
                } else if (value instanceof Map) {
                    Object o = type.newInstance();
                    ((DBObject) o).MapToObj((Map)value);
                    field.set(this, o);
                }
            }
//            else {
//                field.set(this, value);
//            }

        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }


}
