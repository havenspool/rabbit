package com.havenspool.rabbit;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.dbutils.QueryRunner;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by havens on 15-8-12.
 */
public class DBObjectManager {
    private static final String TABLE_SUFFIX_ALL_NO_INCR = "_all_no_incr";
    private static final String TABLE_SUFFIX_NO_KEY = "_no_key";
    private static final String TABLE_SUFFIX_ALL = "_all";
    private static final String TABLE_SUFFIX_KEY = "_key";

    private static final String OBJECT_ATTR_EXCLUDE_SPLIT_KEY = ",";

    private static Map<String, Set<String>> ObjectColumnsCache = new ConcurrentHashMap<String, Set<String>>();
    private static Map<String, Class> ObjectTable = new ConcurrentHashMap<String, Class>();
    private static Map<Class, String> ObjectCache = new ConcurrentHashMap<Class, String>();
    private static Map<Class, Map<String, Field>> ObjectFieldCache = new ConcurrentHashMap<Class, Map<String, Field>>();

    private static Map<String, String> SQLINSERTCACHE = new ConcurrentHashMap<String, String>();
    private static Map<String, String> SQLUPDATECACHE = new ConcurrentHashMap<String, String>();
    private static Map<String, String> SQLDELETECACHE = new ConcurrentHashMap<String, String>();

    private static Map<String, Field> TableInsertIncrKeyField = new ConcurrentHashMap<String, Field>();

    static {
        init(DBManager.DB_CONFIG);
    }
    public static void init(DBConfig dbConfig){
        try {
            for (String name : dbConfig.dataSources.keySet()) {
                DataSourceConf df = dbConfig.dataSources.get(name);
                DBObjectManager.registerTables(DataSourceManager.getQueryRunner(name), df);
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    private static void registerTables(QueryRunner queryRunner,
                                       DataSourceConf config) throws SQLException {
        String key;
        Connection con = null;
        ResultSet rs = null;
        try {
            con = queryRunner.getDataSource().getConnection();
            for (String table : config.tableToClass.keySet()) {
                try {
                    String incrKey = null;
                    Set<String> keys = new HashSet<String>();
                    Set<String> no_incr_key_columns = new HashSet<String>();
                    Set<String> no_key_columns = new HashSet<String>();
                    Set<String> all_columns = new HashSet<String>();
                    rs = con.getMetaData().getPrimaryKeys(null, null, table);
                    while (rs.next()) { // column name in the NO. 4
                        key = rs.getString(4);   //rs.getString("COLUMN_NAME");
                        keys.add(key);
                    }
                    rs.close();
//            con = queryRunner.getDataSource().getConnection();
                    rs = con.getMetaData().getColumns(null, null, table, null);
                    while (rs.next()) { // column name in the NO. 4
                        String name = rs.getString(4); //rs.getString("COLUMN_NAME");
                        all_columns.add(name);
                        if (keys.contains(name)) {
                            if (!"YES".equals(rs.getString("IS_AUTOINCREMENT"))) {
                                no_incr_key_columns.add(name);
                            } else {
                                incrKey = name;
                            }
//                    System.out.println(rs.getString("IS_AUTOINCREMENT")); "YES"
                            // nothing
                        } else {
                            no_incr_key_columns.add(name);
                            no_key_columns.add(name);
                        }
                    }

                    ObjectColumnsCache.put(table + TABLE_SUFFIX_ALL_NO_INCR, no_incr_key_columns);
                    ObjectColumnsCache.put(table + TABLE_SUFFIX_NO_KEY, no_key_columns);
                    ObjectColumnsCache.put(table + TABLE_SUFFIX_ALL, all_columns);
                    ObjectColumnsCache.put(table + TABLE_SUFFIX_KEY, keys);

                    String className = config.tableToClass.get(table);

                    Class clazz = Class.forName(className);

                    registerTableClassField(clazz);

                    Set<String> excludes = new HashSet<String>();
                    String[] sss = config.tableExcludes.get(table).split(OBJECT_ATTR_EXCLUDE_SPLIT_KEY);
                    Collections.addAll(excludes, sss);

                    no_key_columns.removeAll(excludes); // remove update attribute

                    // must register json attribute and then json register key
                    DBObjectManager.registerTableClass(table, clazz);
                    if (incrKey != null) {
                        Field field = ObjectFieldCache.get(clazz).get(incrKey);
                        if (field != null) {
                            TableInsertIncrKeyField.put(table, field);
                        }
                    }
                } catch (Exception ex) {
                    ex.printStackTrace();
                }

            }
        } finally {
            if (rs != null)
                rs.close();
//            DbUtils.close(con);
        }

        for (String table : config.tableToClass.keySet()) {
            SQLINSERTCACHE.put(table, makeInsertSQL(table));
            SQLUPDATECACHE.put(table, makeUpdateSQL(table));
            SQLDELETECACHE.put(table, makeDeleteSQL(table));
        }

        for (String className : config.classToTable.keySet()) {
            try {
                Class clazz = Class.forName(className);
                String table = config.classToTable.get(className);
                DBObjectManager.setTableNameByClass(clazz, table);
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
        }
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

    protected static void registerTableClass(String table_name, Class clazz) {
        ObjectTable.put(table_name, clazz);
    }

    public static Class getClassByTable(String table_name) {
        return ObjectTable.get(table_name);
    }

    public static String getTableNameByObject(Class clazz){
        return ObjectCache.get(clazz);
    }

    public static void setTableNameByClass(Class clazz,String table_name){
        ObjectCache.put(clazz, table_name);
    }

    public static Set<String> getTableAllColumnsNoIncr(String table) {
        return ObjectColumnsCache.get(table + TABLE_SUFFIX_ALL_NO_INCR);
    }
    public static Set<String> getTableAllColumnsNoKey(String table) {
        return ObjectColumnsCache.get(table + TABLE_SUFFIX_NO_KEY);
    }

    public static Set<String> getTablePrimaryKey(String table) {
        return ObjectColumnsCache.get(table + TABLE_SUFFIX_KEY);
    }
    public static Field getInsertIncrKeyField(String table_name) {
        return TableInsertIncrKeyField.get(table_name);
    }
    public static String getInsertSQLByTable(final String table) {
        return SQLINSERTCACHE.get(table);
    }

    public static String getUpdateSQLByTable(final String table) {
        return SQLUPDATECACHE.get(table);
    }

    public static String getDeleteSQLByTable(final String table) {
        return SQLDELETECACHE.get(table);
    }

    private static String makeInsertSQL(String table) {
        StringBuilder s = new StringBuilder("insert into ").append(table).append("(");
        StringBuilder sv = new StringBuilder(" values(");

        Set<String> columns = DBObjectManager.getTableAllColumnsNoIncr(table); // true means if it is not auto increase then add key's column
        int size = columns.size();
        for (String column : columns) {
            if (--size == 0) {
                // last item
                s.append('`').append(column).append("`)");
                sv.append("?)");
            } else {
                s.append('`').append(column).append("`,");
                sv.append("?,");
            }
        }
        return s.append(sv).toString();
    }

    private static String makeUpdateSQL(String table) {
        Set<String> primary_keys = DBObjectManager.getTablePrimaryKey(table);
        StringBuilder s = new StringBuilder("update ").append(table).append(" set ");
        Set<String> columns = DBObjectManager.getTableAllColumnsNoKey(table);
        int size = columns.size();
        for (String column : columns) {
            if (--size == 0) {
                // last item
                s.append('`').append(column).append("`=? ");
            } else {
                s.append('`').append(column).append("`=?, ");
            }
        }
        s.append(" where ");
        int key_size = primary_keys.size();
        int kye_i = 1;
        for (String primary_key : primary_keys) {
            s.append('`').append(primary_key).append("` = ? ");
            if (kye_i != key_size)
                s.append(" AND ");
            kye_i++;
        }
        return s.toString();
    }

    private static String makeDeleteSQL(String table) {
        Set<String> primary_keys = DBObjectManager.getTablePrimaryKey(table);
        StringBuilder s = new StringBuilder("delete from ");
        s.append(table).append(" where ");
        int key_size = primary_keys.size();
        int kye_i = 1;
        for (String primary_key : primary_keys) {
            s.append('`').append(primary_key).append("` = ? ");
            if (kye_i != key_size)
                s.append(" AND ");
            kye_i++;
        }
        return s.toString();
    }
}
