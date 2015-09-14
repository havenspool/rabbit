package com.havenspool.rabbit;

import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.ScalarHandler;

import java.lang.reflect.Field;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Set;

/**
 * Created by havens on 15-8-12.
 */
public class DBObjectDAO implements DAO{
    protected QueryRunner innerRunner;
    protected GenKeyQueryRunner innerInsertRunner;
    public DBObjectDAO(QueryRunner queryRunner) {
        setQueryRunner(queryRunner);
    }

    public void setQueryRunner(QueryRunner queryRunner) {
        innerRunner = queryRunner;
        innerInsertRunner = new GenKeyQueryRunner<Long>(queryRunner.getDataSource(),
                new ScalarHandler<Long>());
    }

    public void update(DBObject obj) throws DBException {
        update(obj, obj.getTableName());
    }

    public void insert(DBObject obj) throws DBException {
        insert(obj, obj.getTableName());
    }

    public void delete(DBObject obj) throws DBException {
        delete(obj, obj.getTableName());
    }

    public void update(DBObject obj,String table) throws DBException {
        try {
            Set<String> primary_keys = DBObjectManager.getTablePrimaryKey(table);
            Set<String> columns = DBObjectManager.getTableAllColumnsNoKey(table);
            Object[] objs = new Object[columns.size() + primary_keys.size()];
            int count = 0;
            for (String column : columns) {
                objs[count] = obj.getValueByField(column);
                count++;
            }

            for (String primary_key : primary_keys) {
                objs[count] = obj.getValueByField(primary_key);
                count++;
            }
            String sql = DBObjectManager.getUpdateSQLByTable(table);
            int mount = innerRunner.update(sql, objs);
            if (mount < 1) {
                throw new SQLException("No data update." + sql + "\n" + obj);
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new DBException(e.getMessage());
        }
    }

    public void delete(DBObject obj,String table) throws DBException {
        try {
            Set<String> primary_keys = DBObjectManager.getTablePrimaryKey(table);
            Object[] objs = new Object[primary_keys.size()];
            int count = 0;
            for (String primary_key : primary_keys) {
                objs[count] = obj.getValueByField(primary_key);
                count++;
            }
            String sql = DBObjectManager.getDeleteSQLByTable(table);
            int mount = innerRunner.update(sql, objs);
            if (mount < 1) {
                throw new SQLException("No data delete." + sql + "\n" + obj);
            }
        } catch (Exception e) {
            throw new DBException(e.getMessage());
        }
    }

    public void insert(DBObject obj,String table) throws DBException {
        try {
            Field keyField = DBObjectManager.getInsertIncrKeyField(table);
            Set<String> columns = DBObjectManager.getTableAllColumnsNoIncr(table); // true means if it is not auto increase then add key's column
            Object[] objs = new Object[columns.size()];
            int count = 0;
            for (String column : columns) {
                objs[count] = obj.getValueByField(column);
                count++;
            }

            String sql = DBObjectManager.getInsertSQLByTable(table);
            // no thread safe
            int mount = innerInsertRunner.insert(sql, objs);
            if (mount < 1) {
                throw new SQLException("No data insert." + sql + "\n" + obj);
            }

            if (keyField != null /* is auto increase */) {
                if (keyField.getType().equals(Integer.TYPE)) {
                    keyField.set(obj, ((Long) innerInsertRunner.getGeneratedKeys()).intValue());
                } else if (keyField.getType().equals(Long.TYPE)) {
                    keyField.set(obj, innerInsertRunner.getGeneratedKeys());
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new DBException(e.getMessage());
        }
    }

    public void updateByColumns(DBObject obj, String... columns) throws DBException {
        try {
            String table=obj.getTableName();
            Set<String> primary_keys = DBObjectManager.getTablePrimaryKey(table);
            Object[] objs = new Object[columns.length + primary_keys.size()];
            int count = 0;
            for (String column : columns) {
                objs[count] = obj.getValueByField(column);
                count++;
            }

            for (String primary_key : primary_keys) {
                objs[count] = obj.getValueByField(primary_key);
                count++;
            }
            String sql = someColumnUpdateSQL(table, columns);
            int mount = innerRunner.update(sql, objs);
            if (mount < 1) {
                throw new SQLException("No data update." + sql + "\n" + obj);
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new DBException(e.getMessage());
        }
    }

    private static String someColumnUpdateSQL(String table,String...columns) {
        Set<String> primary_keys = DBObjectManager.getTablePrimaryKey(table);
        StringBuilder s = new StringBuilder("update ").append(table).append(" set ");
        int size = columns.length;
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

    public void update(Collection<? extends DBObject> objs, String table_name) throws DBException {

    }

    public void insert(Collection<? extends DBObject> objs, String table_name) throws DBException {

    }

    public void delete(Collection<? extends DBObject> objs, String table_name) throws DBException {

    }
}
