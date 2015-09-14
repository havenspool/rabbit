package com.havenspool.rabbit;

import java.util.Collection;

/**
 * Created by havens on 15-8-12.
 */
public interface DAO {
    void update(DBObject obj) throws DBException;

    void delete(DBObject obj) throws DBException;

    void insert(DBObject obj) throws DBException;

    void updateByColumns(DBObject obj, String... columns) throws DBException;

    void update(DBObject obj, String table) throws DBException;

    void delete(DBObject obj, String table) throws DBException;

    void insert(DBObject obj, String table) throws DBException;

    /*

      BATCH OPERATION

     */

    void update(Collection<? extends DBObject> objs, String table_name) throws DBException;

    void insert(Collection<? extends DBObject> objs, String table_name) throws DBException;

    void delete(Collection<? extends DBObject> objs, String table_name) throws DBException;
}
