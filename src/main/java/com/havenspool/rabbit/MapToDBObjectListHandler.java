package com.havenspool.rabbit;

import org.apache.commons.dbutils.BasicRowProcessor;
import org.apache.commons.dbutils.ResultSetHandler;
import org.apache.commons.dbutils.RowProcessor;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by havens on 15-8-13.
 */
public class MapToDBObjectListHandler<T extends DBObject> implements ResultSetHandler<List<T>> {

    private RowProcessor convert;
    private MapToDBObject<T> mapToObject;

    public MapToDBObjectListHandler(Class<T> clazz) {
        mapToObject = new MapToDBObject<T>(clazz);
        convert = new BasicRowProcessor();
    }

    public MapToDBObjectListHandler(Class<T> clazz, MapToObjectHandler<T> handler) {
        mapToObject = new MapToDBObject<T>(clazz, handler);
        convert = new BasicRowProcessor();
    }

    public List<T> handle(ResultSet rs) throws SQLException {
        List<T> rows = new ArrayList<T>();
        while (rs.next()) {
            T tmp = this.handleRow(rs);
            if (tmp != null)
                rows.add(tmp);
        }
        return rows;
    }

    private T handleRow(ResultSet rs) throws SQLException {
        Map<String, Object> map = this.convert.toMap(rs);
        return mapToObject.mapToDBObject(map);
    }

}