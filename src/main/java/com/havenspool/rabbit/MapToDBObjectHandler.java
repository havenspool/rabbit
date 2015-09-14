package com.havenspool.rabbit;

import org.apache.commons.dbutils.BasicRowProcessor;
import org.apache.commons.dbutils.ResultSetHandler;
import org.apache.commons.dbutils.RowProcessor;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

/**
 * Created by havens on 15-8-13.
 */
public class MapToDBObjectHandler<T extends DBObject> implements ResultSetHandler<T> {

    private RowProcessor convert;
    private MapToDBObject<T> mapToObject;

    public MapToDBObjectHandler(Class<T> clazz) {
        mapToObject = new MapToDBObject<T>(clazz);
        convert = new BasicRowProcessor();
    }

    public T handle(ResultSet rs) throws SQLException {
        Map<String,Object> map = rs.next() ? this.convert.toMap(rs) : null;
        if (map == null) return null;
        return mapToObject.mapToDBObject(map);
    }


}
