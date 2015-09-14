package com.havenspool.rabbit;

import java.util.Map;

/**
 * Created by havens on 15-8-12.
 */
public class DataSourceConf {
    public String driver;
    public String name;
    public String url;
    public String user;
    public String password;
    public boolean _default;

    // table and class relationship
    public Map<String, String> tableToClass;
    public Map<String, String> tableExcludes;
    public Map<String, String> tableToKeyField;
    public Map<String, String> classToTable;
}
