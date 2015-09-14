package com.havenspool.rabbit;

import com.jolbox.bonecp.BoneCPDataSource;
import org.apache.commons.dbutils.QueryRunner;

import javax.sql.DataSource;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Created by havens on 15-8-12.
 */
public class DataSourceManager {
    private Map<String, DataSource> dataSources = new ConcurrentHashMap<String, DataSource>(10);
    private DataSource dataSource = null;
    private static final DataSourceManager INSTANCE;

    static {
        INSTANCE = new DataSourceManager();
    }

    private DataSourceManager() {
        init(DBManager.DB_CONFIG.dataSources);
    }

    public static DataSourceManager getInstance(){
        return INSTANCE;
    }

    public void init(Map<String, DataSourceConf> _dataSourceConf) {
        for (String name : _dataSourceConf.keySet()) {
            DataSourceConf dataSourceConf = _dataSourceConf.get(name);
            try {
                // setup the connection pool
                BoneCPDataSource boneCPDataSource = new BoneCPDataSource();
                boneCPDataSource.setDriverClass(dataSourceConf.driver);
                boneCPDataSource.setJdbcUrl(dataSourceConf.url); //
                boneCPDataSource.setUsername(dataSourceConf.user);
                boneCPDataSource.setPassword(dataSourceConf.password);
//                boneCPDataSource.setIdleConnectionTestPeriod(300);
                boneCPDataSource.setMinConnectionsPerPartition(5);
                boneCPDataSource.setMaxConnectionsPerPartition(15);
//            boneCPDataSource.setPartitionCount(1);
                boneCPDataSource.setPartitionCount(3);
                boneCPDataSource.setAcquireIncrement(5);
//                boneCPDataSource.setIdleMaxAge(240);
                boneCPDataSource.setIdleMaxAge(0, TimeUnit.MILLISECONDS);
                boneCPDataSource.setConnectionTimeoutInMs(10000); // process time out
                boneCPDataSource.setIdleConnectionTestPeriodInMinutes(10);
                boneCPDataSource.setConnectionTestStatement("/* ping */ SELECT 1");
                if (dataSourceConf._default)
                    dataSource = boneCPDataSource;
                dataSources.put(name, boneCPDataSource);
            } catch (Exception e) {
                e.printStackTrace();
            }

//            if (dataSource != null) {
//                DBOperationHelper.setInnerRunner(new QueryRunner(dataSource));
//                DBBatchOperationHelper.setInnerRunner(new QueryRunner(dataSource));
//            }
        }
    }

    public QueryRunner newQueryRunner() {
        return new QueryRunner(dataSource);
    }

    public QueryRunner newQueryRunner(String name) {
        DataSource _dataSource = dataSources.get(name);
        if (_dataSource != null) {
            return new QueryRunner(_dataSource);
        }
        return null;
    }

    public static QueryRunner getQueryRunner() {
        return INSTANCE.newQueryRunner();
    }

    public static QueryRunner getQueryRunner(final String name) {
        return INSTANCE.newQueryRunner(name);
    }
}
