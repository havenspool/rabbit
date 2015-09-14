package com.havenspool.rabbit;

import java.sql.SQLException;

/**
 * Created by havens on 15-8-13.
 */
public class UserImplDao extends DBObjectDAO implements UserDao{

    public UserImplDao() {
        super(DataSourceManager.getQueryRunner());
    }

    public User getUser(int id){
        User user=null;
        try {
            user =innerRunner.query("select * from users where id = ? ",new MapToDBObjectHandler<User>(User.class), id);
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return user;
    }

    public User getUser(String name){
        User user=null;
        try {
            user = innerRunner.query("select * from users where name = ? ",new MapToDBObjectHandler<User>(User.class), name);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return user;
    }

    public void insert(User user) {
        insert(user);
    }

    public void update(User user) {
        update(user);
    }
    public void update(User user,String... columns) {
        update(user,columns);
    }

    public void delete(User user) {
        delete(user);
    }
    public void delete(User user,String... columns) {
        delete(user,columns);
    }

    public void deleteById(int id) {
        delete(getUser(id));
    }

}
