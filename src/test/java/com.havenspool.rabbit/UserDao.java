package com.havenspool.rabbit;

/**
 * Created by havens on 15-8-13.
 */
public interface UserDao {
    User getUser(int id);
    User getUser(String name);
    void insert(User user);
    void update(User user);
    void delete(User user);
    void update(User user,String... columns);
    void delete(User user,String... columns);
    void deleteById(int id);
}
