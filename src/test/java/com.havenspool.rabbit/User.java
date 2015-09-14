package com.havenspool.rabbit;

/**
 * Created by havens on 15-8-10.
 */
public class User extends DBObject{
    public String table_name="users";
    public int id;
    public String name;
    public String pwd;

    public static void main(String[] args) {
        User user=new User();
        user.id=10001;
        user.name="havens";
        user.pwd="123456";
        System.out.println(user.toString());
    }

}
