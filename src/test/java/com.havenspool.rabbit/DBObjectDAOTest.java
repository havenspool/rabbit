package com.havenspool.rabbit;

/**
 * Created by havens on 15-8-12.
 */
public class DBObjectDAOTest {

    public static void main(String[] args) throws DBException{
        User user=new User();
        user.name="havens";
        user.pwd="123456";

        DBObjectDAO dao=new DBObjectDAO(DataSourceManager.getQueryRunner());
//        dao.insert(user);
        user.id=14;
        user.pwd="654321";
        long t1=System.nanoTime();
        for(int i=0;i<1000;i++)
            dao.updateByColumns(user, "pwd");
        long t2=System.nanoTime();
        user.id=14;
        user.pwd="789445";
        long t3=System.nanoTime();
        for(int i=0;i<1000;i++)
            dao.update(user);
        long t4=System.nanoTime();
        System.out.println("some:"+(t2-t1)+" all:"+(t4-t3)+" fast:"+((t2-t1)-(t4-t3)));
//        dao.delete(user);
    }

}
