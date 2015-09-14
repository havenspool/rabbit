package com.havenspool.rabbit;

/**
 * Created by havens on 15-8-12.
 */
public class DBException extends Exception {

    private static final long serialVersionUID = 2319271489032044021L;

    private String reason;

    public DBException(String reason) {
        super(reason);
        this.reason = reason;
    }

    public String reason(){
        return this.reason;
    }
}