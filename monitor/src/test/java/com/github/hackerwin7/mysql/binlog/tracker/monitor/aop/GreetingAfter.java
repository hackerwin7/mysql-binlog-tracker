package com.github.hackerwin7.mysql.binlog.tracker.monitor.aop;

import org.springframework.aop.AfterReturningAdvice;

import java.lang.reflect.Method;

/**
 * Created by hp on 7/28/15.
 */
public class GreetingAfter implements AfterReturningAdvice {

    @Override
    public void afterReturning(Object object, Method method, Object[] obs, Object ob)  throws Exception {
        System.out.println("after");
    }
}
