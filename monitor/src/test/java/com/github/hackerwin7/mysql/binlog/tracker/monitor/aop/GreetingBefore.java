package com.github.hackerwin7.mysql.binlog.tracker.monitor.aop;


import org.springframework.aop.MethodBeforeAdvice;

import java.lang.reflect.Method;

/**
 * Created by hp on 7/28/15.
 */
public class GreetingBefore implements MethodBeforeAdvice {

    @Override
    public void before(Method method, Object[] obs, Object ob) throws Exception {
        System.out.println("before");
    }
}
