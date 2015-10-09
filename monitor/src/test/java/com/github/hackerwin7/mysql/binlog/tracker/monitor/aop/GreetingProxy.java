package com.github.hackerwin7.mysql.binlog.tracker.monitor.aop;

import org.springframework.aop.framework.ProxyFactory;

/**
 * Created by hp on 7/28/15.
 */
public class GreetingProxy {
    public static void main(String[] args) throws Exception {
        ProxyFactory factory = new ProxyFactory();
        factory.setTarget(new Greeting());
        factory.addAdvice(new GreetingBefore());
        factory.addAdvice(new GreetingAfter());

        Greeting greeting = (Greeting) factory.getProxy();
        greeting.hello();
    }
}
