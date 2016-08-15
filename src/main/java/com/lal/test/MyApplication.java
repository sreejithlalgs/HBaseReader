package com.lal.test;

import java.util.HashSet;
import java.util.Set;

import javax.ws.rs.core.Application;

import com.lal.test.hbase.HBaseReadAPI;

/**
 * @author Sreejithlal G S
 * @since 15-Aug-2016
 * 
 */
public class MyApplication extends Application {

    private Set<Object> singletons;

    public MyApplication() {
        singletons = new HashSet<>();
        singletons.add(new HBaseReadAPI());
    }

    @Override
    public Set<Object> getSingletons() {
        return singletons;
    }

}
