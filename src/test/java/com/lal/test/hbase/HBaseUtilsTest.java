package com.lal.test.hbase;

import static org.junit.Assert.*;

import java.io.IOException;

import org.apache.hadoop.hbase.exceptions.HBaseException;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class HBaseUtilsTest {

    @Before
    public void setUp() throws Exception {
        //set env variable 'HBASE_CONF_DIR'
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    @Ignore
    public void test() {
        try {

            HBaseUtils.createTable("test", "testcf");
            assertTrue(true);
        } catch (IOException e) {
            assertTrue(false);
            e.printStackTrace();
        }
    }

    @Test
    @Ignore
    public void testRead() {
        try {
            String result = HBaseUtils.read("test", "row1", "testcf", "a");
            assertNotNull(result);
        } catch (IOException | HBaseException e) {
            assertTrue(false);
            e.printStackTrace();
        }
    }
    

}
