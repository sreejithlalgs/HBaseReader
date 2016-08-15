package com.lal.test.hbase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.exceptions.HBaseException;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;

/**
 * @author Sreejithlal G S
 * @since 15-Aug-2016
 * 
 */
public class HBaseUtils {

    private static final Logger LOG = LoggerFactory.getLogger(HBaseUtils.class);

    public static void createTable(String tableName, String... columnFamilies) throws IOException {

        Configuration conf = HBaseConfiguration.create();
        conf.addResource(new Path(System.getenv("HBASE_CONF_DIR"), "hbase-site.xml"));

        LOG.info("Started creating table {}", tableName);

        TableName hTableName = TableName.valueOf(tableName);
        try (Connection connection = ConnectionFactory.createConnection(conf)) {

            Admin admin = connection.getAdmin();
            if (admin.tableExists(hTableName)) {
                LOG.info("table already exists, deleting table {} and creating new", tableName);
                admin.deleteTable(hTableName);
            }
            HTableDescriptor table = new HTableDescriptor(hTableName);
            for (String eachCF : columnFamilies) {
                HColumnDescriptor newColumn = new HColumnDescriptor(eachCF);
                newColumn.setCompactionCompressionType(Algorithm.GZ);
                newColumn.setMaxVersions(HConstants.ALL_VERSIONS);
                admin.addColumn(hTableName, newColumn);
                table.addFamily(newColumn);
            }
            admin.createTable(table);
            LOG.info("table: {} successfully created", tableName);
        }

    }

    public static String read(String tableName, String rowKey, String family) throws IOException, HBaseException {
        LOG.info("getttig data from table {} with row key {}", tableName, rowKey);
        LOG.info(System.getenv("HBASE_CONF_DIR"));
        Configuration conf = HBaseConfiguration.create();
        conf.addResource(new Path(System.getenv("HBASE_CONF_DIR"), "hbase-site.xml"));
        TableName hTableName = TableName.valueOf(tableName);
        try (Connection connection = ConnectionFactory.createConnection(conf)) {
            Table table = connection.getTable(hTableName);
            if (table == null)
                throw new HBaseException("No table found with name: " + tableName);
            Get get = new Get(Bytes.toBytes(rowKey));
            get.addFamily(Bytes.toBytes(family));
            Result result = table.get(get);
            Map<String, String> data = new HashMap<>();
            for (Cell cell : result.rawCells()) {
                data.put(
                        Bytes.toString(CellUtil.cloneFamily(cell)) + ":"
                                + Bytes.toString(CellUtil.cloneQualifier(cell)),
                        Bytes.toString(CellUtil.cloneValue(cell)));
            }

            if (data.isEmpty()) {
                return null;
            }
            return toJson(data);
        }
    }

    public static String read(String tableName, String rowKey, String family, String qualifier)
            throws IOException, HBaseException {
        LOG.info("getttig data from table {} with row key {}", tableName, rowKey);
        LOG.info(System.getenv("HBASE_CONF_DIR"));
        Configuration conf = HBaseConfiguration.create();
        conf.addResource(new Path(System.getenv("HBASE_CONF_DIR"), "hbase-site.xml"));
        TableName hTableName = TableName.valueOf(tableName);
        try (Connection connection = ConnectionFactory.createConnection(conf)) {
            Table table = connection.getTable(hTableName);
            if (table == null)
                throw new HBaseException("No table found with name: " + tableName);

            Get get = new Get(Bytes.toBytes(rowKey));
            get.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
            Result result = table.get(get);
            Map<String, String> data = new HashMap<>();
            for (Cell cell : result.rawCells()) {
                data.put(
                        Bytes.toString(CellUtil.cloneFamily(cell)) + ":"
                                + Bytes.toString(CellUtil.cloneQualifier(cell)),
                        Bytes.toString(CellUtil.cloneValue(cell)));
            }

            if (data.isEmpty()) {
                return null;
            }
            return toJson(data);

        }
    }

    public static String toJson(Object map) {
        Gson gson = new Gson();
        return gson.toJson(map);
    }
}
