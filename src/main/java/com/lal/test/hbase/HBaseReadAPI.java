package com.lal.test.hbase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.apache.hadoop.hbase.exceptions.HBaseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Sreejithlal G S
 * @since 15-Aug-2016
 * 
 */
@Path("/v1")
public class HBaseReadAPI {
    
    private static final Logger LOG = LoggerFactory.getLogger(HBaseReadAPI.class);


    @GET
    @Path("/record")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getRow(@DefaultValue("") @QueryParam("table") String tableName,
            @DefaultValue("") @QueryParam("rowkey") String rowKey,
            @DefaultValue("") @QueryParam("column") String columnFamilyQualifier) {

        if (rowKey.isEmpty() || columnFamilyQualifier.isEmpty() || rowKey.isEmpty()) {
            return getResponse(400, "table/rowkey/column is missing. please check request");
        }
        
        LOG.info("read api invoked with table: {}  rowkey: {} column: {}",tableName, rowKey, columnFamilyQualifier);

        String[] values = columnFamilyQualifier.split(":");
        Map<String, String> errorMap = new HashMap<>();

        try {
            String result = values.length >= 2 ? HBaseUtils.read(tableName, rowKey, values[0], values[1])
                    : HBaseUtils.read(tableName, rowKey, values[0]);
            if (result == null) {
                errorMap.put("error_msg", "No records found");
                return getResponse(204, HBaseUtils.toJson(errorMap));
            }
            return getResponse(200, result);
        } catch (IOException | HBaseException e) {
            LOG.error(e.getMessage(),e.getCause());
            errorMap.put("error_msg", e.getMessage());
            return getResponse(500, HBaseUtils.toJson(errorMap));
        }

    }

    private static Response getResponse(int code, String msg) {
        return Response.status(Status.fromStatusCode(code)).entity(msg).build();
    }
}
