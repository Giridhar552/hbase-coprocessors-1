package com.frosty.coprocessors;

/**
 * Created with IntelliJ IDEA.
 * User: sbenner
 * Date: 3/25/15
 * Time: 3:42 PM
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class EventsIndexer extends BaseRegionObserver {
    private static final Logger logger = LoggerFactory.getLogger(EventsIndexer.class);
    private Configuration conf = null;
    private HConnection connection = null;

    @Override
    public void start(CoprocessorEnvironment env) throws IOException {
        if (conf == null) conf = env.getConfiguration();
        if (connection == null) connection = HConnectionManager.createConnection(conf);


    }

    public void postPut(final ObserverContext<RegionCoprocessorEnvironment> e,
                        final Put put, final WALEdit edit, final Durability durability) throws IOException {

        String tableName = e.getEnvironment().getRegion().getRegionInfo().getTable().getNameAsString()
                .toLowerCase();

        if (!tableName.equals("event"))
            return;

        if (tableName.equals("event")) {
            try {
                HTableInterface table = connection.getTable("event_idx");

                if (table != null) {

                    String id = new String(CellUtil.cloneValue(put.get(Bytes.toBytes("Event"), Bytes.toBytes("id")).get(0)));
                    String sequence = new String(CellUtil.cloneValue(put.get(Bytes.toBytes("Event"), Bytes.toBytes("sequence")).get(0)));
                    String timestamp = String.valueOf(put.get(Bytes.toBytes("Event"), Bytes.toBytes("id")).get(0).getTimestamp());
                    System.out.println(id + ": " + sequence);
                    byte[] rowkey = Bytes.toBytes(id + ":" + new StringBuffer(sequence).reverse());
                    Put indexput = new Put(rowkey);
                    indexput.add(
                            Bytes.toBytes("rowkey"),
                            Bytes.toBytes("data"),
                            Bytes.toBytes(id));
                    indexput.add(
                            Bytes.toBytes("rowkey"),
                            Bytes.toBytes("timestamp"),
                            Bytes.toBytes(timestamp));
                    // put.add(Bytes.toBytes("rowkey"), null, rowkey);
                    table.put(indexput);
                    table.close();
                }


            } catch (Exception ex) {
                logger.error(ex.getMessage(), ex);
            }
        }


    }

    @Override
    public void stop(CoprocessorEnvironment env) throws IOException {
        // pool.close();
    }


}
