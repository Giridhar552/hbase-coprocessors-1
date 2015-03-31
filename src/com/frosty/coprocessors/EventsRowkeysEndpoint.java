package com.frosty.coprocessors;

/**
 * Created with IntelliJ IDEA.
 * User: sbenner
 * Date: 3/25/15
 * Time: 3:42 PM
 */

import com.frosty.coprocessors.generated.EventsRowkeyProtos;
import com.frosty.coprocessors.generated.EventsRowkeyProtos.EventsRowkeyService;
import com.google.protobuf.ByteString;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.CoprocessorException;
import org.apache.hadoop.hbase.coprocessor.CoprocessorService;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.protobuf.ResponseConverter;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class EventsRowkeysEndpoint extends EventsRowkeyService implements Coprocessor, CoprocessorService {

    private static final Logger logger = LoggerFactory.getLogger(EventsRowkeysEndpoint.class);
    ;
    private RegionCoprocessorEnvironment env;

    public EventsRowkeysEndpoint() {
    }

    @Override
    public void getRowkeys(RpcController controller,
                           EventsRowkeyProtos.RowkeyRequest request,
                           RpcCallback<EventsRowkeyProtos.RowkeyResponse> done) {

        Scan scan = new Scan();

        long minTimestamp = request.getMinTimestamp();
        long maxTimestamp = request.getMaxTimestamp();

        logger.info("minTimestamp" + minTimestamp);
        logger.info("maxTimestamp" + maxTimestamp);


        // scan.setFilter(new KeyOnlyFilter());
        // scan.setFilter(new FirstKeyOnlyFilter());

        try {

            scan.addColumn(Bytes.toBytes("rowkey"), Bytes.toBytes("data"));

            scan.setTimeRange(minTimestamp, maxTimestamp);
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }
        EventsRowkeyProtos.RowkeyResponse response = null;
        InternalScanner scanner = null;
        try {
            scanner = env.getRegion().getScanner(scan);
            List<Cell> results = new ArrayList<Cell>();
            boolean hasMore = false;

            EventsRowkeyProtos.RowkeyResponse.Builder b = EventsRowkeyProtos.RowkeyResponse.newBuilder();
            do {
                hasMore = scanner.next(results);
                for (Cell kv : results) {
                    byte[] currentRow = CellUtil.cloneValue(kv); //CellUtil.cloneRow(kv);
                    b.addRowkeys(ByteString.copyFrom(currentRow));

                }
                results.clear();
            } while (hasMore);

//
//            for (ByteString s : b.getRowkeysList()) {
//                System.out.println(s.toString());
//            }

            if (b.getRowkeysCount() > 1) {
                response = b.build();
            }

        } catch (IOException ioe) {
            ResponseConverter.setControllerException(controller, ioe);
        } finally {
            if (scanner != null) {
                try {
                    scanner.close();
                } catch (IOException ignored) {
                }
            }
        }
        done.run(response);
    }

    @Override
    public void getRowkeyKeyValue(RpcController controller, EventsRowkeyProtos.RowkeyRequest request, RpcCallback<EventsRowkeyProtos.RowkeyResponse> done) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void start(CoprocessorEnvironment env) throws IOException {
        //   if (conf == null) conf = env.getConfiguration();
        // if (connection == null) connection = HConnectionManager.createConnection(conf);
        if (env instanceof RegionCoprocessorEnvironment) {
            this.env = (RegionCoprocessorEnvironment) env;
        } else {
            throw new CoprocessorException("Must be loaded on a table region!");
        }

    }

    @Override
    public void stop(CoprocessorEnvironment env) throws IOException {
        // pool.close();
    }

    @Override
    public Service getService() {
        return this;
    }
}
