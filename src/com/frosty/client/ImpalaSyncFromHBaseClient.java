package com.frosty.client;

import com.frosty.coprocessors.generated.EventsRowkeyProtos;
import com.google.protobuf.ByteString;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.apache.hadoop.hbase.ipc.ServerRpcController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: sbenner
 * Date: 3/30/15
 * Time: 10:56 PM
 */
public class ImpalaSyncFromHBaseClient {

    private static final Logger logger = LoggerFactory.getLogger(ImpalaSyncFromHBaseClient.class);

    public static void main(String[] args) {
        Configuration conf = HBaseConfiguration.create();


        Configuration customConf = new Configuration();
        customConf.setStrings("hbase.zookeeper.quorum",
                "localhost");
        // Increase RPC timeout, in case of a slow computation
        customConf.setLong("hbase.rpc.timeout", 600000);
        // Default is 1, set to a higher value for faster scanner.next(..)
        customConf.setLong("hbase.client.scanner.caching", 1000);

        HTable table = null;

        try {
            table = new HTable(conf, "event_idx");
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }

        final EventsRowkeyProtos.RowkeyRequest request = EventsRowkeyProtos.RowkeyRequest.getDefaultInstance();

        EventsRowkeyProtos.RowkeyRequest.Builder newBuilder = request.toBuilder();

        newBuilder.setMinTimestamp(1427627090180L);
        newBuilder.setMaxTimestamp(4102444800000L);


        EventsRowkeyProtos.RowkeyRequest build = newBuilder.build();
        final byte[] info = build.toByteArray();

        if (table != null) {
            Map<byte[], List<ByteString>> results = null;


            try {
                results = table.coprocessorService(EventsRowkeyProtos.EventsRowkeyService.class,
                        null, null,
                        new Batch.Call<EventsRowkeyProtos.EventsRowkeyService, List<ByteString>>() {

                            public List<ByteString> call(EventsRowkeyProtos.EventsRowkeyService instance)
                                    throws IOException {
                                ServerRpcController controller = new ServerRpcController();
                                final EventsRowkeyProtos.RowkeyRequest parseFrom = request.parseFrom(info);
                                BlockingRpcCallback<EventsRowkeyProtos.RowkeyResponse> rpcCallback =
                                        new BlockingRpcCallback<EventsRowkeyProtos.RowkeyResponse>();
                                instance.getRowkeys(controller, parseFrom, rpcCallback);
                                EventsRowkeyProtos.RowkeyResponse response = rpcCallback.get();
                                if (controller.failedOnException()) {
                                    throw controller.getFailedOn();
                                }

                                return response.getRowkeysList();
                            }

                        }
                );
            } catch (Throwable t) {
                logger.error(t.getMessage(), t);
            }

            List<ByteString> l = results.values().iterator().next();

            for (ByteString s : l)
                System.out.println(s.toStringUtf8());


//            for(ByteString s: l)
//            System.out.println(new String(s));
            //assertEquals(1,results.size());
            //  Iterator<String> iter = results.values().iterator();
            //String val = iter.next();
        }


    }
}
