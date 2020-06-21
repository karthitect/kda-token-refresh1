package com.amazonaws.services.kinesisanalytics;

import com.amazonaws.services.kinesisanalytics.payloads.APIToken;
import com.amazonaws.services.kinesisanalytics.payloads.EmployeeInfo;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SampleStatefulOperator extends KeyedBroadcastProcessFunction<Long, EmployeeInfo, APIToken, Tuple2<Long, Long>> {

    private static final Logger LOG = LoggerFactory.getLogger(SampleStatefulOperator.class);

    private transient ValueState<Long> employeeCountDesc;

    // broadcast state descriptor
    MapStateDescriptor<Void, APIToken> apiTokenDesc;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        ValueStateDescriptor<Long> descriptor =
                new ValueStateDescriptor<>("employeeCount",
                        Long.class);
        employeeCountDesc = getRuntimeContext().getState(descriptor);

        apiTokenDesc =
                new MapStateDescriptor<>("apitokens", Types.VOID, Types.POJO(APIToken.class));
    }

    @Override
    public void processElement(EmployeeInfo ei,
                               ReadOnlyContext readOnlyContext,
                               Collector<Tuple2<Long, Long>> collector) throws Exception {
        // get current token from broadcast state
        APIToken token = readOnlyContext
                .getBroadcastState(this.apiTokenDesc)
                // access MapState with null as VOID default value
                .get(null);

        if(token == null) {
            LOG.warn("Token hasn't been set!");

            // TODO:
            // It is possible that we'll get incoming data that may have to
            // be buffered until we receive our first token
        } else {
            LOG.info(String.format("Got token %s for (%d, %d)",
                    token.getTokenvalue().toString(), ei.getCompanyid(), ei.getEmployeeid()));
        }

        // update employee count
        if(employeeCountDesc.value() == null) {
            employeeCountDesc.update(0L);
        }

        employeeCountDesc.update(employeeCountDesc.value() + 1L);
    }

    @Override
    public void processBroadcastElement(APIToken apiToken,
                                        Context context,
                                        Collector<Tuple2<Long, Long>> collector) throws Exception {
        // store the new token by updating the broadcast state
        BroadcastState<Void, APIToken> bcState = context.getBroadcastState(this.apiTokenDesc);
        // storing in MapState with null as VOID default value
        bcState.put(null, apiToken);
    }
}
