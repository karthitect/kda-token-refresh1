/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"). You may
 * not use this file except in compliance with the License. A copy of the
 * License is located at
 *
 *    http://aws.amazon.com/apache2.0/
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazonaws.services.kinesisanalytics;

import com.amazonaws.services.kinesisanalytics.payloads.APIToken;
import com.amazonaws.services.kinesisanalytics.payloads.EmployeeInfo;
import com.amazonaws.services.kinesisanalytics.sources.MockTokenSource;
import com.amazonaws.services.kinesisanalytics.sources.RandEmployeeInfoSource;
import com.amazonaws.services.kinesisanalytics.utils.ParameterToolUtils;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class StreamingJob {

	public static void main(String[] args) throws Exception {
		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		ParameterTool parameter = ParameterToolUtils.fromArgsAndApplicationProperties(args);

		runTokenStream(args, env, parameter);
	}

	private static void runTokenStream(String[] args,
									   StreamExecutionEnvironment env,
									   ParameterTool parameter) throws Exception {

		// token source (sparse; meant for broadcast)
		DataStream<APIToken> apiTokens = env.addSource(new MockTokenSource());

		// data source (primary stream)
		KeyedStream<EmployeeInfo, Tuple> empStreamByCompany = env.addSource(new RandEmployeeInfoSource())
				.keyBy("companyid");

		MapStateDescriptor<Void, APIToken> bcStateDescriptor =
				new MapStateDescriptor<>("apitokens", Types.VOID, Types.POJO(APIToken.class));

		BroadcastStream<APIToken> broadcastTokens = apiTokens.broadcast(bcStateDescriptor);

		DataStream<Tuple2<Long, Long>> resultStream = empStreamByCompany
				.connect(broadcastTokens)
				.process(new SampleStatefulOperator());

		env.execute("Flink Streaming API Token Test");
	}
}
