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

package com.amazonaws.services.kinesisanalytics.sources;

import com.amazonaws.services.kinesisanalytics.payloads.APIToken;
import com.github.javafaker.Faker;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MockTokenSource implements SourceFunction<APIToken> {
    private static final Logger LOG = LoggerFactory.getLogger(MockTokenSource.class);

    private static final Long WATERMARK_GEN_PERIOD = 35L;
    private static final Long SLEEP_PERIOD = 2L;
    private static final Long DEFAULT_TOKEN_DURATION = 10000L;

    private transient Faker _faker;

    public MockTokenSource() {
        initIfNecessary();
    }

    @Override
    public void run(SourceFunction.SourceContext<APIToken> sourceContext) throws Exception {
        Long lastTokenGenTime = System.currentTimeMillis();
        APIToken apiToken = getNextToken();

        while(true) {
            Long currTime = System.currentTimeMillis();

            if((currTime - lastTokenGenTime) > 10000L) {
                apiToken = getNextToken();
                lastTokenGenTime = System.currentTimeMillis();
            }

            sourceContext.collectWithTimestamp(apiToken, apiToken.getTokenissuetimestamp());

            // for demo only
            if(currTime % WATERMARK_GEN_PERIOD == 0) {
                sourceContext.emitWatermark(new Watermark(currTime));
            }

            Thread.sleep(SLEEP_PERIOD);
        }
    }

    @Override
    public void cancel() {
    }

    private void initIfNecessary() {
        if(_faker == null) _faker = new Faker();
    }

    /// Call API here
    private APIToken getNextToken() {
        initIfNecessary();

        APIToken token = new APIToken();

        token.setTokenissuetimestamp(System.currentTimeMillis());
        token.setTokenduration(DEFAULT_TOKEN_DURATION);
        token.setTokenvalue(_faker.idNumber().valid());

        return token;
    }
}