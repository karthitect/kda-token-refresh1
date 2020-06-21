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

import com.amazonaws.services.kinesisanalytics.payloads.EmployeeInfo;
import com.github.javafaker.Faker;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

import java.text.SimpleDateFormat;
import java.util.concurrent.ThreadLocalRandom;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RandEmployeeInfoSource implements SourceFunction<EmployeeInfo> {

    private static final Logger LOG = LoggerFactory.getLogger(RandEmployeeInfoSource.class);

    private static final Long WATERMARK_GEN_PERIOD = 35L;
    private static final Long SLEEP_PERIOD = 2L;

    private transient Faker _faker;
    private transient SimpleDateFormat _sdfr;

    public RandEmployeeInfoSource() {
        initIfNecessary();
    }

    @Override
    public void run(SourceContext<EmployeeInfo> sourceContext) throws Exception {
        while(true) {
            EmployeeInfo ei = getNextEmployee();

            sourceContext.collectWithTimestamp(ei, ei.getEventtimestamp());

            if(ei.getEventtimestamp() % WATERMARK_GEN_PERIOD == 0) {
                sourceContext.emitWatermark(new Watermark(ei.getEventtimestamp()));
            }

            Thread.sleep(SLEEP_PERIOD);
        }
    }

    @Override
    public void cancel() {
    }

    private void initIfNecessary() {
        if(_faker == null) _faker = new Faker();
        if(_sdfr == null) _sdfr = new SimpleDateFormat("yyyy-MM-dd");
    }

    private EmployeeInfo getNextEmployee() {
        initIfNecessary();

        EmployeeInfo ei = new EmployeeInfo();

        ei.setCompanyid(ThreadLocalRandom.current().nextLong(5));
        ei.setEmployeeid(ThreadLocalRandom.current().nextLong(50));
        ei.setMname(_faker.name().fullName());
        ei.setDob(_sdfr.format(_faker.date().birthday()));
        ei.setEventtimestamp(System.currentTimeMillis());

        return ei;
    }
}