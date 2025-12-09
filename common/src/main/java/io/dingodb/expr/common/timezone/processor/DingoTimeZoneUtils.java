/*
 * Copyright 2021 DataCanvas
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.dingodb.expr.common.timezone.processor;

import io.dingodb.expr.common.timezone.core.DateTimeType;
import io.dingodb.expr.common.timezone.core.DingoDateTime;
import io.dingodb.expr.common.timezone.core.SimpleTimeZoneConfig;

import java.time.ZoneId;

public class DingoTimeZoneUtils {

    private static volatile DingoTimeZoneProcessor defaultProcessor;

    private DingoTimeZoneUtils() {
    }

    public static void initialize() {
        SimpleTimeZoneConfig.initializeFromSystem();
        getDefaultProcessor();
    }

    public static DingoTimeZoneProcessor getDefaultProcessor() {
        if (defaultProcessor == null) {
            synchronized (DingoTimeZoneUtils.class) {
                if (defaultProcessor == null) {
                    defaultProcessor = new DingoTimeZoneProcessor();
                }
            }
        }
        return defaultProcessor;
    }

    public static void setDefaultProcessor(DingoTimeZoneProcessor processor) {
        synchronized (DingoTimeZoneUtils.class) {
            defaultProcessor = processor;
        }
    }

    public static Object processInput(Object input, DateTimeType targetType) {
        DingoTimeZoneProcessor processor = getDefaultProcessor();
        SmartInputProcessor inputProcessor = new SmartInputProcessor(
            processor.getInputZone());
        return inputProcessor.processInput(input, targetType);
    }

    public static Object convertOutput(DingoDateTime internal, DateTimeType outputType) {
        DingoTimeZoneProcessor processor = getDefaultProcessor();
        return processor.processDateTime(internal, internal.getType(), outputType);
    }

    public static Object process(Object input, DateTimeType outputType) {
        DingoTimeZoneProcessor processor = getDefaultProcessor();
        return processor.processDateTime(input, outputType);
    }

    public static String safeToString(Object dateTimeValue) {
        DingoTimeZoneProcessor processor = getDefaultProcessor();
        return processor.toSafeString(dateTimeValue);
    }

    public static void setApplicationZone(ZoneId zone) {
        SimpleTimeZoneConfig.setApplicationZone(zone);
        synchronized (DingoTimeZoneUtils.class) {
            defaultProcessor = new DingoTimeZoneProcessor();
        }
    }

    public static void setStorageZone(ZoneId zone) {
        SimpleTimeZoneConfig.setStorageZone(zone);
        synchronized (DingoTimeZoneUtils.class) {
            defaultProcessor = new DingoTimeZoneProcessor();
        }
    }
}
