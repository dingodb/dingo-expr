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

package io.dingodb.expr.common.timezone.core;

import lombok.extern.slf4j.Slf4j;

import java.time.ZoneId;
import java.util.TimeZone;

@Slf4j
public class SimpleTimeZoneConfig {

    private static ZoneId applicationZone = ZoneId.systemDefault();
    private static ZoneId storageZone = ZoneId.of("UTC");

    private SimpleTimeZoneConfig() {
    }

    public static ZoneId getApplicationZone() {
        return applicationZone;
    }

    public static void setApplicationZone(ZoneId zone) {
        applicationZone = zone;
    }

    public static ZoneId getStorageZone() {
        return storageZone;
    }

    public static void setStorageZone(ZoneId zone) {
        storageZone = zone;
    }

    public static TimeZone getApplicationTimeZone() {
        return TimeZone.getTimeZone(applicationZone);
    }

    public static void initializeFromSystem() {
        // TODO
        String appZone = System.getProperty("app.timezone",
            System.getenv().getOrDefault("APP_TIMEZONE", ""));
        String storageZone = System.getenv().getOrDefault("DB_TIMEZONE",
            System.getProperty("db.timezone", "UTC"));

        if (!appZone.isEmpty()) {
            try {
                setApplicationZone(ZoneId.of(appZone));
            } catch (Exception e) {
                log.error("Invalid application timezone: {}, using default: {}", appZone, applicationZone);
            }
        }

        try {
            setStorageZone(ZoneId.of(storageZone));
        } catch (Exception e) {
            log.error("Invalid storage timezone: {}, using default: UTC", storageZone);
            setStorageZone(ZoneId.of("UTC"));
        }
    }

    public static void resetToDefaults() {
        applicationZone = ZoneId.systemDefault();
        storageZone = ZoneId.of("UTC");
    }
}
