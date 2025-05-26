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

package io.dingodb.expr.runtime.utils;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoField;
import java.time.temporal.WeekFields;

public final class TimestampUtils {

    private TimestampUtils() {
    }

    public static int extractQuarter(Timestamp value) {
        return (extractMonth(value) - 1) / 3 + 1;
    }

    /**
     * Extraction Year.
     *
     * @param value  the Timestamp value
     * @return       year
     */
    public static int extractYear(Timestamp value) {
        return toZonedDateTime(value).getYear();
    }

    /**
     * Extract month (1-12).
     *
     * @param value   the Timestamp value
     * @return        month (1-12)
     */
    public static int extractMonth(Timestamp value) {
        return toZonedDateTime(value).getMonthValue();
    }

    /**
     * Withdrawal day (1-31).
     *
     * @param value   the Timestamp value
     * @return        day
     */
    public static int extractDay(Timestamp value) {
        return toZonedDateTime(value).getDayOfMonth();
    }

    /**
     * Extract the week number of the year (ISO standard).
     *
     * @param value  the Timestamp value
     * @return       week
     */
    public static int extractWeek(Timestamp value) {
        return toZonedDateTime(value).get(WeekFields.ISO.weekOfYear());
    }

    /**
     * Extraction hours（0-23）.
     *
     * @param value    the Timestamp value
     * @return         hour
     */
    public static int extractHour(Timestamp value) {
        return toZonedDateTime(value).getHour();
    }

    /**
     * Extraction minutes（0-59）.
     *
     * @param value  the Timestamp value
     * @return       minutes
     */
    public static int extractMinute(Timestamp value) {
        return toZonedDateTime(value).getMinute();
    }

    /**
     * Extract seconds（0-59）.
     *
     * @param value   the Timestamp value
     * @return        seconds
     */
    public static int extractSecond(Timestamp value) {
        return toZonedDateTime(value).getSecond();
    }

    /**
     * Extract milliseconds（0-999）.
     *
     * @param value    the Timestamp value
     * @return         milliseconds
     */
    public static int extractMillisecond(Timestamp value) {
        return toZonedDateTime(value).get(ChronoField.MILLI_OF_SECOND);
    }

    /**
     * Extract the hour of the day（0-23）.
     *
     * @param value   the Timestamp value
     * @return        the hour of the day
     */
    public static int extractDayHour(Timestamp value) {
        return toZonedDateTime(value).getHour();
    }

    /**
     * Extract the minute of the day（0-1439）.
     *
     * @param value    the Timestamp value
     * @return         the minute of the day
     */
    public static int extractDayMinute(Timestamp value) {
        ZonedDateTime zdt = toZonedDateTime(value);
        return zdt.getHour() * 60 + zdt.getMinute();
    }

    /**
     * Extract the seconds of the day（0-86399）.
     *
     * @param value    the Timestamp value
     * @return         the seconds of the day
     */
    public static int extractDaySecond(Timestamp value) {
        return toZonedDateTime(value).toLocalTime().toSecondOfDay();
    }

    /**
     * Extract hour and minute combination（HHMM）.
     *
     * @param value    the Timestamp value
     * @return         hour and minute combination
     */
    public static int extractHourMinute(Timestamp value) {
        ZonedDateTime zdt = toZonedDateTime(value);
        return zdt.getHour() * 100 + zdt.getMinute();
    }

    /**
     * Extract the seconds within the hour（0-3599）.
     *
     * @param value     the Timestamp value
     * @return          the seconds within the hour
     */
    public static int extractHourSecond(Timestamp value) {
        ZonedDateTime zdt = toZonedDateTime(value);
        return zdt.getMinute() * 60 + zdt.getSecond();
    }

    /**
     * Extract minutes and seconds（MMSS）.
     *
     * @param value    the Timestamp value
     * @return         minutes and seconds
     */
    public static int extractMinuteSecond(Timestamp value) {
        ZonedDateTime zdt = toZonedDateTime(value);
        return zdt.getMinute() * 100 + zdt.getSecond();
    }

    private static ZonedDateTime toZonedDateTime(Timestamp value) {
        return value.toInstant().atZone(ZoneId.of("UTC"));
    }


}
