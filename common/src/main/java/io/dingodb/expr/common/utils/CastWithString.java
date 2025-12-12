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

package io.dingodb.expr.common.utils;

import org.checkerframework.checker.nullness.qual.NonNull;

import java.math.BigDecimal;

public class CastWithString {

    public static double doubleCastWithStringCompat(String value) {
        double result = 0;
        try {
            //For efficiency.
            result = Double.parseDouble(value);
        } catch (NumberFormatException e) {
            int lastNumberPos = 0;
            if (value != null) {
                //find the last digit position.
                String v = trimDigitString(value);
                try {
                    result = Double.parseDouble(v);
                } catch (NumberFormatException e1) {
                    result = 0;
                }
            }
        }
        return result;
    }

    public static float floatCastWithStringCompat(@NonNull String value) {
        float result = 0;
        String val = value.trim();
        try {
            result = Float.parseFloat(val);
        } catch (NumberFormatException e) {
            int lastNumberPos = 0;
            if (value != null) {
                //find the last digit position.
                String v = trimDigitString(value);
                try {
                    result = Float.parseFloat(v);
                } catch (NumberFormatException e1) {
                    result = 0;
                }
            }
        }

        return result;
    }

    public static @NonNull BigDecimal decimalCastWithStringCompat(@NonNull String value) {
        BigDecimal result = null;
        String val = value.trim();
        try {
            result = new BigDecimal(val);
        } catch (NumberFormatException e) {
            int lastNumberPos = 0;
            if (value != null) {
                //find the last digit position.
                String v = trimDigitString(value);
                try {
                    result = new BigDecimal(v);
                } catch (NumberFormatException e1) {
                    result = new BigDecimal(0);
                }
            }
        }

        return result;
    }

    public static int intCastWithStringCompat(@NonNull String value) {
        int result = 0;
        String val = value.trim();
        try {
            result = Integer.parseInt(val);
        } catch (NumberFormatException e) {
            int lastNumberPos = 0;
            if (value != null) {
                String v = trimDigitString(value);
                try {
                    result = Integer.parseInt(value.trim());
                } catch (NumberFormatException e1) {
                    result = 0;
                }
            }
        }

        return result;
    }

    public static long longCastWithStringCompat(@NonNull String value) {
        long result = 0;
        String val = value.trim();
        try {
            result = Long.parseLong(val);
        } catch (NumberFormatException e) {
            int lastNumberPos = 0;
            if (value != null) {
                //find the last digit position.
                String v = trimDigitString(value);
                try {
                    result = Long.parseLong(v);
                } catch (NumberFormatException e1) {
                    result = 0;
                }
            }
        }

        return result;
    }

    public static String trimDigitString(String value) {
        int lastNumberPos = 0;
        int ePos = -1;

        value = value.trim();
        for (int i = 0; i < value.length(); i++) {
            if (Character.isDigit(value.charAt(i))) {
                lastNumberPos++;
            } else if (value.charAt(i) == '.'
                       || value.charAt(i) == 'e'
                       || value.charAt(i) == 'E') {
                if (ePos != -1) {
                    break;
                } else {
                    ePos = i;
                }
                lastNumberPos++;
            } else {
                break;
            }
        }

        if (lastNumberPos > 0 && lastNumberPos == ePos + 1) {
            lastNumberPos--;
        }
        return value.substring(0, lastNumberPos);
    }
}
