/*
 * Copyright 2017 Stefan Zobel
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
package net.topaz.grpc;

import java.security.SecureRandom;

/**
 * A 128 bit {@code UUID}.
 */
public final class UUID128 {

    private static final SecureRandom ng = new SecureRandom();

    /**
     * Equivalent to <blockquote>
     * 
     * <pre>
     * String id = UUID.randomUUID().toString().replace("-", "");
     * </pre>
     * 
     * </blockquote> but with 128 bits of randomness instead of 122 bits and only
     * marginally slower (on Java 17) than the {@code java.util.UUID} equivalent.
     * 
     * @return a 128 bit random UUID string
     */
    public static String random128BitHex() {
        byte[] rnd = new byte[16];
        ng.nextBytes(rnd);
        long msb = 0;
        long lsb = 0;
        for (int i = 0; i < 8; i++) {
            msb = (msb << 8) | (rnd[i] & 0xff);
        }
        for (int i = 8; i < 16; i++) {
            lsb = (lsb << 8) | (rnd[i] & 0xff);
        }
        StringBuilder sb = new StringBuilder(32);
        sb.append(digits(msb >> 32, 8));
        sb.append(digits(msb >> 16, 4));
        sb.append(digits(msb, 4));
        sb.append(digits(lsb >> 48, 4));
        sb.append(digits(lsb, 12));
        return sb.toString();
    }

    /** Returns val represented by the specified number of hex digits. */
    private static String digits(long val, int digits) {
        long hi = 1L << (digits * 4);
        return Long.toHexString(hi | (val & (hi - 1))).substring(1);
    }

    private UUID128() {
        throw new AssertionError();
    }
}
