/*
 * Copyright 2014 Gary Dusbabek
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

package collene;

public class Utils {
    
    public static long bytesToLong(byte[] buf) {
        return (((long)buf[0] << 56) +
                ((long)(buf[1] & 255) << 48) +
                ((long)(buf[2] & 255) << 40) +
                ((long)(buf[3] & 255) << 32) +
                ((long)(buf[4] & 255) << 24) +
                ((buf[5] & 255) << 16) + 
                ((buf[6] & 255) << 8) +
                ((buf[7] & 255) << 0));
    }
    
    public static byte[] longToBytes(long l) {
        byte[] buf = new byte[8];
        buf[0] = (byte)(l >>> 56);
        buf[1] = (byte)(l >>> 48);
        buf[2] = (byte)(l >>> 40);
        buf[3] = (byte)(l >>> 32);
        buf[4] = (byte)(l >>> 24);
        buf[5] = (byte)(l >>> 16);
        buf[6] = (byte)(l >>> 8);
        buf[7] = (byte)(l >>> 0);
        return buf;
    }
}
