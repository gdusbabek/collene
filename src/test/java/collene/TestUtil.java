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

import java.io.File;
import java.math.BigInteger;
import java.util.Random;

public class TestUtil {
    private static final Random random = new Random(System.nanoTime());
    private static char[] chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789".toCharArray();
            
    public static File getRandomTempDir() {
        String path = System.getProperty("java.io.tmpdir");
        String rand = "__collene_test_" + new BigInteger(128, random).toString(16);
        File f = new File(new File(path), rand);
        if (!f.mkdirs())
            throw new RuntimeException("Could not make temp dir");
        return f;
    }
    
    public static void removeDirOnExit(File f) {
        if (f.isDirectory()) {
            for (File ch : f.listFiles()) {
                removeDirOnExit(ch);
            }
        }
        f.deleteOnExit();
    }
    
    public static void removeDir(File f) {
        if (f.isDirectory()) {
            for (File ch : f.listFiles()) {
                removeDir(ch);
            }
        }
        f.delete();
    }
    
    public static String randomString(int length) {
        char[] ch = new char[length];
        for (int i = 0; i < length; i++) {
            ch[i] = chars[random.nextInt(chars.length)];
        }
        return new String(ch);
    }
}
