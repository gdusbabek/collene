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

package freedb;

import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.CharacterCodingException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.util.List;
import java.util.ArrayList;

/**
 * Test a byte array to verify that it can be read in as using a particular character set.
 */
class FormatValidator {
    // blow up if there is a problem.
    static String validate(byte[] data, String... charsets) throws FormatException {
        boolean passed = false;
        List<CharacterCodingException> problems = new ArrayList<CharacterCodingException>(charsets.length);
        List<String> problemEncodings = new ArrayList<String>(charsets.length);
        for (int i = 0; !passed && i < charsets.length; i++) {
            CharsetDecoder dec = Charset.forName(charsets[i]).newDecoder();
            ByteBuffer buf = ByteBuffer.wrap(data);
            try {
                CharBuffer cb = dec.onMalformedInput(CodingErrorAction.REPORT).decode(buf);
                passed = true;
                return charsets[i];
            } catch (CharacterCodingException ex) {
                problems.add(ex);
                problemEncodings.add(charsets[i]);
            }
        }
        // if we got here, we never found a solution.
        StringBuilder err = new StringBuilder();
        for (int i = 0; i < problems.size(); i++) {
            CharacterCodingException ex = problems.get(i);
            String encoding = problemEncodings.get(i);
            err.append(encoding);
            err.append(":");
            err.append(ex.getMessage());
            err.append(",");
        }
        throw new FormatException(err.toString());
    }
}
