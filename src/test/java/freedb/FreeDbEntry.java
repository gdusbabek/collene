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

import java.io.Serializable;
import java.util.Hashtable;
import java.util.StringTokenizer;

/**
 * Represents an xmcd entry as it comes out of the freedb flat file.
 */
public class FreeDbEntry implements Serializable {
    private int discLength = 0;
    private int revision = 0;
    private String submitted = "";
    private Hashtable<Integer, String> titles = new Hashtable<Integer, String>();
    private Hashtable<Integer, String> extt = new Hashtable<Integer, String>();
    private String title = "";
    private String year = "";
    private String extd = "";
    private String genre = "";
    private String playOrder = "";
    private String discId = "";

    private String album = "";
    private String artist = "";

    private final String archivePath;
    private transient String encoding = "";
    private transient boolean manyUnicode = false;
    private transient boolean manyQ = false;
    
    public String toString() {
        return String.format("%s %s %s", artist, album, title);
    }

    // constructor.
    FreeDbEntry(String path) {
        archivePath = path;
    }

    // count the discids
    public int hasMultipleDiscIds() {
        if (discId.length() == 0)
            return 0;
        else if (discId.indexOf(",") == -1)
            return 1;
        else {
            StringTokenizer toker = new StringTokenizer(discId, ",");
            return toker.countTokens();
        }
    }

    // true if a lot of unicode characters were detected.
    public boolean getManyUnicode() { return manyUnicode; }

    // true if a lot of question marks were found.
    public boolean getManyQ() { return manyQ; }

    // set the guessed encoding.
    public void setEncoding(String s) { encoding = s; }

    // get the guessed encoding.
    public String getEncoding() { return encoding; }

    // get an explanation of the quality.
    public String qualityExplanation() {
        return qualityExplanation;
    }

    private transient String qualityExplanation = "";

    // determine the quality of this entry based on a set of coded heuristics.
    private int quality = Integer.MIN_VALUE;
    public int quality() {
        if (quality != Integer.MIN_VALUE) return quality;

        qualityExplanation = "";
        int q = 100; // can only go down from there.

        if (discLength == 0) {
            q -= 5;
            qualityExplanation += " discLength==0,";
        }
        if (playOrder.equals("")) {
            q -= 2;
            qualityExplanation += " no playOrder,";
        }
        if (submitted.equals("")) {
            q -= 3;
            qualityExplanation += " no submitted,";
        }
        if (hasMultipleDiscIds() > 1) {
            q -= 5;
            qualityExplanation += " has multiple discids,";
        }

        // not trustworthy; big offenses
        
        if (discId.equals("")) {
            q -= 25;
            qualityExplanation += " no discid,";
        }
        if (year.equals("")) {
            q -= 25;
            qualityExplanation += " no year,";
        }
        if (genre.equals("")) {
            q -= 25;
            qualityExplanation += " no genre,";
        }
        if (title.equals("")) {
            q -= 50;
            qualityExplanation += " no title,";
        }

        char[] s = title.toCharArray();
        int qCount = qCount(s);
        if (qCount > 0 && (s.length / qCount) < 3) {
            manyQ = true;
            q -= 30;
            qualityExplanation += " many question marks,";
        }

        int hiCount = hiCount(s);
        if (hiCount > 0 && (s.length / hiCount) < 3) {
            q =- 25;
            manyUnicode = true;
            qualityExplanation += " too much unicode,";
        }

        quality = q;
        return q;
    }

    // count the question marks.
    private static int qCount(char[] s) {
        int count = 0;
        for (char c : s)
            count += c == '?' ? 1 : 0;
        return count;
    }

    // count the high order characters.
    private static int hiCount(char[] s) {
        int count = 0;
        for (char c : s)
            count += ((short)c) > 128 ? 1 : 0;
        return count;
    }

    // set track.
    void setTrack(int index, String value) {
        titles.put(index, value);
    }

    // set extt info
    void setExtt(int index, String value) {
        extt.put(index, value);
    }

    // get track count
    public int getTrackCount() {
        return titles.size();
    }

    // get extt count.
    public int getExttCount() {
        return extt.size();
    }

    // get track.
    public String getTrack(int i) {
        return titles.get(i);
    }

    // get extt
    public String getExtt(int i) {
        return extt.get(i);
    }

    //
    // Geneated slop.
    //

    public int getDiscLength() {
        return discLength;
    }

    void setDiscLength(int discLength) {
        this.discLength = discLength;
    }

    public int getRevision() {
        return revision;
    }

    void setRevision(int revision) {
        this.revision = revision;
    }

    public String getSubmitted() {
        return submitted;
    }

    void setSubmitted(String submitted) {
        this.submitted = submitted == null ? "" : submitted;
    }

    public String getTitle() {
        return title;
    }

    void setTitle(String title) {
        this.title = title == null ? "" : title;
    }

    public String getYear() {
        return year;
    }

    void setYear(String year) {
        this.year = year == null ? "" : year;
    }

    public String getExtd() {
        return extd;
    }

    void setExtd(String extd) {
        this.extd = extd == null ? "" : extd;
    }

    public String getGenre() {
        return genre;
    }

    void setGenre(String genre) {
        this.genre = genre == null ? "" : genre;
    }

    public String getPlayOrder() {
        return playOrder;
    }

    void setPlayOrder(String playOrder) {
        this.playOrder = playOrder == null ? "" : playOrder;
    }

    public String getDiscId() {
        return discId;
    }

    void setDiscId(String discId) {
        this.discId = discId == null ? "" : discId;
    }

    public String getAlbum() {
        return album;
    }

    void setAlbum(String album) {
        this.album = album;
    }

    public String getArtist() {
        return artist;
    }

    void setArtist(String artist) {
        this.artist = artist;
    }

    public String getArchivePath() { return archivePath; }
}
