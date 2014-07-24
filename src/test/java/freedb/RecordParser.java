package freedb;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Hashtable;

/**
 * 
 */
class RecordParser {
    private static final int CR = 0x0000000d;
    private static final int LF = 0x0000000a;

    public FreeDbEntry parse(TarRecord tr) throws FormatException {
        
        byte[] raw = tr.data;
        String charset = null;
        try {
            charset = FormatValidator.validate(raw,"UTF-8","ISO-8859-1","US-ASCII");
        } catch (FormatException ex) {
            ex.setArchivePath(tr.header.getName());
            throw ex;
        }

        // chop raw bytes into a set of lines.
        int[] markers = markLines(raw);
        //System.out.println("Found " + markers.length + " lines in " + tr.header.getName());
        int pos = 0;
        String[] lines = new String[markers.length];
        for (int i = 0; i < markers.length; i++) {
            try {
                lines[i] = new String(raw, pos, markers[i] - pos, charset);
            } catch (UnsupportedEncodingException ex) {
                ex.printStackTrace();
            }
            pos = markers[i] + 1;
        }

        int discLength = 0;
        int revision = 0;
        String submitted = "";
        int startDiscDataLine = 0;

        // figure out where things are.
        for (int i = 0; i < lines.length; i++) {
            if (lines[i].startsWith("# Disc length"))
                discLength = extractIntAfterColon(lines[i]);
            else if (lines[i].startsWith("# Revision"))
                revision = extractIntAfterColon(lines[i]);
            else if (lines[i].startsWith("# Submitted via"))
                submitted = extractStringAfterColon(lines[i]);
            else if (!lines[i].startsWith("#") && startDiscDataLine == 0) {
                startDiscDataLine = i;
                i = lines.length;
            }
        }

        // get all the data lines.
        Hashtable<String, String> pairs = new Hashtable<String, String>();
        for (int i = startDiscDataLine; i < lines.length; i++) {
            int eq = lines[i].indexOf("=");
            if (eq < 0) continue;
            String key = lines[i].substring(0, eq);
            String value = lines[i].substring(eq + 1).trim();
            // put in pairs. If key is already there, append the value.
            String oldValue = pairs.get(key);
            if (oldValue == null)
                pairs.put(key, value);
            else {
                pairs.remove(key);
                pairs.put(key, oldValue + value);
            }
        }

        // create the entry.
        FreeDbEntry entry = new FreeDbEntry(tr.header.getName());
        entry.setDiscId(pairs.get("DISCID"));
        entry.setTitle(pairs.get("DTITLE"));
        entry.setYear(pairs.get("DYEAR"));
        entry.setGenre(pairs.get("DGENRE"));
        entry.setExtd(pairs.get("EXTD"));
        entry.setPlayOrder(pairs.get("PLAYORDER"));

        // split the title.
        String title = entry.getTitle();
        int delim = title.indexOf(" / ");
        if (delim < 0) {
            entry.setAlbum(title);
            entry.setArtist(title);
        } else {
            entry.setArtist(title.substring(0, delim).trim());
            entry.setAlbum(title.substring(delim + 3).trim());
        }

        // tracks.
        int t = 0;
        while (t > -1) {
            String track = pairs.get("TTITLE" + t);
            if (track != null)
                entry.setTrack(t++, track);
            else t = -1;
        }

        // track exts.
        t = 0;
        while (t > -1) {
            String ext = pairs.get("EXTT" + t);
            if (ext != null)
                entry.setExtt(t++, ext);
            else t = -1;
        }

        entry.setEncoding(charset);

        return entry;
    }

    private static int extractIntAfterColon(String s) {
        String orig = s;
        s = extractStringAfterColon(s);
        int end = s.indexOf(" ");
        s = s.substring(0, end < 0 ? s.length() : end);
        int i = 0;
        try {
            i = Integer.parseInt(s);
        } catch (Throwable th) {
            System.err.println("Problem extracing int from: " + orig);
        }
        return i;
    }

    private static String extractStringAfterColon(String s) {
        String i = "";
        s = s.substring(s.indexOf(":") + 1).trim();
        return s;
    }

    /*
      Database entries must be in the US-ASCII, ISO-8859-1 (the 8-bit ASCII
 extension also known as "Latin alphabet #1" or ISO-Latin-1) or UTF-8 (Unicode)
 character set. Lines must always be terminated by a newline/linefeed
 (ctrl-J, or 0Ah) character or a carriage return character (ctrl-M, or 0Dh)
 followed by a newline/linefeed character. All lines in a database entry must
 be less than or equal to 256 characters in length, including the terminating
 character(s). Database entries with lines that are longer will be considered
 invalid. There must be no blank lines in a database entry.
      */
    private int[] markLines(byte[] raw) {
        // hold each line in an array list until the end.
        ArrayList<Integer> markers = new ArrayList<Integer>();
        int pos = 0;
        int start = 0;
        while (pos < raw.length) {
            // look for LF or CRLF
            if (raw[pos] == LF)
                markers.add(pos);
            pos++;
        }
        int[] i = new int[markers.size()];
        for (int j = 0; j < i.length; j++) i[j] = markers.get(j);
        return i;
    }
}
