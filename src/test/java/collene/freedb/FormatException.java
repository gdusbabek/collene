package collene.freedb;

/**
 * Thrown when a data format problem is encountered.
 */
class FormatException extends Exception {

    private String archivePath = "";

    // constructor.
    FormatException(String msg) {
        super(msg);
    }

    // so we know which entry caused the problem.
    void setArchivePath(String s) { archivePath = s; }

    // get the entry that caused the problem.
    String getArchivePath() { return archivePath; }
}
