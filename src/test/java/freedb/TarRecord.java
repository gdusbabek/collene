package freedb;

/**
 * Simple TAR record.
 */
class TarRecord {
    final TarHeader header;
    final byte[] data;

    TarRecord(TarHeader header, byte[] data) {
        this.header = header;
        this.data = data;
    }
}
