package collene;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.google.common.base.Charsets;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class CassandraIO implements IO {
    // special row key used to store all keys. todo: obvious consistency problems. I think we mostly get around this in
    // lucene by knowing that a particular Directory instnace only operates on a subset of the keys.
    private static final String KEY_LIST_KEY = "__COLLLENE_KEY_LIST_KEY__";
    private final int columnSize;
    private final String keyspace;
    private final String index;
    private final String rowPrefix;
    private final String prefixedKeyListKey;
    
    private Cluster cluster;
    private Session session = null;
    
    // no attempt to make consistent. races are entirely possible. deal with it for now.
    private Set<String> keyCache = new HashSet<>();
    
    private static final ThreadLocal<CharsetDecoder> decoders = new ThreadLocal<CharsetDecoder>() {
        @Override
        protected CharsetDecoder initialValue() {
            return Charsets.UTF_8.newDecoder();
        }
    };
    
    public CassandraIO(String rowPrefix, int columnSize, String keyspace, String index) {
        this.columnSize = columnSize;
        this.keyspace = keyspace;
        this.index = index;
        this.rowPrefix = rowPrefix;
        this.prefixedKeyListKey = prefix(KEY_LIST_KEY);
    }
    
    public CassandraIO start(String addr) {
        try {
            cluster = Cluster.builder()
                    .addContactPointsWithPorts(asSocketAddresses(addr))
//                    .addContactPoint(addr)
                    .build();
            ensureSession();
        } catch (UnknownHostException ex) {
            throw new RuntimeException(ex);
        }
        return this;
    }
    
    private void ensureSession() {
        if (session == null || session.isClosed()) {
            session = cluster.connect(keyspace);
        }
    }
    
    private static Collection<InetSocketAddress> asSocketAddresses(String... hostAndPorts) throws UnknownHostException {
        List<InetSocketAddress> addrs = new ArrayList<InetSocketAddress>(hostAndPorts.length);
        for (String spec : hostAndPorts) {
            String[] parts = spec.split(":", -1);
            addrs.add(new InetSocketAddress(InetAddress.getByName(parts[0]), Integer.parseInt(parts[1])));
        }
        return addrs;
    }
    
    public void close() {
        ensureSession();
        session.close();
        cluster.close();
        session = null;
    }
    
    @Override
    public void put(String key, long col, byte[] value) throws IOException {
        boolean needsKeyRowUpdate = keyCache.add(key);
        ensureSession();
        key = prefix(key);
        BatchStatement batch = new BatchStatement();
        PreparedStatement stmt = session.prepare(String.format("insert into %s.%s (key, name, value) values(?, ?, ?);", keyspace, index));
        BoundStatement bndStmt = new BoundStatement(stmt.setConsistencyLevel(ConsistencyLevel.ONE));
        batch.add(bndStmt.bind(key, col, ByteBuffer.wrap(value)));
        
        if (needsKeyRowUpdate) {
            stmt = session.prepare(String.format("insert into %s.%s (key, name, value) values (?, ?, ?);", keyspace, index));
            bndStmt = new BoundStatement(stmt.setConsistencyLevel(ConsistencyLevel.ONE));
            batch.add(bndStmt.bind(prefixedKeyListKey, (long)key.hashCode(), ByteBuffer.wrap(key.getBytes(Charsets.UTF_8))));
        }
        
        session.execute(batch);
    }

    @Override
    public byte[] get(String key, long col) throws IOException {
        ensureSession();
        key = prefix(key);
        PreparedStatement stmt = session.prepare(String.format("select value from %s.%s where key = ? and name = ?", keyspace, index));
        BoundStatement bndStmt = new BoundStatement(stmt.setConsistencyLevel(ConsistencyLevel.ONE));
        ResultSet rs = session.execute(bndStmt.bind(key, col));
        Row row = rs.one();
        if (row == null) {
            return null;
        }
        ByteBuffer bb = row.getBytes("value");
        byte[] b = new byte[bb.remaining()];
        bb.get(b);
        return b;
    }

    @Override
    public int getColSize() {
        return columnSize;
    }

    @Override
    public String[] allKeys() throws IOException {
        ensureSession();
        PreparedStatement stmt = session.prepare(String.format("select value from %s.%s where key = ?", keyspace, index));
        BoundStatement bndStmt = new BoundStatement(stmt.setConsistencyLevel(ConsistencyLevel.ONE));
        ResultSet rs = session.execute(bndStmt.bind(prefixedKeyListKey));
        Row row = rs.one();
        if (row == null) {
            return new String[]{};
        } else {
            String[] keys = new String[row.getColumnDefinitions().size()];
            for (int i = 0; i < keys.length; i++) {
                keys[i] = toString(row.getBytes(i));
            }
            return keys;
        }
    }
    
    private static String toString(ByteBuffer bb) throws CharacterCodingException {
        CharBuffer cb = decoders.get().decode(bb.duplicate());
        return cb.toString();
    }

    @Override
    public void delete(String key) throws IOException {
        ensureSession();
        key = prefix(key);
        BatchStatement delBatch = new BatchStatement();
        
        PreparedStatement stmt = session.prepare(String.format("delete from %s.%s where key = ?", keyspace, index));
        BoundStatement bndStmt = new BoundStatement(stmt.setConsistencyLevel(ConsistencyLevel.ONE));
        delBatch = delBatch.add(bndStmt.bind(key));
        
        stmt = session.prepare(String.format("delete from %s.%s where key = ? and name = ?", keyspace, index));
        bndStmt = new BoundStatement(stmt.setConsistencyLevel(ConsistencyLevel.ONE));
        delBatch = delBatch.add(bndStmt.bind(prefixedKeyListKey, (long)prefixedKeyListKey.hashCode()));
        
        session.execute(delBatch);
        
        keyCache.remove(key);
    }

    @Override
    public boolean hasKey(String key) throws IOException {
        // todo: this way vs doing allKeys()?
        return get(key, 0L) != null;
    }
    
    private String prefix(String key) {
        return String.format("%s_%s", rowPrefix, key);
    }
}
