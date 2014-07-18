package collene;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class CassandraIO implements IO {
    private final int columnSize;
    private final String keyspace;
    private final String index;
    
    private Cluster cluster;
    private Session session = null;
    
    // todo: hack until I figure out how to address this.
    private Set<String> allKeys = new HashSet<String>();
    
    public CassandraIO(int columnSize, String keyspace, String index) {
        this.columnSize = columnSize;
        this.keyspace = keyspace;
        this.index = index;
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
        cluster.close();
        session = null;
    }
    
    @Override
    public void put(String key, long col, byte[] value) throws IOException {
        allKeys.add(key);
        ensureSession();
        PreparedStatement stmt = session.prepare(String.format("insert into %s.%s (key, name, value) values(?, ?, ?);", keyspace, index));
        BoundStatement bndStmt = new BoundStatement(stmt.setConsistencyLevel(ConsistencyLevel.ONE));
        session.execute(bndStmt.bind(key, col, ByteBuffer.wrap(value)));
        session.close();
    }

    @Override
    public byte[] get(String key, long col) throws IOException {
        ensureSession();
        PreparedStatement stmt = session.prepare(String.format("select value from %s.%s where key = ? and name = ?", keyspace, index));
        BoundStatement bndStmt = new BoundStatement(stmt.setConsistencyLevel(ConsistencyLevel.ONE));
        ResultSet rs = session.execute(bndStmt.bind(key, col));
        Row row = rs.one();
        if (row == null) {
            session.close();
            return null;
        }
        ByteBuffer bb = row.getBytes("value");
        byte[] b = new byte[bb.remaining()];
        bb.get(b);
        session.close();
        return b;
    }

    @Override
    public int getColSize() {
        return columnSize;
    }

    @Override
    public String[] allKeys() throws IOException {
        return allKeys.toArray(new String[allKeys.size()]);
    }

    @Override
    public void delete(String key) throws IOException {
        allKeys.remove(key);
        ensureSession();
        PreparedStatement stmt = session.prepare(String.format("delete from %s.%s where key = ?", keyspace, index));
        BoundStatement bndStmt = new BoundStatement(stmt.setConsistencyLevel(ConsistencyLevel.ONE));
        session.execute(bndStmt.bind(key));
        session.close();
    }

    @Override
    public boolean hasKey(String key) throws IOException {
        return get(key, 0L) != null;
    }
}
