/**
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

import com.datastax.driver.core.BatchStatement;
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
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @see collene.IO 
 * 
 * Put and get things in Cassandra. You can use the same column family for multiple indexes by specifying different
 * rowPrefix values in the constructor.
 * 
 * The column family is expected to be compatible with the following CQL:
 * 
 * create table if not exists cindex (
 *   key text,
 *   name bigint,
 *   value blob,
 *   primary key(key, name)
 * )
 * with compact storage;
 */
public class CassandraIO implements IO {
    private final int columnSize;
    private final String keyspace;
    private final String columnFamily;
    private final String rowPrefix;
    
    // keeping the cluster around is not too important. However, it is handy for when a session needs to be recreated.
    private Cluster cluster;
    private Session session = null;

    /**
     * Create an IO instance.
     * @param rowPrefix Prefix added to each key before writing to cassandra. This allows you to use the same column
     *                  family for multiple indexes.
     * @param columnSize byte size for each column.
     * @param keyspace Cassandra keyspace to use
     * @param columnFamily Cassandra column family to use.
     */
    public CassandraIO(String rowPrefix, int columnSize, String keyspace, String columnFamily) {
        this.columnSize = columnSize;
        this.keyspace = keyspace;
        this.columnFamily = columnFamily;
        this.rowPrefix = rowPrefix;
    }

    /**
     * Hijack the session and cluster of an existing IO instance to write to a different index.
     */
    public CassandraIO clone(String newRowPrefix) {
        CassandraIO io = new CassandraIO(newRowPrefix, columnSize, keyspace, columnFamily);
        io.session = this.session;
        io.cluster = this.cluster;
        return io;
    }

    /**
     * connect to a cluster and build a session.
     * @param addr a host:port tuple
     * @return this instance.
     */
    public CassandraIO start(String addr) {
        try {
            cluster = Cluster.builder()
                    .addContactPointsWithPorts(asSocketAddresses(addr))
                    .build();
            ensureSession();
        } catch (UnknownHostException ex) {
            throw new RuntimeException(ex);
        }
        return this;
    }

    /**
     * Replaces the session instance of an existing instance.
     */
    public CassandraIO session(Session session) {
        this.session = session;
        this.cluster = session.getCluster();
        return this;
    }
    
    // ensures the session is active.
    private void ensureSession() {
        if (session == null || session.isClosed()) {
            session = cluster.connect(keyspace);
        }
    }
    
    // convert a bunch of host:port tuples to a collection of socket addresses.
    private static Collection<InetSocketAddress> asSocketAddresses(String... hostAndPorts) throws UnknownHostException {
        List<InetSocketAddress> addrs = new ArrayList<InetSocketAddress>(hostAndPorts.length);
        for (String spec : hostAndPorts) {
            String[] parts = spec.split(":", -1);
            addrs.add(new InetSocketAddress(InetAddress.getByName(parts[0]), Integer.parseInt(parts[1])));
        }
        return addrs;
    }
    
    /** close down */
    public void close() {
        ensureSession();
        session.close();
        cluster.close();
        session = null;
    }
    
    /** @inheritDoc */
    @Override
    public void put(String key, long col, byte[] value) throws IOException {
        ensureSession();
        String prefixedKey = prefix(key);
        BatchStatement batch = new BatchStatement();
        PreparedStatement stmt = session.prepare(String.format("insert into %s.%s (key, name, value) values(?, ?, ?);", keyspace, columnFamily));
        BoundStatement bndStmt = new BoundStatement(stmt.setConsistencyLevel(ConsistencyLevel.ONE));
        batch.add(bndStmt.bind(prefixedKey, col, ByteBuffer.wrap(value)));
        session.execute(batch);
    }

    /** @inheritDoc */
    @Override
    public byte[] get(String key, long col) throws IOException {
        ensureSession();
        String prefixedKey = prefix(key);
        PreparedStatement stmt = session.prepare(String.format("select value from %s.%s where key = ? and name = ?", keyspace, columnFamily));
        BoundStatement bndStmt = new BoundStatement(stmt.setConsistencyLevel(ConsistencyLevel.ONE));
        ResultSet rs = session.execute(bndStmt.bind(prefixedKey, col));
        Row row = rs.one();
        if (row == null) {
            return null;
        }
        ByteBuffer bb = row.getBytes("value");
        byte[] b = new byte[bb.remaining()];
        bb.get(b);
        return b;
    }

    /** @inheritDoc */
    @Override
    public Iterable<byte[]> allValues(String key) throws IOException {
        ensureSession();
        String prefixedKey = prefix(key);
        PreparedStatement stmt = session.prepare(String.format("select value from %s.%s where key = ?", keyspace, columnFamily));
        BoundStatement bndStmt = new BoundStatement(stmt.setConsistencyLevel(ConsistencyLevel.ONE));
        ResultSet rs = session.execute(bndStmt.bind(prefixedKey));
        List<byte[]> values = new ArrayList<byte[]>();
        for (Row row : rs.all()) {
            values.add(toBytes(row.getBytes(0)));
        }
        return values;
    }
    
    // copied from org.apache.cassandra.utils.ByteBufferUtil.getArray(ByteBuffer).  
    /**
     * You should almost never use this.  Instead, use the write* methods to avoid copies.
     */
    private byte[] toBytes(ByteBuffer buffer) {
        int length = buffer.remaining();

        if (buffer.hasArray())
        {
            int boff = buffer.arrayOffset() + buffer.position();
            if (boff == 0 && length == buffer.array().length)
                return buffer.array();
            else
                return Arrays.copyOfRange(buffer.array(), boff, boff + length);
        }
        // else, DirectByteBuffer.get() is the fastest route
        byte[] bytes = new byte[length];
        buffer.duplicate().get(bytes);

        return bytes;
    }

    /** @inheritDoc */
    @Override
    public int getColSize() {
        return columnSize;
    }
    
    /** @inheritDoc */
    @Override
    public void delete(String key) throws IOException {
        ensureSession();
        String prefixedKey = prefix(key);
        BatchStatement delBatch = new BatchStatement();
        
        PreparedStatement stmt = session.prepare(String.format("delete from %s.%s where key = ?", keyspace, columnFamily));
        BoundStatement bndStmt = new BoundStatement(stmt.setConsistencyLevel(ConsistencyLevel.ONE));
        delBatch = delBatch.add(bndStmt.bind(prefixedKey));
        
        session.execute(delBatch);
    }

    /** @inheritDoc */
    @Override
    public void delete(String key, long col) throws IOException {
        ensureSession();
        String prefixedKey = prefix(key);
        PreparedStatement stmt = session.prepare(String.format("delete from %s.%s where key = ? and name = ?", keyspace, columnFamily));
        BoundStatement bndStmt = new BoundStatement(stmt.setConsistencyLevel(ConsistencyLevel.ONE));
        session.execute(bndStmt.bind(prefixedKey, col));
    }

    /** @inheritDoc */
    @Override
    public boolean hasKey(String key) throws IOException {
        // if performance ends up sucking, let's go back to this:
        // return get(key, 0L) != null;
        ensureSession();
        String prefixedKey = prefix(key);
        PreparedStatement stmt = session.prepare(String.format("select value from %s.%s where key = ? limit 1", keyspace, columnFamily));
        BoundStatement bndStmt = new BoundStatement(stmt.setConsistencyLevel(ConsistencyLevel.ONE));
        ResultSet rs = session.execute(bndStmt.bind(prefixedKey));
        List<byte[]> values = new ArrayList<byte[]>();
        return rs.one() != null;
    }
    
    // prefix a key in the standard way.
    private String prefix(String key) {
        return String.format("%s/%s", rowPrefix, key);
    }
}
