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

public class CassandraIO implements IO {
    private final int columnSize;
    private final String keyspace;
    private final String index;
    private final String rowPrefix;
    
    private Cluster cluster;
    private Session session = null;
    
    // no attempt to make consistent. races are entirely possible. deal with it for now.
    private Set<String> keyCache = new HashSet<>();
    
    public CassandraIO(String rowPrefix, int columnSize, String keyspace, String index) {
        this.columnSize = columnSize;
        this.keyspace = keyspace;
        this.index = index;
        this.rowPrefix = rowPrefix;
    }
    
    public CassandraIO clone(String newRowPrefix) {
        CassandraIO io = new CassandraIO(newRowPrefix, columnSize, keyspace, index);
        io.session = this.session;
        io.cluster = this.cluster;
        return io;
    }
    
    // connect to a cluster and build a session.
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
    
    // use this active session.
    public CassandraIO session(Session session) {
        this.session = session;
        this.cluster = session.getCluster();
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
        ensureSession();
        String prefixedKey = prefix(key);
        BatchStatement batch = new BatchStatement();
        PreparedStatement stmt = session.prepare(String.format("insert into %s.%s (key, name, value) values(?, ?, ?);", keyspace, index));
        BoundStatement bndStmt = new BoundStatement(stmt.setConsistencyLevel(ConsistencyLevel.ONE));
        batch.add(bndStmt.bind(prefixedKey, col, ByteBuffer.wrap(value)));
        session.execute(batch);
    }

    @Override
    public byte[] get(String key, long col) throws IOException {
        ensureSession();
        String prefixedKey = prefix(key);
        PreparedStatement stmt = session.prepare(String.format("select value from %s.%s where key = ? and name = ?", keyspace, index));
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

    @Override
    public Iterable<byte[]> allValues(String key) throws IOException {
        ensureSession();
        String prefixedKey = prefix(key);
        PreparedStatement stmt = session.prepare(String.format("select value from %s.%s where key = ?", keyspace, index));
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

    @Override
    public int getColSize() {
        return columnSize;
    }
    
    @Override
    public void delete(String key) throws IOException {
        ensureSession();
        String prefixedKey = prefix(key);
        BatchStatement delBatch = new BatchStatement();
        
        PreparedStatement stmt = session.prepare(String.format("delete from %s.%s where key = ?", keyspace, index));
        BoundStatement bndStmt = new BoundStatement(stmt.setConsistencyLevel(ConsistencyLevel.ONE));
        delBatch = delBatch.add(bndStmt.bind(prefixedKey));
        
        session.execute(delBatch);
        
        keyCache.remove(key);
    }

    @Override
    public void delete(String key, long col) throws IOException {
        ensureSession();
        String prefixedKey = prefix(key);
        PreparedStatement stmt = session.prepare(String.format("delete from %s.%s where key = ? and name = ?", keyspace, index));
        BoundStatement bndStmt = new BoundStatement(stmt.setConsistencyLevel(ConsistencyLevel.ONE));
        session.execute(bndStmt.bind(prefixedKey, col));
    }

    @Override
    public boolean hasKey(String key) throws IOException {
        // todo: this way vs doing allKeys()?
        return get(key, 0L) != null;
    }
    
    private String prefix(String key) {
        return String.format("%s/%s", rowPrefix, key);
    }
}
