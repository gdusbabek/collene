package collene;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;
import org.cassandraunit.CassandraCQLUnit;
import org.cassandraunit.dataset.cql.ClassPathCQLDataSet;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

@RunWith(Parameterized.class)
public class TestIndexing {
    
    private static File fsIndexDir = TestUtil.getRandomTempDir();
    
    private Directory directory;
    
    // chances are that I'm breaking rules trying to create a static CassandraCQLUnit instance. But this is how I got
    // it to work. Also, it was important to me that I use the same cassandra database for each test.
    public static CassandraCQLUnit cassandra = new CassandraCQLUnit(new ClassPathCQLDataSet("ddl.cql", "collene")) {{
        try {
            this.before();
            this.load();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }};
    
    @Parameterized.Parameters
    public static Collection<Object[]> data() throws Exception {
        Collection<Object[]> list = new ArrayList<Object[]>();
        
        System.out.println("Using test dir " + fsIndexDir.getAbsolutePath());
        Object[] fsDirectory = new Object[]{ FSDirectory.open(fsIndexDir) };
        list.add(fsDirectory);
        
        Object[] memColDirectory = new Object[] { ColDirectory.open(
                "mem",
                new MemoryIO(256), 
                new MemoryIO(256), 
                new MemoryIO(256)) };
        list.add(memColDirectory);
        
        if (cassandra.session == null) {
            throw new RuntimeException("cassandra-unit does not appear to be initialized");
        }
        
        Object[] cassColDirectory = new Object[] { ColDirectory.open(
                "casscol",
                new CassandraIO(NextCassandraPrefix.get(), 256, "collene", "cindex").session(cassandra.session),
                new CassandraIO(NextCassandraPrefix.get(), 256, "collene", "cmeta").session(cassandra.session),
                new CassandraIO(NextCassandraPrefix.get(), 256, "collene", "clock").session(cassandra.session))
        };
        list.add(cassColDirectory);
        
        Object[] splitRowDirectory = new Object[] { ColDirectory.open(
                "casscolsplit",
                new SplitRowIO(20, "/", new CassandraIO(NextCassandraPrefix.get(), 256, "collene", "cindex").session(cassandra.session)),
                new CassandraIO(NextCassandraPrefix.get(), 256, "collene", "cmeta").session(cassandra.session),
                new CassandraIO(NextCassandraPrefix.get(), 256, "collene", "clock").session(cassandra.session))
        };
        list.add(splitRowDirectory);
        
        // todo: randomize list.
        return list;
    }
    
    public TestIndexing(Directory directory) {
        this.directory = directory;
    }

    @AfterClass
    public static void clearDirectories() {
        TestUtil.removeDir(fsIndexDir);
    }
    
    @AfterClass 
    public static void clearData() {
        cassandra.session.close();
        cassandra.cluster.close();
        EmbeddedCassandraServerHelper.cleanEmbeddedCassandra();
    }
    
    @Test
    public void test() throws IOException, ParseException {
        Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_4_9);
        
        // write it out.
        IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_4_9, analyzer);
        config.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
        IndexWriter writer = new IndexWriter(directory, config);
        
        for (int i = 0; i < 100; i++) {
            Collection<Document> documents = new ArrayList<Document>();
            Document doc = new Document();
            doc.add(new Field("key", "aaa_" + i, TextField.TYPE_STORED));
            doc.add(new Field("not", "notaaa", TextField.TYPE_NOT_STORED));
            doc.add(new Field("meta", "aaa_meta_aaa_" + i, TextField.TYPE_STORED));
            documents.add(doc);
            
            writer.addDocuments(documents);
            
            writer.commit();
            writer.forceMerge(1);
            writer.forceMergeDeletes(true);
        }
        
        // now read it back.
        IndexSearcher searcher = new IndexSearcher(DirectoryReader.open(writer, false));
        QueryParser parser = new QueryParser(Version.LUCENE_4_9, "key", analyzer);
        
        Query query = parser.parse("aaa_4");
        TopDocs docs = searcher.search(query, 1);
        Assert.assertTrue(docs.totalHits > 0);
        
        query = parser.parse("fersoius");
        docs = searcher.search(query, 1);
        Assert.assertFalse(docs.totalHits > 0);
        
        writer.close();
        directory.close();
    }
}
