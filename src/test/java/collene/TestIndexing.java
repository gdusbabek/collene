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

package collene;

import com.google.common.base.Joiner;
import com.google.common.collect.Sets;
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
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

@RunWith(Parameterized.class)
public class TestIndexing {

    private static final boolean strictFileChecking = System.getenv().containsKey("STRICT_FILE_CHECKING") && Boolean.parseBoolean(System.getenv("STRICT_FILE_CHECKING"));
    private static final Set<String> expectedFiles = new HashSet<String>(){{
        add("_5j.fdt");
        add("_5j.fdx");
        add("_5j.fnm");
        add("_5j.nvd");
        add("_5j.nvm");
        add("_5j.si");
        add("_5j_1.del");
        add("_5j_Lucene41_0.doc");
        add("_5j_Lucene41_0.pos");
        add("_5j_Lucene41_0.tim");
        add("_5j_Lucene41_0.tip");
        add("segments.gen");
        add("segments_2t");
        add("write.lock");
    }};
    
    private static File fsIndexDir = TestUtil.getRandomTempDir();
    
    private Directory directory;
    
    private static final boolean isTravis = System.getenv().containsKey("TRAVIS") && System.getenv().get("TRAVIS").equals("true");
    
    // chances are that I'm breaking rules trying to create a static CassandraCQLUnit instance. But this is how I got
    // it to work. Also, it was important to me that I use the same cassandra database for each test.
    public static CassandraCQLUnit cassandra = new CassandraCQLUnit(new ClassPathCQLDataSet("ddl.cql", "collene"), "/cassandra.yaml", "127.0.0.1", 9042) {{
        try {
            if (!isTravis) {
                this.before();
            }
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
        //list.add(fsDirectory);
        
        Object[] memColDirectory = new Object[] { ColDirectory.open(
                "mem",
                new MemoryIO(256),
                new MemoryIO(256)) };
        list.add(memColDirectory);
        
        memColDirectory = new Object[] { ColDirectory.open(
                "memsplit",
                new SplitRowIO(20, "/", new MemoryIO(256)),
                new SplitRowIO(20, "/", new MemoryIO(256))) };
        list.add(memColDirectory);
        
        if (cassandra.session == null) {
            throw new RuntimeException("cassandra-unit does not appear to be initialized");
        }
        
        CassandraIO baseCassandraIO = new CassandraIO(NextCassandraPrefix.get(), 256, "collene", "cindex").session(cassandra.session);
        Object[] cassColDirectory = new Object[] { ColDirectory.open(
                "casscol",
                baseCassandraIO,
                baseCassandraIO.clone(NextCassandraPrefix.get()).session(cassandra.session))
        };
        list.add(cassColDirectory);
        
        Object[] splitRowDirectory = new Object[] { ColDirectory.open(
                "casscolsplit",
                new SplitRowIO(20, "/", baseCassandraIO.clone(NextCassandraPrefix.get()).session(cassandra.session)),
                new SplitRowIO(20, "/", baseCassandraIO.clone(NextCassandraPrefix.get()).session(cassandra.session)))
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
        // uses hector. let's not do this.
        //EmbeddedCassandraServerHelper.cleanEmbeddedCassandra();
        TestUtil.removeDirOnExit(new File("/tmp/collene"));
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
        int idToDelete = docs.scoreDocs[0].doc;
        Assert.assertTrue(docs.totalHits > 0);
        
        query = parser.parse("fersoius");
        docs = searcher.search(query, 1);
        Assert.assertFalse(docs.totalHits > 0);
        
        // delete that document.
        DirectoryReader reader = DirectoryReader.open(writer, true);
        writer.tryDeleteDocument(reader, idToDelete);
        
        reader.close();
        writer.close();
        
        // list files
        Set<String> files = new HashSet<String>();
        System.out.println("Listing files for " + directory.toString());
        for (String file : directory.listAll()) {
            files.add(file);
            System.out.println(" " + file);
        }
        
        if (strictFileChecking) {
            System.out.println("String file checking...");
            Sets.SetView<String> difference = Sets.difference(expectedFiles, files);
            Assert.assertEquals(Joiner.on(",").join(difference), 0, difference.size());
        }
        
        reader = DirectoryReader.open(directory);
        searcher = new IndexSearcher(reader);
        query = parser.parse("aaa_4");
        docs = searcher.search(query, 1);
        reader.close();
        Assert.assertFalse(docs.totalHits > 0);
        
        directory.close();
    }
}
