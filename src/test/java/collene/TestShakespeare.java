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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.QueryBuilder;
import org.apache.lucene.util.Version;
import org.cassandraunit.CassandraCQLUnit;
import org.cassandraunit.dataset.cql.ClassPathCQLDataSet;
import org.junit.AfterClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collection;

@RunWith(Parameterized.class)
public class TestShakespeare {
    
    private static File fsIndexDir = TestUtil.getRandomTempDir();
    
    private static final boolean isTravis = System.getenv().containsKey("TRAVIS") && System.getenv().get("TRAVIS").equals("true");
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
    
    private Directory directory;
    
    @Parameterized.Parameters
    public static Collection<Object[]> data() throws Exception {
        Collection<Object[]> list = new ArrayList<Object[]>();
        
        //System.out.println("Using test dir " + fsIndexDir.getAbsolutePath());
        Object[] fsDirectory = new Object[]{ FSDirectory.open(fsIndexDir) };
        list.add(fsDirectory);
        
        Object[] memColDirectory = new Object[] { ColDirectory.open(
                "shakespeare.mem",
                new MemoryIO(4096)) };
        list.add(memColDirectory);
        
        Object[] cassColDirectory = new Object[] { ColDirectory.open(
                "shakespeare.cass",
                new CassandraIO(NextCassandraPrefix.get(), 8192, "collene", "cindex").session(cassandra.session))
        };
        list.add(cassColDirectory);
        
        return list;
    }
    
    public TestShakespeare(Directory directory) {
        this.directory = directory;
    }
    
    @AfterClass
    public static void clearDirectories() {
        TestUtil.removeDir(fsIndexDir);
    }
    
    @Test
    public void rest() throws IOException, ParseException {
        File shakespeareDir = new File("src/test/resources/shakespeare");
        File[] files = shakespeareDir.listFiles(new FileFilter() {
            @Override
            public boolean accept(File pathname) {
                return !pathname.isHidden();
            }
        });
        
        Collection<Document> documents = new ArrayList<Document>();
        for (File f : files) {
            String play = f.getName();
            int lineNumber = 1;
            BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(f)));
            String line = reader.readLine();
            while (line != null) {
                // index it.
                
                Document doc = new Document();
                doc.add(new NumericDocValuesField("line", lineNumber));
                doc.add(new Field("play", play, TextField.TYPE_STORED));
                doc.add(new Field("content", line, TextField.TYPE_STORED));
                documents.add(doc);
                
                lineNumber += 1;
                line = reader.readLine();
            }
            reader.close();
        }
        
        long writeStart = System.currentTimeMillis();
        Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_4_9);
        IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_4_9, analyzer);
        config.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
        IndexWriter writer = new IndexWriter(directory, config);
        writer.addDocuments(documents);
        //System.out.println(String.format("%s %d documents added", directory.getClass().getSimpleName(), documents.size()));
        writer.commit();
        //System.out.println(String.format("%s committed", directory.getClass().getSimpleName()));
//        writer.forceMerge(1);
//        System.out.println(String.format("%s merged", directory.getClass().getSimpleName()));
        long writeEnd = System.currentTimeMillis();
        //System.out.println(String.format("Write for %s took %dms", directory.getClass().getSimpleName(), writeEnd-writeStart));
        
        // let's search!
        IndexSearcher searcher = new IndexSearcher(DirectoryReader.open(writer, false));
        QueryParser parser = new QueryParser(Version.LUCENE_4_9, "content", analyzer);
        
        String[] queryTerms = new String[] { "trumpet" };
        
        for (String term : queryTerms) {
            long searchStart = System.currentTimeMillis();
            Query query = parser.parse(term);
            TopDocs docs = searcher.search(query, 10);
            long searchEnd = System.currentTimeMillis();
            //System.out.println(String.format("%s %d total hits in %d", directory.getClass().getSimpleName(), docs.totalHits, searchEnd - searchStart));
            for (ScoreDoc doc : docs.scoreDocs) {
                //System.out.println(String.format("%d %.2f %d", doc.doc, doc.score, doc.shardIndex));
            }
        }
        
        writer.close(true);
        //System.out.println(String.format("%s closed", directory.getClass().getSimpleName()));
        directory.close();
    }
}
