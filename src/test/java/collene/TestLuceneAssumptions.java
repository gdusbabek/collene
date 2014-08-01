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

import com.google.common.collect.Lists;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class TestLuceneAssumptions {
    private static List<File> pleaseDelete = new ArrayList<File>();
    
    @AfterClass
    public static void deleteDirs() {
        for (File dir : pleaseDelete) {
            TestUtil.removeDir(dir);
        }
    }
    
    @Test
    public void testCanSeeUpdatesAfterAdd() throws Exception {
        // this verifies that any reader can see updates after documents are added.
        File fdir = TestUtil.getRandomTempDir();
        pleaseDelete.add(fdir);
        
        Directory dir = FSDirectory.open(fdir);
        Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_4_9);
        IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_4_9, analyzer);
        config.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
        IndexWriter writer = new IndexWriter(dir, config);
        
        Document doc0 = new Document();
        Document doc1 = new Document();
        doc0.add(new Field("f0", "aaa", TextField.TYPE_STORED));
        doc1.add(new Field("f0", "bbb", TextField.TYPE_STORED));
        List<Document> docs = Lists.newArrayList(doc0, doc1);
        writer.addDocuments(docs, analyzer);

        IndexSearcher searcher = new IndexSearcher(DirectoryReader.open(writer, false));
        QueryParser parser = new QueryParser(Version.LUCENE_4_9, "f0", new StandardAnalyzer(Version.LUCENE_4_9));
        
        Query query = parser.parse("bbb");
        TopDocs topDocs = searcher.search(query, 10);

        Assert.assertEquals(1, topDocs.totalHits);
        Assert.assertEquals(1, topDocs.scoreDocs.length);
        
        writer.close();
        dir.close();
    }
    
    @Test
    public void listAfterEachStep() throws Exception {
        File fdir = TestUtil.getRandomTempDir();
        pleaseDelete.add(fdir);
        
        Directory dir = FSDirectory.open(fdir);
        Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_4_9);
        IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_4_9, analyzer);
        config.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
        
        System.out.println("Before creating writer");
        dump(fdir, dir);
        
        IndexWriter writer = new IndexWriter(dir, config);
        System.out.println("After creating writer");
        dump(fdir, dir);
        
        List<Document> docs = new ArrayList<Document>();
        for (int i = 0; i < 50000; i++) {
            Document doc = new Document();
            for (int f = 0; f < 5; f++) {
                doc.add(new Field("field_" + f, TestUtil.randomString(128), TextField.TYPE_STORED));
            }
            docs.add(doc);
        }
        writer.addDocuments(docs, analyzer);
        docs.clear();
        
        System.out.println("After doc add 0");
        dump(fdir, dir);
        
        for (int i = 0; i < 50000; i++) {
            Document doc = new Document();
            for (int f = 0; f < 5; f++) {
                doc.add(new Field("field_" + f, TestUtil.randomString(128), TextField.TYPE_STORED));
            }
            docs.add(doc);
        }
        writer.addDocuments(docs, analyzer);
        docs.clear();
        
        System.out.println("After doc add 1");
        dump(fdir, dir);
        
        writer.commit();
        
        System.out.println("After commit");
        dump(fdir, dir);
        
        writer.forceMerge(1, true);
        System.out.println("Right after merge");
        dump(fdir, dir);
        
        try { Thread.currentThread().sleep(5000); } catch (Exception ex) {}
        System.out.println("After sleeping after merge");
        dump(fdir, dir);
        
        writer.close();
        System.out.println("After writer close");
        dump(fdir, dir);
        
        dir.close();
        System.out.println("After dir close");
        dump(fdir, dir);
    }
    
    private static void dump(File fileDir, Directory indexDir) throws Exception {
        for (File f : fileDir.listFiles()) {
            System.out.println(String.format("f %s", f.getAbsolutePath()));
        }
        try {
            for (String s : indexDir.listAll()) {
                System.out.println(String.format("i %s", s));
            }
        } catch (AlreadyClosedException ex) {
            System.out.println("Cannot list closed directory");
        }
    }
}
