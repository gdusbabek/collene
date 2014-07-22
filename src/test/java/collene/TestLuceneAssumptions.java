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
}
