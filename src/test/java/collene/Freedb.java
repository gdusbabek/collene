package collene;

import collene.freedb.FreeDbEntry;
import collene.freedb.FreeDbReader;
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
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Version;

import java.io.File;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collection;

public class Freedb {
    
    private static final int MAX_ENTRIES = 10000;// Integer.MAX_VALUE;
    private static final boolean VERBOSE = true;
    private static PrintStream out = System.out;
    
    public static void main(String[] args) throws Exception {
        String freedbPath = "/Users/gdusbabek/Downloads/freedb-complete-20140701.tar.bz2";
        Directory directory = ColDirectory.open(
                new CassandraIO(8192, "collene", "cindex").start("127.0.0.1:9042"),
                new MemoryIO(8192),
                new MemoryIO(8192)
        );

        FreeDbReader reader = new FreeDbReader(new File(freedbPath), 50000);
        reader.start();

        Collection<Document> documents = new ArrayList<Document>(100000);
        Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_4_9);
        IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_4_9, analyzer);
        config.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
        IndexWriter writer = new IndexWriter(directory, config);

        FreeDbEntry entry = reader.next();
        int count = 0;
        while (entry != null) {
            Document doc = new Document();
            String any = entry.toString();
            doc.add(new Field("any", any, TextField.TYPE_STORED));
            doc.add(new Field("artist", entry.getArtist(), TextField.TYPE_NOT_STORED));
            doc.add(new Field("album", entry.getAlbum(), TextField.TYPE_NOT_STORED));
            doc.add(new Field("title", entry.getTitle(), TextField.TYPE_NOT_STORED));
            doc.add(new Field("genre", entry.getGenre(), TextField.TYPE_NOT_STORED));
            doc.add(new Field("year", entry.getYear(), TextField.TYPE_NOT_STORED));
            for (int i = 0; i < entry.getTrackCount(); i++) {
                doc.add(new Field("track", entry.getTrack(i), TextField.TYPE_STORED));
            }
            documents.add(doc);
            if (VERBOSE) {
                out.println(any);
            }
            
            if (documents.size() == 100000) {
                out.println(String.format("Adding batch at count %d", count));
                writer.addDocuments(documents);
                writer.commit();
                out.println("done");
                documents.clear();
                
                // do a quick morrissey search for fun.
                IndexSearcher searcher = new IndexSearcher(DirectoryReader.open(writer, false));
                QueryParser parser = new QueryParser(Version.LUCENE_4_9, "any", analyzer);
                long searchStart = System.currentTimeMillis();
                Query query = parser.parse("morrissey");
                TopDocs docs = searcher.search(query, 10);
                long searchEnd = System.currentTimeMillis();
                out.println(String.format("%s %d total hits in %d", directory.getClass().getSimpleName(), docs.totalHits, searchEnd - searchStart));
                for (ScoreDoc d : docs.scoreDocs) {
                    out.println(String.format("%d %.2f %d", d.doc, d.score, d.shardIndex));
                }
                
            }
            
            count +=1;
            if (count >= MAX_ENTRIES) {
                // done indexing.
                break;
            }
            entry = reader.next();
        }
        
        if (documents.size() > 0) {
            out.println(String.format("Adding batch at count %d", count));
            writer.addDocuments(documents);
            writer.commit();
            out.println("done");
            documents.clear();
            
            // do a quick morrissey search for fun.
            IndexSearcher searcher = new IndexSearcher(DirectoryReader.open(writer, false));
            QueryParser parser = new QueryParser(Version.LUCENE_4_9, "any", analyzer);
            long searchStart = System.currentTimeMillis();
            Query query = parser.parse("morrissey");
            TopDocs docs = searcher.search(query, 10);
            long searchEnd = System.currentTimeMillis();
            out.println(String.format("%s %d total hits in %d", directory.getClass().getSimpleName(), docs.totalHits, searchEnd - searchStart));
            for (ScoreDoc d : docs.scoreDocs) {
                out.println(String.format("%d %.2f %d", d.doc, d.score, d.shardIndex));
            }
        }
        
        out.println("Indexed " + count + " things");
        
        long startMerge = System.currentTimeMillis();
        writer.forceMerge(1, true);
        long endMerge = System.currentTimeMillis();
        out.println(String.format("merge took %d ms", endMerge-startMerge));
        out.println("I think these are the files:");
        for (String s : directory.listAll()) {
            out.println(s);
        }
        
        writer.close();
        System.exit(0);
    }
}
