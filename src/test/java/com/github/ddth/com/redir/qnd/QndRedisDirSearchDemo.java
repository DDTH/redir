package com.github.ddth.com.redir.qnd;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;

import ch.qos.logback.classic.Level;

import com.github.ddth.com.redir.RedisDirectory;

public class QndRedisDirSearchDemo extends BaseQndRedisDir {

    public static void main(String args[]) throws Exception {
        initLoggers(Level.ERROR);
        RedisDirectory DIR = new RedisDirectory(REDIS_HOST, REDIS_PORT, REDIS_PASSWORD);
        DIR.init();

        long t1 = System.currentTimeMillis();
        try {
            IndexReader ir = DirectoryReader.open(DIR);
            IndexSearcher is = new IndexSearcher(ir);

            for (int i = 0; i < 10; i++) {
                Analyzer analyzer = new StandardAnalyzer();
                QueryParser parser = new QueryParser(null, analyzer);
                Query q = parser.parse("contents:clear AND contents:html");
                TopDocs result = is.search(q, 10);
                System.out.println("Hits:" + result.totalHits);
                for (ScoreDoc sDoc : result.scoreDocs) {
                    int docId = sDoc.doc;
                    Document doc = is.doc(docId);
                    System.out.println(doc);
                }
            }

            ir.close();
        } finally {
            DIR.destroy();
        }
        long t2 = System.currentTimeMillis();
        System.out.println("Finished in " + (t2 - t1) / 1000.0 + " sec");
    }

}
