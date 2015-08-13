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

public class QndRedisDirSearch extends BaseQndRedisDir {

    public static void main(String args[]) throws Exception {
        initLoggers(Level.INFO);

        RedisDirectory DIR = new RedisDirectory(REDIS_HOST, REDIS_PORT, REDIS_PASSWORD);
        try {
            DIR.init();

            IndexReader ir = DirectoryReader.open(DIR);
            IndexSearcher is = new IndexSearcher(ir);

            Analyzer analyzer = new StandardAnalyzer();
            QueryParser parser = new QueryParser(null, analyzer);
            Query q = parser.parse("id:thanhnb");
            TopDocs result = is.search(q, 10);
            System.out.println("Hits:" + result.totalHits);
            for (ScoreDoc sDoc : result.scoreDocs) {
                int docId = sDoc.doc;
                Document doc = is.doc(docId);
                System.out.println(doc);
            }

            ir.close();
        } finally {
            DIR.destroy();
        }
    }

}
