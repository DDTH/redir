package com.github.ddth.com.redir.qnd;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.Term;

import ch.qos.logback.classic.Level;

import com.github.ddth.com.redir.RedisDirectory;

public class QndRedisDirIndex extends BaseQndRedisDir {

    public static void main(String args[]) throws Exception {
        initLoggers(Level.INFO);
        RedisDirectory DIR = new RedisDirectory(REDIS_HOST, REDIS_PORT, REDIS_PASSWORD);
        DIR.init();

        long t1 = System.currentTimeMillis();
        try {
            Analyzer analyzer = new StandardAnalyzer();
            IndexWriterConfig iwc = new IndexWriterConfig(analyzer);
            iwc.setOpenMode(OpenMode.CREATE_OR_APPEND);

            IndexWriter iw = new IndexWriter(DIR, iwc);
            Document doc = new Document();
            doc.add(new StringField("id", "thanhnb", Field.Store.YES));
            doc.add(new TextField("name", "Nguyen Ba Thanh", Field.Store.NO));
            iw.updateDocument(new Term("id", "thanhnb"), doc);

            iw.commit();

            iw.close();
        } finally {
            DIR.destroy();
        }
        long t2 = System.currentTimeMillis();
        System.out.println("Finished in " + (t2 - t1) / 1000.0 + " sec");
    }

}
