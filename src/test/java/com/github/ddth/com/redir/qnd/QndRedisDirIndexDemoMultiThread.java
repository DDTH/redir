package com.github.ddth.com.redir.qnd;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.Term;

import ch.qos.logback.classic.Level;

import com.github.ddth.com.redir.RedisDirectory;

public class QndRedisDirIndexDemoMultiThread extends BaseQndRedisDir {

    static final long MAX_ITEMS = 1000;
    static final AtomicLong COUNTER = new AtomicLong(0);
    static final ExecutorService ES = Executors.newFixedThreadPool(32);
    static final AtomicLong JOBS_DONE = new AtomicLong(0);

    public static void main(String args[]) throws Exception {
        initLoggers(Level.ERROR);
        RedisDirectory DIR = new RedisDirectory(REDIS_HOST, REDIS_PORT, REDIS_PASSWORD);
        DIR.init();

        long t1 = System.currentTimeMillis();
        try {
            Analyzer analyzer = new StandardAnalyzer();
            IndexWriterConfig iwc = new IndexWriterConfig(analyzer);
            iwc.setOpenMode(OpenMode.CREATE_OR_APPEND);
            IndexWriter iw = new IndexWriter(DIR, iwc);

            Path docDir = Paths
                    .get("/Users/btnguyen/Workspace/Apps/Apache-Cassandra-2.1.8/javadoc/");
            indexDocs(iw, docDir);

            while (JOBS_DONE.get() < MAX_ITEMS) {
                Thread.sleep(1);
            }
            iw.commit();
            iw.close();
        } finally {
            DIR.destroy();
        }
        long t2 = System.currentTimeMillis();
        System.out.println("Finished in " + (t2 - t1) / 1000.0 + " sec");
        ES.shutdown();
    }

    static void indexDocs(final IndexWriter writer, Path path) throws IOException {
        if (Files.isDirectory(path)) {
            Files.walkFileTree(path, new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
                        throws IOException {
                    indexDoc(writer, file, attrs.lastModifiedTime().toMillis());
                    return FileVisitResult.CONTINUE;
                }
            });
        } else {
            indexDoc(writer, path, Files.getLastModifiedTime(path).toMillis());
        }
    }

    static void indexDoc(final IndexWriter writer, final Path file, final long lastModified) {
        long counter = COUNTER.incrementAndGet();
        if (counter > MAX_ITEMS) {
            return;
        }
        System.out.println("Counter: " + counter);

        Runnable command = new Runnable() {
            @Override
            public void run() {
                try (InputStream stream = Files.newInputStream(file)) {
                    Document doc = new Document();

                    Field pathField = new StringField("path", file.toString(), Field.Store.YES);
                    doc.add(pathField);

                    doc.add(new LongField("modified", lastModified, Field.Store.NO));

                    doc.add(new TextField("contents", new BufferedReader(new InputStreamReader(
                            stream, StandardCharsets.UTF_8))));

                    if (writer.getConfig().getOpenMode() == OpenMode.CREATE) {
                        System.out.println("adding " + file);
                        writer.addDocument(doc);
                    } else {
                        System.out.println("updating " + file);
                        writer.updateDocument(new Term("path", file.toString()), doc);
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                    try {
                        writer.rollback();
                    } catch (IOException e1) {
                        e1.printStackTrace();
                    }
                } finally {
                    long jobsDone = JOBS_DONE.incrementAndGet();
                    System.out.println("Jobs done: " + jobsDone);
                }
            }
        };
        ES.execute(command);
    }

}
