redir
=====

[Redis](http://redis.io) implementation of [Lucene](http://lucene.apache.org) Directory.

By Thanh Ba Nguyen (btnguyen2k (at) gmail.com)

Project home:
[https://github.com/DDTH/redir](https://github.com/DDTH/redir)


## Features ##

- Store [Lucene](http://lucene.apache.org)'s index in [Redis](http://redis.io).


## Installation ##

Latest release version: `0.1.0`. See [RELEASE-NOTES.md](RELEASE-NOTES.md).

Maven dependency:

```xml
<dependency>
	<groupId>com.github.ddth</groupId>
	<artifactId>redir</artifactId>
	<version>0.1.0</version>
</dependency>
```

### Related projects/libs ###

- [jedis](https://github.com/xetorthio/jedis): the underlying lib to access Redis.


## Usage ##

Create a `RedisDirectory` instance:
```java
RedisDirectory DIR = new RedisDirectory(redisHost, redisPort, redisPassword);
DIR.init();
```

Index documents with `IndexWriter`:
```java
Analyzer analyzer = new StandardAnalyzer();
IndexWriterConfig iwc = new IndexWriterConfig(analyzer);
iwc.setOpenMode(OpenMode.CREATE_OR_APPEND);
IndexWriter iw = new IndexWriter(DIR, iwc);

// add/update documents
// ...

iw.commit();
iw.close();
```

Or, search documents with `IndexSearcher`:
```java
IndexReader ir = DirectoryReader.open(DIR);
IndexSearcher is = new IndexSearcher(ir);

// search documents
// ...

ir.close();
```

Call `RedisDirectory.destroy()` when done.


Examples: see [src/test/java](src/test/java).

## License ##

See LICENSE.txt for details. Copyright (c) 2015 Thanh Ba Nguyen.

Third party libraries are distributed under their own license(s).
