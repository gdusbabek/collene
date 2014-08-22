# Collene [![Build Status](https://travis-ci.org/gdusbabek/collene.svg?branch=master)](https://travis-ci.org/gdusbabek/collene)

Sorry. I suck at naming things. COLumns + lucENE.

## Motivations

First, and formost: this is exploratory. It may turn out to yield nothing useful or better than the status quo.
However, here are some problems this may get in the general direction of a solution for:

1. A fully distributed Lucene index without sharding.
  * Really, this is a better fault-tolerance story than any existing open- or closed-source project that I know about.
1. The ability to have an Lucene index whose size is greater than what is currently possible using a single machine.
1. Drop-in library compatibility with Apache Lucene.
1. I also hope this exercise will satisfy my curiosity of knowing if there is anything better than ElasticSearch
   and Solr.

## Primary Interfaces

The main implementational interface is `collene.IO`. 
Implement that to talk to whatever column store you have.
Then everything else should Just Work™.

I've included a `MemoryIO` implementation in testing (it works), and a `CassandraIO` implementation that talks to
Cassandra.

Other `*IO` implementations are intended compose basic `IO` implementations to add functionality transparently, e.g.:
splitting long rows or caching.

The main search class is `collene.ColDirectory`. Use it where you would normally have used an 
`org.apache.lucene.index.Directory`. 

## Performance

I haven't done any pure Apples to Apples testing yet. 
All I really have is the Shakespeare corpus that ships with this code.
Here are the numbers, taken by Running TestShakespeare on my machine with no attempt to be scientific or correct:

1. MMapDirectory (files): 1.2 seconds to index.
1. ColDirectory (memory backed): 1.5 seconds to index.
1. ColDirectory (cassandra backed): 2.3 seconds to index.

I modified the Freedb search application to use different index. Here are the results there.

1. MMapDirectory: Indexed 400k documents in 122s, cold search 2ms, warm search 1ms.
1. ColDirectory (memory): Indexed 400k documents in 119s, cold search 14ms, warm search 1ms .
1. ColDirectory (cassandra): Indexed 400k documents in 211s, cold search 64ms, warm search 1ms.

Here are the vectors for improvement:

1. Profile for hotspots in general.
2. Tune Cassandra
3. Smarter use the the Cassandra Java Driver.

So room for improvement.

## <strike>It Sucks, But</strike> It's Getting Better

Lucene does a lot of tiny one-byte writes. <strike>This means performance will be poor because chances are that you need to
first read a column before you apply the update.</strike> Collene batches IO for performance. This works well with 
Lucene because semantically either a file exists, or it doesn't, and files are never modified.

There is a ton of low hanging performance fruit. Go for it.

## TODOs and Bugs That I Know About

1. I track everything with [Github issues](https://github.com/gdusbabek/collene/issues)
1. If you use a caching `IO` for searches reads, there needs to be a better way of evicting data from the cache.
   Probably a size limit with a last-accessed wins algorithm.
1. Document the code.

## So Then...

### Things I haven't thought too deeply about, but may be a problem.

1. Atomicity. I've treated non-atomic operations (file meta updates) as atomic operations associated with row (file)
   writes.
1. What happens in a partitioned environment?
  * Hypothetically, there could be two writers, which would be bad, very bad.
  * As long as writes are always controlled by a single, same node, we are fine.
