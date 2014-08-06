# Collene [![Build Status](https://travis-ci.org/gdusbabek/collene.svg?branch=master)](https://travis-ci.org/gdusbabek/collene)

Sorry. I suck at naming things. COLumns + lucENE.

## Motivations

First, and formost: this is exploratory. It may turn out to yield nothing useful or better than the status quo.
However, here are some problems this may get in the general direction of a solution for:

1. A fully distributed Lucene index without sharding.
  * Really, this is a better fault-tolerance story than any existing open- or closed-source project that I know about.
1. The ability to have an Lucene index whose size is greater than what is currently possible using a single machine.
1. I also hope this exercise will satisfy my hope curiosity of knowing if there is anything better than ElasticSearch
   and Solr.

## Primary Interface

It is `collene.IO`. Implement that to talk to whatever column store you have. Then everything just works.

I've included a `MemoryIO` implementation in testing (it works), and a `CassandraIO`.

## <strike>It Sucks, But</strike> It's Getting Better

Lucene does a lot of tiny one-byte writes. <strike>This means performance will be poor because chances are that you need to
first read a column before you apply the update.</strike> Collene batches IO for performance. This works well with 
Lucene because semantically either a file exists, or it doesn't, and files are never modified.

There is a ton of low hanging performance fruit. Go for it.

## TODOs and Bugs That I Know About

1. <strike>Test document deletion and observe performance.</strike> (Performance sucks)
1. Multi-directory writing and merging.
1. Multi-directory merging (without IO penalty).

## So Then...

### Things that need to be verified or implemented and then verified

1. <strike>Look at a way to implement `Directory.listAll()`, probably using a long row. (Let's face it, if this grows to
   millions of entries, you have other problems.) After thinking about it for a bit,</strike> I want to use several long rows
   and then read from all of them. This is a little way of sharding.
1. Document retrieval.

### Questions that might help me figure a few things out.

1. When does a doc get its id?. Could I store that in some other place to have quicker document retrieval?

### Things I haven't thought too deeply about, but may be a problem.

1. Atomicity. I've treated non-atomic operations (file meta updates) as atomic operations associated with row (file)
   writes.
1. What happens in a partitioned environment?
  * Hypothetically, there could be two writers, which would be bad, very bad.
  * As long as writes are always controlled by a single, same node, we are fine.
