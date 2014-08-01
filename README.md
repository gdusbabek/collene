# Collene [![Build Status](https://secure.travis-ci.org/gdusbabek/collene.png)](http://travis-ci.org/gdusbabek/collene)

Sorry. I suck at naming things. COLumns + lucENE.

## Motivations

First, and formost: this is exploratory. It may turn out to yield nothing useful or better than the status quo.
However, here are some problems this may get in the general direction of a solution for:

1. A fully distributed Lucene index without sharding.
  * Really, this is a better fault-tolerance story than any existing open- or closed-source project that I know about.
1. The ability to have an Lucene index whose size is greater than what is currently possible using a single machine.
1. I also hope this exercise will satisfy my hope curiosity of knowing if there is anything better than ElasticSearch.

Here are some things I know this will never be good at (if you're interested in these, please keep looking):
1. Fully distributed index building and data ingestion. This will never be possible without resorting to sharding.

## Primary Interface

It is `collene.IO`. Implement that to talk to whatever column store you have. Then everything just works.

I've included a `MemoryIO` implementation in testing (it works), and a `CassandraIO` that currently doesn't work very well.

## It Sucks, But It's Getting Better.

Lucene does a lot of tiny one-byte writes. <strike>This means performance will be poor because chances are that you need to
first read a column before you apply the update.</strike> Collene batches IO for performance. This works well with 
Lucene because semantically either a file exists, or it doesn't, and files are never modified.

The good news is that there is a ton of low hanging performance fruit. Go for it.

## TODOs and Bugs That I Know About

1. Clean up the column families. cmeta and clock could be combined if I ever figure out how to write an empty lock.
1. Will need another column family that can store a collection of all the file names. I've noted somewhere else how it
   would be wise to break this out into N rows, rather than a single row.
1. Multi-directory writing and merging (without IO penalty).

## So Then...

### Things that need to be verified or implemented and then verified

1. Look at a way to implement `Directory.listAll()`, probably using a long row. (Let's face it, if this grows to
   millions of entries, you have other problems.) After thinking about it for a bit, I want to use several long rows
   and then read from all of them. This is a little way of sharding.
1. Document retrieval.

### Questions that might help me figure a few things out.

1. When does a doc get its id?. Could I store that in some other place to have quicker document retrieval?

### Things I haven't thought too deeply about, but may be a problem.

1. What happens in a partitioned environment?
  * Hypothetically, there could be two writers, which would be bad, very bad.
  * As long as writes are always controlled by a single, same node, we are fine.
