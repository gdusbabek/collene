# Collene

Sorry. I suck at naming things. COLumns + lucENE.

## Primary Interface

It is `collene.IO`. Implement that to talk to whatever column store you have.

I've included a `MemoryIO` implementation in testing (it works), and a `CassandraIO` that currently doesn't work very well.

## It Sucks

Lucene does a lot of tiny one-byte writes. This means performance will be poor because chances are that you need to
first read a column before you apply the update.

The good news is that there is a ton of low hanging performance fruit. Go for it.

## Bugs That I Know About

1. Exception when trying to run the tests twice in a row. Should be easy fix; I just haven't done it.
2. Can't run the cassandra test right after the memory test. IndexWriter complains about not being able to acquire a
   lock. This is troubling because it means there is some shared state that I'm not aware of.