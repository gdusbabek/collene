# Collene

Sorry. I suck at naming things. COLumns + lucENE.

## Primary Interface

It is `collene.IO`. Implement that to talk to whatever column store you have.

I've included a `MemoryIO` implementation in testing (it works), and a `CassandraIO` that currently doesn't work very well.

## It Sucks

Lucene does a lot of tiny one-byte writes. This mean performance will be bad because chances are that you need to
first read a column before you apply the update.

The good news is that there is a ton of low hanging performance fruit