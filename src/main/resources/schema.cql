
create keyspace collene with replication = { 'class': 'SimpleStrategy', 'replication_factor': 1 };
use collene;

create table if not exists collene.cindex (
  key text,
  name bigint,
  value blob,
  primary key(key, name)
)
with compact storage 
and compaction = { 'class': 'SizeTieredCompactionStrategy', 'min_threshold': 2, 'max_threshold': 4, 'min_sstable_size': 100, 'tombstone_compaction_interval' : 5, 'tombstone_threshold': 0.1 }  
and compression = { 'sstable_compression': 'LZ4Compressor', 'chunk_length_kb': 64, 'crc_check_chance': 0.5};

create table if not exists collene.cmeta (
  key text,
  name bigint,
  value blob,
  primary key(key, name)
)
with compact storage 
and compaction = { 'class': 'SizeTieredCompactionStrategy', 'min_threshold': 2, 'max_threshold': 4, 'min_sstable_size': 100, 'tombstone_compaction_interval' : 5, 'tombstone_threshold': 0.1 }  
and compression = { 'sstable_compression': 'LZ4Compressor', 'chunk_length_kb': 64, 'crc_check_chance': 0.5};

create table if not exists collene.clock (
  key text,
  name bigint,
  value blob,
  primary key(key, name)
)
with compact storage 
and compaction = { 'class': 'SizeTieredCompactionStrategy', 'min_threshold': 2, 'max_threshold': 4, 'min_sstable_size': 100, 'tombstone_compaction_interval' : 5, 'tombstone_threshold': 0.1 }  
and compression = { 'sstable_compression': 'LZ4Compressor', 'chunk_length_kb': 64, 'crc_check_chance': 0.5};
