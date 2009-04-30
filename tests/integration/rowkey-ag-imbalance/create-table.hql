drop table if exists LoadTest;
create table COMPRESSOR="none" LoadTest (
       'page_views',
       'serp_views',
       ACCESS GROUP page ('page_views'),
       ACCESS GROUP default ('serp_views')
);
