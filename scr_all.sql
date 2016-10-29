SELECT AVG(EXTRACT(second FROM (STOP_DATE-START_DATE) day to second)) second FROM TOTO_UPDATE_LOG WHERE TOTO_SCRIPT LIKE '%:x%' ;

SELECT AVG(EXTRACT(second FROM (STOP_DATE-START_DATE) day to second)+EXTRACT(minute FROM (STOP_DATE-START_DATE) day to second)*60) avg_sec
    ,MIN(EXTRACT(second FROM (STOP_DATE-START_DATE) day to second)+EXTRACT(minute FROM (STOP_DATE-START_DATE) day to second)*60) min_sec
    ,MAX(EXTRACT(second FROM (STOP_DATE-START_DATE) day to second)+EXTRACT(minute FROM (STOP_DATE-START_DATE) day to second)*60) max_sec
    ,SUM(EXTRACT(second FROM (STOP_DATE-START_DATE) day to second)+EXTRACT(minute FROM (STOP_DATE-START_DATE) day to second)*60) sum_sec
    ,COUNT(*)
  FROM (SELECT * FROM TOTO_UPDATE_LOG ORDER BY STOP_DATE DESC)
   WHERE rownum<1000
  ;


SELECT AVG(EXTRACT(second FROM (DATE_INS-DATE_START) day to second)+EXTRACT(minute FROM (DATE_INS-DATE_START) day to second)*60) avg_sec
    ,MIN(EXTRACT(second FROM (DATE_INS-DATE_START) day to second)+EXTRACT(minute FROM (DATE_INS-DATE_START) day to second)*60) min_sec
    ,MAX(EXTRACT(second FROM (DATE_INS-DATE_START) day to second)+EXTRACT(minute FROM (DATE_INS-DATE_START) day to second)*60) max_sec
    ,SUM(EXTRACT(second FROM (DATE_INS-DATE_START) day to second)+EXTRACT(minute FROM (DATE_INS-DATE_START) day to second)*60) sum_sec
    ,COUNT(*)
  FROM (SELECT * FROM TOTO_OUTCOMES_TMP ORDER BY DATE_INS DESC)
   WHERE rownum < 9000
  ;
  
SELECT *
  FROM (SELECT * FROM TOTO_OUTCOMES_TMP ORDER BY DATE_INS DESC)
   WHERE rownum < 100
   ORDER BY EXTRACT(second FROM (DATE_INS-DATE_START) day to second)+EXTRACT(minute FROM (DATE_INS-DATE_START) day to second)*60 desc
  ;

select d.object_name, d.object_type, d.subobject_name, count(*) blk_cnt,
       count(*)*kcbwds.blk_size bytes
  from x$kcbwds kcbwds,
       (select lo_setid, hi_setid from v$buffer_pool 
         where name = 'KEEP') setds,
       x$bh bh,
       dba_objects d
 where set_id between setds.lo_setid and setds.hi_setid
   and bh.set_ds = kcbwds.addr
   and d.data_object_id = bh.obj
  group by d.object_name, d.object_type,d.subobject_name, kcbwds.blk_size;
  
ALTER TABLE SL.toto_outcomes_ok STORAGE (BUFFER_POOL KEEP);

select name, (1-(physical_reads/(db_block_gets+consistent_gets))) "Buffer Pool Hit Ratio"
from v$buffer_pool_statistics
where db_block_gets + consistent_gets != 0;


--Определяем размер партиции для таблицы
--размер блока в байтах
select value from v$parameter where name = 'db_block_size';    

--узнаю реальный средний физический размер партиции
select AVG(blocks*8192/1024/1024) as mb, COUNT(*) from SYS.ALL_TAB_SUBPARTITIONS WHERE TABLE_NAME = 'TOTO_OUTCOMES_FULL';
select * from SYS.DBA_TAB_SUBPARTITIONS;


ALTER TABLE TOTO_OUTCOMES_OK MOVE STORAGE ( INITIAL 4194304 );

select sum(value) from v$sysstat  where name in ('table scan rows gotten','table fetch by rowid');
select value from v$sysstat  where name = 'table fetch continued row';

select (select value from v$sysstat  where name = 'table fetch continued row')/
(select sum(value) from v$sysstat  where name in ('table scan rows gotten','table fetch by rowid')) from dual;

select num_rows, chain_cnt from all_tables where owner='SL' and table_name='TOTO_OUTCOMES_OK';

select owner,table_name,num_rows, chain_cnt from dba_tables where chain_cnt is not null and chain_cnt<>0;

select e.username, s.sid, s.value  from v$statname n, v$sesstat s, v$session e where s.statistic#=n.statistic# and s.sid=e.sid and
name='table fetch continued row' and s.value>0;

select sum( (num_rows*AVG_ROW_LEN)) must_be_used, sum((blocks*8192 - AVG_SPACE_FREELIST_BLOCKS)) used from all_tables;update toto_outcomes_ok tar
    set sum15=0
      , sum14=0
      , sum13=0
      , sum12=0
      , sum11=0
      , sum10=0
      , sum9=0
      , upd_status = 0
    where sum15+sum14+sum13+sum12+sum11+sum10+sum9+upd_status>0;
COMMIT;

update toto_outcomes_ok tar
    set sum15=0
      , sum14=0
      , sum13=0
      , sum12=0
      , sum11=0
      , sum10=0
      , sum9=0
      , upd_status = 0
    where sum15+sum14+sum13+sum12+sum11+sum10+sum9+upd_status>0
    ;
COMMIT;

CREATE TABLE TOTO_SREZ AS
  SELECT * FROM toto_outcomes_ok where sum15+sum14+sum13+sum12+sum11+sum10+sum9+upd_status>0;
  
  
  SELECT TABLE_NAME, CACHE, BUFFER_POOL     FROM USER_TABLES    ORDER BY TABLE_NAME;
select name, value, isdefault, ismodified from v$parameter where name ='db_cache_advice' or name ='db_cache_size';
SELECT * FROM v$db_object_cache;

select distinct s.buffer_pool,
                ts.block_size,
                o.owner,
                o.object_name,
                o.total_blocks,
                o.total_blocks * ts.block_size / 1024 / 1024 SizeMB
  from (select ob.owner, ob.object_name, sum(b.blocks) total_blocks
          from (select bh.OBJD, count(1) blocks from V$bh bh group by bh.OBJD) b,
               dba_objects ob
         where b.OBJD = ob.object_id
         group by ob.owner, ob.object_name) o,
       dba_segments s,
       dba_tablespaces ts
 where s.owner = o.owner
   and s.segment_name = o.object_name
   and s.tablespace_name = ts.tablespace_name
   and s.buffer_pool = 'KEEP'
 order by buffer_pool desc, ts.block_size asc, o.total_blocks desc;

select d.object_name, d.object_type, d.subobject_name, count(*) blk_cnt,
       count(*)*kcbwds.blk_size bytes
  from x$kcbwds kcbwds,
       (select lo_setid, hi_setid from v$buffer_pool 
         where name = 'KEEP') setds,
       x$bh bh,
       dba_objects d
 where set_id between setds.lo_setid and setds.hi_setid
   and bh.set_ds = kcbwds.addr
   and d.data_object_id = bh.obj
  group by d.object_name, d.object_type,d.subobject_name, kcbwds.blk_size;  CREATE INDEX "SL"."TOTO_RATES_IDX_TXT" ON "SL"."TOTO_RATES" ("R") 
   INDEXTYPE IS "CTXSYS"."CONTEXT" ;



CREATE TABLE TEST_TXT AS
  SELECT * FROM TOTO_RATES ;
  
UPDATE TEST_TXT SET R=TRANSLATE(R, '12X', 'ABC') ;
UPDATE TEST_TXT SET R=TRANSLATE(R1||R2||R3||R4||R5||R6||R7||R8||R9||R10||R11||R12||R13||R14||R15, '12X', 'ABC');

UPDATE TEST_TXT SET R=TRANSLATE(R1, '12X', 'ABC')||' '||TRANSLATE(R2, '12X', 'ABC')||' '||TRANSLATE(R3, '12X', 'ABC')||' ' 
                    ||TRANSLATE(R4, '12X', 'ABC')||' '||TRANSLATE(R5, '12X', 'ABC')||' '||TRANSLATE(R6, '12X', 'ABC')||' '
                    ||TRANSLATE(R7, '12X', 'ABC')||' '||TRANSLATE(R8, '12X', 'ABC')||' '||TRANSLATE(R9, '12X', 'ABC')||' '
                    ||TRANSLATE(R10, '12X', 'ABC')||' '||TRANSLATE(R11, '12X', 'ABC')||' '||TRANSLATE(R12, '12X', 'ABC')||' '
                    ||TRANSLATE(R13, '12X', 'ABC')||' '||TRANSLATE(R14, '12X', 'ABC')||' '||TRANSLATE(R15, '12X', 'ABC');

select TRANSLATE('1xv2svdX 12', '12X', 'ABC') from dual;
  
  CREATE INDEX "SL"."TEST_TXT_IDX" ON "SL"."TEST_TXT" ("R") 
   INDEXTYPE IS "CTXSYS"."CONTEXT" ;

drop index TEST_TXT_IDX;

SELECT DISTINCT R--, SCORE(1) as sc
    ,CONTAINS ( R, '$AAAAAAAAAAAAAA', 2 )
    ,--CONTAINS ( R, 'A and A and A and A and A and A and A and A and A and A and A and A and A and A and A', 1 ) > 0
  --,CONTAINS ( R, 'A A A A A A A A A A A A A A A', 1 ) > 0
  --,CONTAINS ( R, '!AAAAAAAAAAAAAA', 1 ) > 0
  --,CONTAINS ( R, '!111111111111111', 1 ) > 0
  --,CONTAINS ( R, 'A or A or A or A or A or A or A or A or A or A or A or A or A or A or A', 1 ) > 0
    ,DECODE(tar.r1, '1', 1, 0)+DECODE(tar.r2, '1', 1, 0)
                                      +DECODE(tar.r3, '1', 1,0)+DECODE(tar.r4, '1', 1,0)
                                      +DECODE(tar.r5, '1', 1,0)+DECODE(tar.r6, '1', 1,0)
                                      +DECODE(tar.r7, '1', 1,0)+DECODE(tar.r8, '1', 1,0)
                                      +DECODE(tar.r9, '1', 1,0)+DECODE(tar.r10, '1', 1,0)
                                      +DECODE(tar.r11, '1', 1,0)+DECODE(tar.r12, '1', 1,0)
                                      +DECODE(tar.r13, '1', 1,0)+DECODE(tar.r14, '1', 1,0)
                                      +DECODE(tar.r15, '1', 1,0) as cn
  FROM TEST_TXT tar
  WHERE  
    DECODE(tar.r1, '1', 1, 0)+DECODE(tar.r2, '1', 1, 0)
                                      +DECODE(tar.r3, '1', 1,0)+DECODE(tar.r4, '1', 1,0)
                                      +DECODE(tar.r5, '1', 1,0)+DECODE(tar.r6, '1', 1,0)
                                      +DECODE(tar.r7, '1', 1,0)+DECODE(tar.r8, '1', 1,0)
                                      +DECODE(tar.r9, '1', 1,0)+DECODE(tar.r10, '1', 1,0)
                                      +DECODE(tar.r11, '1', 1,0)+DECODE(tar.r12, '1', 1,0)
                                      +DECODE(tar.r13, '1', 1,0)+DECODE(tar.r14, '1', 1,0)
                                      +DECODE(tar.r15, '1', 1,0)>=11
  ORDER BY cn desc
                                      ;
   
   
--Делаем еще таблицу TOTO_OUTCOMES_OK, организованную по индексу (это копия таблицы TOTO_OUTCOMES)
--Эту таблицу можно использовать для поиска (запросы к ней быстрее чем к 
CREATE TABLE SL.TOTO_OUTCOMES_OK_2
(
  ID,
  RATE_1,  RATE_2,  RATE_3,  RATE_4,  RATE_5,  RATE_6,  RATE_7,  RATE_8,  RATE_9,  RATE_10,
  RATE_11,  RATE_12,  RATE_13,  RATE_14,  RATE_15,  DATE_INS,  SUM9,  SUM10,  SUM11,  SUM12,  SUM13,  SUM14,  SUM15,
  UPD_STATUS, 
  CONSTRAINT TOTO_OUTCOMES_OK_PK2
  PRIMARY KEY
  (RATE_1, RATE_2, RATE_3, RATE_4, RATE_5, RATE_6, RATE_7, RATE_8, RATE_9, RATE_10, RATE_11, RATE_12, RATE_13, RATE_14, RATE_15)
  ENABLE VALIDATE
)
ORGANIZATION INDEX
PCTTHRESHOLD 50
TABLESPACE USERS
RESULT_CACHE (MODE DEFAULT)
PCTFREE    10
INITRANS   2
MAXTRANS   255
STORAGE    (
            INITIAL          64K
            NEXT             1M
            MAXSIZE          UNLIMITED
            MINEXTENTS       1
            MAXEXTENTS       UNLIMITED
            PCTINCREASE      0
            BUFFER_POOL      KEEP
            FLASH_CACHE      DEFAULT
            CELL_FLASH_CACHE DEFAULT
           )
NOLOGGING 
NOCOMPRESS
NOPARALLEL
NOMONITORING
as select * from TOTO_OUTCOMES_FULL;
/--Процедура (обратный способ) поиска всех summa в таблице TOTO_RATES для каждой записи в таблице TOTO_OUTCOMES_OK (или TOTO_OUTCOMES)
declare  
    nSumma number:=0; --сумма
    nSumSumma15 number:=0;
    nSumSumma14 number:=0;
    nSumSumma13 number:=0;
    nSumSumma12 number:=0;
    nSumSumma11 number:=0;
    nSumSumma10 number:=0;
    nSumSumma9 number:=0;
    dDateStart timestamp(5);
    dDateStart2 timestamp(5);
    nRate_cnt number;
    ucnt number;
    i number:=1;
BEGIN
  For ForRate in (select * from toto_outcomes_ok t where rownum<10 --id<=20
    ) loop --делаем расчет для первых 20 записей в БИГ таблице
      dDateStart:= SYSTIMESTAMP;
      nRate_cnt:=16;
      nSumSumma15:=0;
      nSumSumma14:=0;
      nSumSumma13:=0;
      nSumSumma12:=0;
      nSumSumma11:=0;
      nSumSumma10:=0;
      nSumSumma9:=0; 
            
      insert into toto_outcomes_tmp 
            (RATE_1,RATE_2,RATE_3,RATE_4,RATE_5,
             RATE_6,RATE_7,RATE_8,RATE_9,RATE_10,
             RATE_11,RATE_12,RATE_13,RATE_14,RATE_15,
             sum15,sum14,sum13,sum12,sum11,sum10,sum9,date_start)
          SELECT ForRate.Rate_1, ForRate.Rate_2, ForRate.Rate_3, ForRate.Rate_4, ForRate.Rate_5
            ,ForRate.Rate_6, ForRate.Rate_7, ForRate.Rate_8, ForRate.Rate_9, ForRate.Rate_10
            ,ForRate.Rate_11,ForRate.Rate_12,ForRate.Rate_13,ForRate.Rate_14,ForRate.Rate_15
            ,sum(CASE when cnt_comp =15 then summa else 0 end) sum15 
            ,sum(CASE when cnt_comp>=14 then summa else 0 end) sum14 
            ,sum(CASE when cnt_comp>=13 then summa else 0 end) sum13
            ,sum(CASE when cnt_comp>=12 then summa else 0 end) sum12
            ,sum(CASE when cnt_comp>=11 then summa else 0 end) sum11
            ,sum(CASE when cnt_comp>=10 then summa else 0 end) sum10
            ,sum(CASE when cnt_comp>= 9 then summa else 0 end) sum9 
            ,dDateStart
            FROM (
          select --rat.R1||rat.R2||rat.R3||rat.R4||rat.R5||rat.R6||rat.R7||rat.R8||rat.R9
                  --||rat.R10||rat.R11||rat.R12||rat.R13||rat.R14||rat.R15 as r_comp
                DECODE(rat.r1, ForRate.Rate_1, 1, 0)+DECODE(rat.r2, ForRate.Rate_2, 1, 0)+DECODE(rat.r3, ForRate.Rate_3, 1,0)
                    +DECODE(rat.r4, ForRate.Rate_4, 1,0)+DECODE(rat.r5, ForRate.Rate_5, 1,0)+DECODE(rat.r6, ForRate.Rate_6, 1,0)
                    +DECODE(rat.r7, ForRate.Rate_7, 1,0)+DECODE(rat.r8, ForRate.Rate_8, 1,0)+DECODE(rat.r9, ForRate.Rate_9, 1,0)
                    +DECODE(rat.r10, ForRate.Rate_10, 1,0)+DECODE(rat.r11, ForRate.Rate_11, 1,0)+DECODE(rat.r12, ForRate.Rate_12, 1,0)
                    +DECODE(rat.r13, ForRate.Rate_13, 1,0)+DECODE(rat.r14, ForRate.Rate_14, 1,0)+DECODE(rat.r15, ForRate.Rate_15, 1,0) cnt_comp
                ,summa
            from toto_rates rat 
            where /*DECODE(rat.r1, ForRate.Rate_1, 1, 0)+DECODE(rat.r2, ForRate.Rate_2, 1, 0)+DECODE(rat.r3, ForRate.Rate_3, 1, 0)
                  +DECODE(rat.r4, ForRate.Rate_4, 1, 0)+DECODE(rat.r5, ForRate.Rate_5, 1, 0)+DECODE(rat.r6, ForRate.Rate_6, 1, 0)
                  +DECODE(rat.r7, ForRate.Rate_7, 1, 0)+DECODE(rat.r8, ForRate.Rate_8, 1, 0)+DECODE(rat.r9, ForRate.Rate_9, 1, 0)
                  +DECODE(rat.r10, ForRate.Rate_10, 1, 0)+DECODE(rat.r11, ForRate.Rate_11, 1, 0)+DECODE(rat.r12, ForRate.Rate_12, 1, 0)
                  +DECODE(rat.r13, ForRate.Rate_13, 1, 0)+DECODE(rat.r14, ForRate.Rate_14, 1, 0)+DECODE(rat.r15, ForRate.Rate_15, 1, 0)>=9*/
            CONTAINS(r, ForRate.Rate_1||' and '||
                        ForRate.Rate_2||' and '||
                        ForRate.Rate_3||' and '||
                        ForRate.Rate_4||' and '||
                        ForRate.Rate_5||' and '||
                        ForRate.Rate_6||' and '||
                        ForRate.Rate_7||' and '||
                        ForRate.Rate_8||' and '||
                        ForRate.Rate_9||' and '||
                        ForRate.Rate_10||' and '||
                        ForRate.Rate_11||' and '||
                        ForRate.Rate_12||' and '||
                        ForRate.Rate_13||' and '||
                        ForRate.Rate_14||' and '||
                        ForRate.Rate_15
                        , 1 ) >= 9*3
          )
            GROUP BY ForRate.Rate_1, ForRate.Rate_2, ForRate.Rate_3, ForRate.Rate_4, ForRate.Rate_5
              ,ForRate.Rate_6, ForRate.Rate_7, ForRate.Rate_8, ForRate.Rate_9, ForRate.Rate_10
              ,ForRate.Rate_11,ForRate.Rate_12,ForRate.Rate_13,ForRate.Rate_14,ForRate.Rate_15,dDateStart
            ;               
            
            commit;
  end loop;
  commit; 
END;
/--Процедура (обратный способ) поиска всех summa в таблице TOTO_RATES для каждой записи в таблице TOTO_OUTCOMES_OK (или TOTO_OUTCOMES)
declare  
    nSumma number:=0; --сумма
    nSumSumma15 number:=0;
    nSumSumma14 number:=0;
    nSumSumma13 number:=0;
    nSumSumma12 number:=0;
    nSumSumma11 number:=0;
    nSumSumma10 number:=0;
    nSumSumma9 number:=0;
    var_idx varchar2(4000 byte);
    var_where varchar2(4000 byte):='';
    dDateStart timestamp(5);
    dDateStart2 timestamp(5);
    nRate_cnt number;
    ucnt number;
    i number:=1;
    j number:=1;
BEGIN
  For ForRate in (select * from toto_outcomes_ok t where rownum<3 --id<=20
    ) loop --делаем расчет для первых 20 записей в БИГ таблице
      dDateStart:= SYSTIMESTAMP;
      nRate_cnt:=16;
      nSumSumma15:=0;
      nSumSumma14:=0;
      nSumSumma13:=0;
      nSumSumma12:=0;
      nSumSumma11:=0;
      nSumSumma10:=0;
      nSumSumma9:=0; 
      nSumma:=0; 
      var_where:='';
      --Ищем  совпадения
      For ForVar in (select * 
                     from toto_idx 
                     where rate_cnt>=9 
                     order by rate_cnt desc, id
                     ) loop
          
      
          if i=16 or nRate_cnt<>ForVar.rate_cnt and var_where is not null then
              var_idx:= 'select sum(summa)'||
              ' from toto_rates '||
              ' where '||var_where;            
              
              
              dDateStart2:= SYSTIMESTAMP;
              EXECUTE IMMEDIATE var_idx into nSumma;
              --логируем время выполнения запроса
              insert into toto_update_log (toto_script,start_date,rate_cnt,ucnt) values (var_idx,dDateStart2,nRate_cnt,nSumma);
              
             -- DBMS_OUTPUT.put_line(nRate_cnt||' '||ForVar.rate_cnt||' nSumma='||nSumma||' var_idx: '|| var_idx);
              
              if nSumma>0 then
                  case when nRate_cnt>=15 then
                            nSumSumma15:=nSumSumma15+nSumma;
                            nSumSumma14:=nSumSumma14+nSumma;
                            nSumSumma13:=nSumSumma13+nSumma;
                            nSumSumma12:=nSumSumma12+nSumma;
                            nSumSumma11:=nSumSumma11+nSumma;
                            nSumSumma10:=nSumSumma10+nSumma;
                            nSumSumma9:=nSumSumma9+nSumma;
                       when nRate_cnt=14 then
                            nSumSumma14:=nSumSumma14+nSumma;
                            nSumSumma13:=nSumSumma13+nSumma;
                            nSumSumma12:=nSumSumma12+nSumma;
                            nSumSumma11:=nSumSumma11+nSumma;
                            nSumSumma10:=nSumSumma10+nSumma;
                            nSumSumma9:=nSumSumma9+nSumma;
                    when nRate_cnt=13 then
                            nSumSumma13:=nSumSumma13+nSumma;
                            nSumSumma12:=nSumSumma12+nSumma;
                            nSumSumma11:=nSumSumma11+nSumma;
                            nSumSumma10:=nSumSumma10+nSumma;
                            nSumSumma9:=nSumSumma9+nSumma;
                    when nRate_cnt=12 then
                            nSumSumma12:=nSumSumma12+nSumma;
                            nSumSumma11:=nSumSumma11+nSumma;
                            nSumSumma10:=nSumSumma10+nSumma;
                            nSumSumma9:=nSumSumma9+nSumma;
                    when nRate_cnt=11 then
                            nSumSumma11:=nSumSumma11+nSumma;
                            nSumSumma10:=nSumSumma10+nSumma;
                            nSumSumma9:=nSumSumma9+nSumma;
                    when nRate_cnt=10 then
                            nSumSumma10:=nSumSumma10+nSumma;
                            nSumSumma9:=nSumSumma9+nSumma;
                    when nRate_cnt=9 then
                            nSumSumma9:=nSumSumma9+nSumma; 
                  end case;  
              end if;
              var_where:='';

                if nRate_cnt<>ForVar.rate_cnt then
                    nRate_cnt:=case when nRate_cnt=16 then 14 else nRate_cnt-1 end;
                end if;
                
              nSumma:=0;
              i:=1;
              --commit;
            
              if j=100 then
                  commit;
                  j:=1;
              end if;
              
          else
            i:=i+1;
            j:=j+1;
          end if;
           
          var_where:=case when nRate_cnt=ForVar.rate_cnt and var_where is not null then var_where||' OR ' else '' end||
          ' (r1 '||case when ForVar.rate_1=1 then '='''||ForRate.Rate_1||'''' else 'in ('''||case when ForRate.Rate_1='1' then '2'',''X'')' when ForRate.Rate_1='X' then '1'',''2'')' when ForRate.Rate_1='2' then '1'',''X'')' end end||
          ' and r2 '||case when ForVar.rate_2=1 then '='''||ForRate.Rate_2||'''' else 'in ('''||case when ForRate.Rate_2='1' then '2'',''X'')' when ForRate.Rate_2='X' then '1'',''2'')' when ForRate.Rate_2='2' then '1'',''X'')' end end||
          ' and r3 '||case when ForVar.rate_3=1 then '='''||ForRate.Rate_3||'''' else 'in ('''||case when ForRate.Rate_3='1' then '2'',''X'')' when ForRate.Rate_3='X' then '1'',''2'')' when ForRate.Rate_3='2' then '1'',''X'')' end end||
          ' and r4 '||case when ForVar.rate_4=1 then '='''||ForRate.Rate_4||'''' else 'in ('''||case when ForRate.Rate_4='1' then '2'',''X'')' when ForRate.Rate_4='X' then '1'',''2'')' when ForRate.Rate_4='2' then '1'',''X'')' end end||
          ' and r5 '||case when ForVar.rate_5=1 then '='''||ForRate.Rate_5||'''' else 'in ('''||case when ForRate.Rate_5='1' then '2'',''X'')' when ForRate.Rate_5='X' then '1'',''2'')' when ForRate.Rate_5='2' then '1'',''X'')' end end||
          ' and r6 '||case when ForVar.rate_6=1 then '='''||ForRate.Rate_6||'''' else 'in ('''||case when ForRate.Rate_6='1' then '2'',''X'')' when ForRate.Rate_6='X' then '1'',''2'')' when ForRate.Rate_6='2' then '1'',''X'')' end end||
          ' and r7 '||case when ForVar.rate_7=1 then '='''||ForRate.Rate_7||'''' else 'in ('''||case when ForRate.Rate_7='1' then '2'',''X'')' when ForRate.Rate_7='X' then '1'',''2'')' when ForRate.Rate_7='2' then '1'',''X'')' end end||
          ' and r8 '||case when ForVar.rate_8=1 then '='''||ForRate.Rate_8||'''' else 'in ('''||case when ForRate.Rate_8='1' then '2'',''X'')' when ForRate.Rate_8='X' then '1'',''2'')' when ForRate.Rate_8='2' then '1'',''X'')' end end||
          ' and r9 '||case when ForVar.rate_9=1 then '='''||ForRate.Rate_9||'''' else 'in ('''||case when ForRate.Rate_9='1' then '2'',''X'')' when ForRate.Rate_9='X' then '1'',''2'')' when ForRate.Rate_9='2' then '1'',''X'')' end end||
          ' and r10 '||case when ForVar.rate_10=1 then '='''||ForRate.Rate_10||'''' else 'in ('''||case when ForRate.Rate_10='1' then '2'',''X'')' when ForRate.Rate_10='X' then '1'',''2'')' when ForRate.Rate_10='2' then '1'',''X'')' end end||
          ' and r11 '||case when ForVar.rate_11=1 then '='''||ForRate.Rate_11||'''' else 'in ('''||case when ForRate.Rate_11='1' then '2'',''X'')' when ForRate.Rate_11='X' then '1'',''2'')' when ForRate.Rate_11='2' then '1'',''X'')' end end||
          ' and r12 '||case when ForVar.rate_12=1 then '='''||ForRate.Rate_12||'''' else 'in ('''||case when ForRate.Rate_12='1' then '2'',''X'')' when ForRate.Rate_12='X' then '1'',''2'')' when ForRate.Rate_12='2' then '1'',''X'')' end end||
          ' and r13 '||case when ForVar.rate_13=1 then '='''||ForRate.Rate_13||'''' else 'in ('''||case when ForRate.Rate_13='1' then '2'',''X'')' when ForRate.Rate_13='X' then '1'',''2'')' when ForRate.Rate_13='2' then '1'',''X'')' end end||
          ' and r14 '||case when ForVar.rate_14=1 then '='''||ForRate.Rate_14||'''' else 'in ('''||case when ForRate.Rate_14='1' then '2'',''X'')' when ForRate.Rate_14='X' then '1'',''2'')' when ForRate.Rate_14='2' then '1'',''X'')' end end||
          ' and r15 '||case when ForVar.rate_15=1 then '='''||ForRate.Rate_15||'''' else 'in ('''||case when ForRate.Rate_15='1' then '2'',''X'')' when ForRate.Rate_15='X' then '1'',''2'')' when ForRate.Rate_15='2' then '1'',''X'')' end end||')';
            
            --DBMS_OUTPUT.put_line(nRate_cnt||' '||ForVar.rate_cnt||' var_where='||var_where); 
          
      end loop;
 
      if i<16 and var_where is not null then
              var_idx:= 'select sum(summa)'||
              ' from toto_rates '||
              ' where '||var_where;            
              
              
              dDateStart2:= SYSTIMESTAMP;
              EXECUTE IMMEDIATE var_idx into nSumma;
              --логируем время выполнения запроса
              insert into toto_update_log (toto_script,start_date,rate_cnt,ucnt) values (var_idx,dDateStart2,nRate_cnt,nSumma);
              
             -- DBMS_OUTPUT.put_line(nRate_cnt||' '||ForVar.rate_cnt||' nSumma='||nSumma||' var_idx: '|| var_idx);
              
              if nSumma>0 then
                  case when nRate_cnt>=15 then
                            nSumSumma15:=nSumSumma15+nSumma;
                            nSumSumma14:=nSumSumma14+nSumma;
                            nSumSumma13:=nSumSumma13+nSumma;
                            nSumSumma12:=nSumSumma12+nSumma;
                            nSumSumma11:=nSumSumma11+nSumma;
                            nSumSumma10:=nSumSumma10+nSumma;
                            nSumSumma9:=nSumSumma9+nSumma;
                       when nRate_cnt=14 then
                            nSumSumma14:=nSumSumma14+nSumma;
                            nSumSumma13:=nSumSumma13+nSumma;
                            nSumSumma12:=nSumSumma12+nSumma;
                            nSumSumma11:=nSumSumma11+nSumma;
                            nSumSumma10:=nSumSumma10+nSumma;
                            nSumSumma9:=nSumSumma9+nSumma;
                    when nRate_cnt=13 then
                            nSumSumma13:=nSumSumma13+nSumma;
                            nSumSumma12:=nSumSumma12+nSumma;
                            nSumSumma11:=nSumSumma11+nSumma;
                            nSumSumma10:=nSumSumma10+nSumma;
                            nSumSumma9:=nSumSumma9+nSumma;
                    when nRate_cnt=12 then
                            nSumSumma12:=nSumSumma12+nSumma;
                            nSumSumma11:=nSumSumma11+nSumma;
                            nSumSumma10:=nSumSumma10+nSumma;
                            nSumSumma9:=nSumSumma9+nSumma;
                    when nRate_cnt=11 then
                            nSumSumma11:=nSumSumma11+nSumma;
                            nSumSumma10:=nSumSumma10+nSumma;
                            nSumSumma9:=nSumSumma9+nSumma;
                    when nRate_cnt=10 then
                            nSumSumma10:=nSumSumma10+nSumma;
                            nSumSumma9:=nSumSumma9+nSumma;
                    when nRate_cnt=9 then
                            nSumSumma9:=nSumSumma9+nSumma; 
                  end case;  
              end if;

      end if;   
            
            insert into toto_outcomes_tmp (RATE_1,RATE_2,RATE_3,RATE_4,RATE_5,
                                           RATE_6,RATE_7,RATE_8,RATE_9,RATE_10,
                                           RATE_11,RATE_12,RATE_13,RATE_14,RATE_15,
                                           sum9,sum10,sum11,sum12,sum13,sum14,sum15,date_start)
                            values (ForRate.Rate_1,ForRate.Rate_2,ForRate.Rate_3,ForRate.Rate_4,ForRate.Rate_5,
                                    ForRate.Rate_6,ForRate.Rate_7,ForRate.Rate_8,ForRate.Rate_9,ForRate.Rate_10,
                                    ForRate.Rate_11,ForRate.Rate_12,ForRate.Rate_13,ForRate.Rate_14,ForRate.Rate_15,
                                    nSumSumma9,nSumSumma10,nSumSumma11,nSumSumma12,nSumSumma13,nSumSumma14,nSumSumma15,dDateStart);               
                  
                commit; 
         
        
  end loop;
  commit; 
END;
/declare
  px1 toto_outcomes_ok.rate_1%type;   px1i toto_outcomes_ok.rate_1%type;
  px2 toto_outcomes_ok.rate_2%type;   px2i toto_outcomes_ok.rate_2%type;
  px3 toto_outcomes_ok.rate_3%type;   px3i toto_outcomes_ok.rate_3%type;
  px4 toto_outcomes_ok.rate_4%type;   px4i toto_outcomes_ok.rate_4%type;
  px5 toto_outcomes_ok.rate_5%type;   px5i toto_outcomes_ok.rate_5%type;
  px6 toto_outcomes_ok.rate_6%type;   px6i toto_outcomes_ok.rate_6%type;
  px7 toto_outcomes_ok.rate_7%type;   px7i toto_outcomes_ok.rate_7%type;
  px8 toto_outcomes_ok.rate_8%type;   px8i toto_outcomes_ok.rate_8%type;
  px9 toto_outcomes_ok.rate_9%type;   px9i toto_outcomes_ok.rate_9%type;
  px10 toto_outcomes_ok.rate_10%type; px10i toto_outcomes_ok.rate_10%type;
  px11 toto_outcomes_ok.rate_11%type; px11i toto_outcomes_ok.rate_11%type;
  px12 toto_outcomes_ok.rate_12%type; px12i toto_outcomes_ok.rate_12%type;
  px13 toto_outcomes_ok.rate_13%type; px13i toto_outcomes_ok.rate_13%type;
  px14 toto_outcomes_ok.rate_14%type; px14i toto_outcomes_ok.rate_14%type;
  px15 toto_outcomes_ok.rate_15%type; px15i toto_outcomes_ok.rate_15%type;
  
  ps15 toto_outcomes_ok.sum15%type:=0;
  ps14 toto_outcomes_ok.sum14%type:=0;
  ps13 toto_outcomes_ok.sum13%type:=0;
  ps12 toto_outcomes_ok.sum12%type:=0;
  ps11 toto_outcomes_ok.sum11%type:=0;
  ps10 toto_outcomes_ok.sum10%type:=0;
  ps9 toto_outcomes_ok.sum9%type:=0;  
  
  TYPE t_idx IS TABLE OF toto_idx%ROWTYPE;
  col_TOTO_IDX t_idx; -- Коолекция для таблицы toto_idx. 
  
  TYPE t_rates IS TABLE OF toto_rates_oic%ROWTYPE;
  col_TOTO_RATES t_rates; -- Коолекция для таблицы toto_idx. 
    
  var_idx varchar2(4000);
  dDateStart timestamp(5);
  dDateStart2 timestamp(5);
  l number:=1;
  nType number:=1; --1 - делае update  без коллекций, 2 - update через коллекцию
BEGIN
  select * BULK COLLECT INTO col_TOTO_RATES from toto_rates_oic where cnt>=100 and summa>=0 and rownum<2;
   
  --for i in 1 .. col_toto_rates_rowid.count loop
  FOR i IN col_TOTO_RATES.FIRST..col_TOTO_RATES.LAST loop
    dDateStart:= SYSTIMESTAMP;   
    ps15:=0;ps14:=0;ps13:=0;ps12:=0;ps11:=0;ps10:=0;ps9:=0;
    
    select * BULK COLLECT INTO col_TOTO_IDX from toto_idx order by rate_cnt desc;
               
    FOR j IN col_TOTO_IDX.FIRST..col_TOTO_IDX.LAST loop
      px1:=case when col_TOTO_IDX(j).RATE_1=1 then col_TOTO_RATES(i).R1 else case when col_TOTO_RATES(i).r1='1' then '2' when col_TOTO_RATES(i).r1='X' then '1' when col_TOTO_RATES(i).r1='2' then '1' end end;
      px1i:=case when col_TOTO_IDX(j).RATE_1=1 then col_TOTO_RATES(i).r1 else case when col_TOTO_RATES(i).r1='1' then 'X' when col_TOTO_RATES(i).r1='X' then '2' when col_TOTO_RATES(i).r1='2' then 'X' end end; 
      px2:=case when col_TOTO_IDX(j).RATE_2=1 then col_TOTO_RATES(i).r2 else case when col_TOTO_RATES(i).r2='1' then '2' when col_TOTO_RATES(i).r2='X' then '1' when col_TOTO_RATES(i).r2='2' then '1' end end;
      px2i:=case when col_TOTO_IDX(j).RATE_2=1 then col_TOTO_RATES(i).r2 else case when col_TOTO_RATES(i).r2='1' then 'X' when col_TOTO_RATES(i).r2='X' then '2' when col_TOTO_RATES(i).r2='2' then 'X' end end; 
      px3:=case when col_TOTO_IDX(j).RATE_3=1 then col_TOTO_RATES(i).r3 else case when col_TOTO_RATES(i).r3='1' then '2' when col_TOTO_RATES(i).r3='X' then '1' when col_TOTO_RATES(i).r3='2' then '1' end end;
      px3i:=case when col_TOTO_IDX(j).RATE_3=1 then col_TOTO_RATES(i).r3 else case when col_TOTO_RATES(i).r3='1' then 'X' when col_TOTO_RATES(i).r3='X' then '2' when col_TOTO_RATES(i).r3='2' then 'X' end end; 
      px4:=case when col_TOTO_IDX(j).RATE_4=1 then col_TOTO_RATES(i).r4 else case when col_TOTO_RATES(i).r4='1' then '2' when col_TOTO_RATES(i).r4='X' then '1' when col_TOTO_RATES(i).r4='2' then '1' end end;
      px4i:=case when col_TOTO_IDX(j).RATE_4=1 then col_TOTO_RATES(i).r4 else case when col_TOTO_RATES(i).r4='1' then 'X' when col_TOTO_RATES(i).r4='X' then '2' when col_TOTO_RATES(i).r4='2' then 'X' end end; 
      px5:=case when col_TOTO_IDX(j).RATE_5=1 then col_TOTO_RATES(i).r5 else case when col_TOTO_RATES(i).r5='1' then '2' when col_TOTO_RATES(i).r5='X' then '1' when col_TOTO_RATES(i).r5='2' then '1' end end;
      px5i:=case when col_TOTO_IDX(j).RATE_5=1 then col_TOTO_RATES(i).r5 else case when col_TOTO_RATES(i).r5='1' then 'X' when col_TOTO_RATES(i).r5='X' then '2' when col_TOTO_RATES(i).r5='2' then 'X' end end; 
      px6:=case when col_TOTO_IDX(j).RATE_6=1 then col_TOTO_RATES(i).r6 else case when col_TOTO_RATES(i).r6='1' then '2' when col_TOTO_RATES(i).r6='X' then '1' when col_TOTO_RATES(i).r6='2' then '1' end end;
      px6i:=case when col_TOTO_IDX(j).RATE_6=1 then col_TOTO_RATES(i).r6 else case when col_TOTO_RATES(i).r6='1' then 'X' when col_TOTO_RATES(i).r6='X' then '2' when col_TOTO_RATES(i).r6='2' then 'X' end end; 
      px7:=case when col_TOTO_IDX(j).RATE_7=1 then col_TOTO_RATES(i).r7 else case when col_TOTO_RATES(i).r7='1' then '2' when col_TOTO_RATES(i).r7='X' then '1' when col_TOTO_RATES(i).r7='2' then '1' end end;
      px7i:=case when col_TOTO_IDX(j).RATE_7=1 then col_TOTO_RATES(i).r7 else case when col_TOTO_RATES(i).r7='1' then 'X' when col_TOTO_RATES(i).r7='X' then '2' when col_TOTO_RATES(i).r7='2' then 'X' end end; 
      px8:=case when col_TOTO_IDX(j).RATE_8=1 then col_TOTO_RATES(i).r8 else case when col_TOTO_RATES(i).r8='1' then '2' when col_TOTO_RATES(i).r8='X' then '1' when col_TOTO_RATES(i).r8='2' then '1' end end;
      px8i:=case when col_TOTO_IDX(j).RATE_8=1 then col_TOTO_RATES(i).r8 else case when col_TOTO_RATES(i).r8='1' then 'X' when col_TOTO_RATES(i).r8='X' then '2' when col_TOTO_RATES(i).r8='2' then 'X' end end; 
      px9:=case when col_TOTO_IDX(j).RATE_9=1 then col_TOTO_RATES(i).r9 else case when col_TOTO_RATES(i).r9='1' then '2' when col_TOTO_RATES(i).r9='X' then '1' when col_TOTO_RATES(i).r9='2' then '1' end end;
      px9i:=case when col_TOTO_IDX(j).RATE_9=1 then col_TOTO_RATES(i).r9 else case when col_TOTO_RATES(i).r9='1' then 'X' when col_TOTO_RATES(i).r9='X' then '2' when col_TOTO_RATES(i).r9='2' then 'X' end end; 
      px10:=case when col_TOTO_IDX(j).RATE_10=1 then col_TOTO_RATES(i).r10 else case when col_TOTO_RATES(i).r10='1' then '2' when col_TOTO_RATES(i).r10='X' then '1' when col_TOTO_RATES(i).r10='2' then '1' end end;
      px10i:=case when col_TOTO_IDX(j).RATE_10=1 then col_TOTO_RATES(i).r10 else case when col_TOTO_RATES(i).r10='1' then 'X' when col_TOTO_RATES(i).r10='X' then '2' when col_TOTO_RATES(i).r10='2' then 'X' end end; 
      px11:=case when col_TOTO_IDX(j).RATE_11=1 then col_TOTO_RATES(i).r11 else case when col_TOTO_RATES(i).r11='1' then '2' when col_TOTO_RATES(i).r11='X' then '1' when col_TOTO_RATES(i).r11='2' then '1' end end;
      px11i:=case when col_TOTO_IDX(j).RATE_11=1 then col_TOTO_RATES(i).r11 else case when col_TOTO_RATES(i).r11='1' then 'X' when col_TOTO_RATES(i).r11='X' then '2' when col_TOTO_RATES(i).r11='2' then 'X' end end; 
      px12:=case when col_TOTO_IDX(j).RATE_12=1 then col_TOTO_RATES(i).r12 else case when col_TOTO_RATES(i).r12='1' then '2' when col_TOTO_RATES(i).r12='X' then '1' when col_TOTO_RATES(i).r12='2' then '1' end end;
      px12i:=case when col_TOTO_IDX(j).RATE_12=1 then col_TOTO_RATES(i).r12 else case when col_TOTO_RATES(i).r12='1' then 'X' when col_TOTO_RATES(i).r12='X' then '2' when col_TOTO_RATES(i).r12='2' then 'X' end end; 
      px13:=case when col_TOTO_IDX(j).RATE_13=1 then col_TOTO_RATES(i).r13 else case when col_TOTO_RATES(i).r13='1' then '2' when col_TOTO_RATES(i).r13='X' then '1' when col_TOTO_RATES(i).r13='2' then '1' end end;
      px13i:=case when col_TOTO_IDX(j).RATE_13=1 then col_TOTO_RATES(i).r13 else case when col_TOTO_RATES(i).r13='1' then 'X' when col_TOTO_RATES(i).r13='X' then '2' when col_TOTO_RATES(i).r13='2' then 'X' end end; 
      px14:=case when col_TOTO_IDX(j).RATE_14=1 then col_TOTO_RATES(i).r14 else case when col_TOTO_RATES(i).r14='1' then '2' when col_TOTO_RATES(i).r14='X' then '1' when col_TOTO_RATES(i).r14='2' then '1' end end;
      px14i:=case when col_TOTO_IDX(j).RATE_14=1 then col_TOTO_RATES(i).r14 else case when col_TOTO_RATES(i).r14='1' then 'X' when col_TOTO_RATES(i).r14='X' then '2' when col_TOTO_RATES(i).r14='2' then 'X' end end; 
      px15:=case when col_TOTO_IDX(j).RATE_15=1 then col_TOTO_RATES(i).r15 else case when col_TOTO_RATES(i).r15='1' then '2' when col_TOTO_RATES(i).r15='X' then '1' when col_TOTO_RATES(i).r15='2' then '1' end end;
      px15i:=case when col_TOTO_IDX(j).RATE_15=1 then col_TOTO_RATES(i).r15 else case when col_TOTO_RATES(i).r15='1' then 'X' when col_TOTO_RATES(i).r15='X' then '2' when col_TOTO_RATES(i).r15='2' then 'X' end end; 
                    
      ps15:=case when col_TOTO_IDX(j).RATE_cnt=15 then col_TOTO_RATES(i).SUMMA else 0 end;
      ps14:=case when col_TOTO_IDX(j).RATE_cnt in (14,15) then col_TOTO_RATES(i).SUMMA else 0 end;
      ps13:=case when col_TOTO_IDX(j).RATE_cnt in (13,14,15) then col_TOTO_RATES(i).SUMMA else 0 end;
      ps12:=case when col_TOTO_IDX(j).RATE_cnt in (12,13,14,15) then col_TOTO_RATES(i).SUMMA else 0 end;
      ps11:=case when col_TOTO_IDX(j).RATE_cnt in (11,12,13,14,15) then col_TOTO_RATES(i).SUMMA else 0 end;
      ps10:=case when col_TOTO_IDX(j).RATE_cnt in (10,11,12,13,14,15) then col_TOTO_RATES(i).SUMMA else 0 end;
      ps9 :=case when col_TOTO_IDX(j).RATE_cnt in (9,10,11,12,13,14,15) then col_TOTO_RATES(i).SUMMA else 0 end;
      
      dDateStart2:= SYSTIMESTAMP;  
      if nType=1 then
        --через запрос без коллеций                            
        var_idx:= 'update toto_outcomes_ok set '
          ||'sum15=sum15+:1'
          ||', sum14=sum14+:2'
          ||', sum13=sum13+:3'
          ||', sum12=sum12+:4'
          ||', sum11=sum11+:5'
          ||', sum10=sum10+:6'
          ||', sum9=sum9+:7'
          ||', upd_status=upd_status+1 '
          ||' where (rate_1=:8 or rate_1=:9)
              and (rate_2=:10 or rate_2=:11)
              and (rate_3=:12 or rate_3=:13)
              and (rate_4=:14 or rate_4=:15)
              and (rate_5=:16 or rate_5=:17)
              and (rate_6=:18 or rate_6=:19)
              and (rate_7=:20 or rate_7=:21)
              and (rate_8=:22 or rate_8=:23)
              and (rate_9=:23 or rate_9=:25)
              and (rate_10=:24 or rate_10=:27)
              and (rate_11=:26 or rate_11=:29)
              and (rate_12=:28 or rate_12=:31)
              and (rate_13=:30 or rate_13=:33)
              and (rate_14=:32 or rate_14=:35)
              and (rate_15=:34 or rate_15=:37)';
        
            EXECUTE IMMEDIATE var_idx 
            using ps15,ps14,ps13,ps12,ps11,ps10,ps9,px1,px1i,px2,px2i,px3,px3i,px4,px4i,px5,px5i,
                  px6,px6i,px7,px7i,px8,px8i,px9,px9i,px10,px10i,
                  px11,px11i,px12,px12i,px13,px13i,px14,px14i,px15,px15i; 
      else  
        update toto_outcomes_ok tar
            set sum15=sum15 + ps15
              , sum14=sum14 + ps14
              , sum13=sum13 + ps13
              , sum12=sum12 + ps12
              , sum11=sum11 + ps11
              , sum10=sum10 + ps10
              , sum9=sum9 + ps9
              , upd_status = upd_status+1 
            where (tar.rate_1=px1 or tar.rate_1=px1i)
                 and (tar.rate_2=px2 or tar.rate_2=px2i)
                 and (tar.rate_3=px3 or tar.rate_3=px3i)
                 and (tar.rate_4=px4 or tar.rate_4=px4i)
                 and (tar.rate_5=px5 or tar.rate_5=px5i)
                 and (tar.rate_6=px6 or tar.rate_6=px6i)
                 and (tar.rate_7=px7 or tar.rate_7=px7i)
                 and (tar.rate_8=px8 or tar.rate_8=px8i)
                 and (tar.rate_9=px9 or tar.rate_9=px9i)
                 and (tar.rate_10=px10 or tar.rate_10=px10i)
                 and (tar.rate_11=px11 or tar.rate_11=px11i)
                 and (tar.rate_12=px12 or tar.rate_12=px12i)
                 and (tar.rate_13=px13 or tar.rate_13=px13i)
                 and (tar.rate_14=px14 or tar.rate_14=px14i)
                 and (tar.rate_15=px15 or tar.rate_15=px15i)
                 ;
      end if;
          
      --ниже это просто для лога чтобы логировать сам скрипт запроса
      var_idx:= 'update toto_outcomes_ok 
        set sum15=sum15+'||ps15||
        ', sum14=sum14+'||ps14||
        ', sum13=sum13+'||ps13||
        ', sum12=sum12+'||ps12||
        ', sum11=sum11+'||ps11||
         ', sum10=sum10+'||ps10||
        ', sum9=sum9+'||ps9||
        ', upd_status=upd_status+1 
        where (rate_1='''||px1||''' or rate_1='''||px1i||''')
        and (rate_2='''||px2||''' or rate_2='''||px2i||''')
        and (rate_3='''||px3||''' or rate_3='''||px3i||''')
        and (rate_4='''||px4||''' or rate_4='''||px4i||''')
        and (rate_5='''||px5||''' or rate_5='''||px5i||''')
        and (rate_6='''||px6||''' or rate_6='''||px6i||''')
        and (rate_7='''||px7||''' or rate_7='''||px7i||''')
        and (rate_8='''||px8||''' or rate_8='''||px8i||''')
        and (rate_9='''||px9||''' or rate_9='''||px9i||''')
        and (rate_10='''||px10||''' or rate_10='''||px10i||''')
        and (rate_11='''||px11||''' or rate_11='''||px11i||''')
        and (rate_12='''||px12||''' or rate_12='''||px12i||''')
        and (rate_13='''||px13||''' or rate_13='''||px13i||''')
        and (rate_14='''||px14||''' or rate_14='''||px14i||''')
        and (rate_15='''||px15||''' or rate_15='''||px15i||''')'; 
                 
      insert into toto_update_log (toto_script,start_date,rate_cnt) values (var_idx,dDateStart2, col_TOTO_IDX(j).RATE_cnt);
      
      l:=l+1;
      if l=300 then
        commit;
        l:=1;
      end if;
          
      --commit;
    end loop;
    
    --это тоже просто лого
    insert into toto_outcomes_tmp (RATE_1,RATE_2,RATE_3,RATE_4,RATE_5,
                               RATE_6,RATE_7,RATE_8,RATE_9,RATE_10,
                               RATE_11,RATE_12,RATE_13,RATE_14,RATE_15,date_start)
                values (col_TOTO_RATES(i).r1,col_TOTO_RATES(i).r2,col_TOTO_RATES(i).r3,col_TOTO_RATES(i).r4,col_TOTO_RATES(i).r5,
                        col_TOTO_RATES(i).r6,col_TOTO_RATES(i).r7,col_TOTO_RATES(i).r8,col_TOTO_RATES(i).r9,col_TOTO_RATES(i).r10,
                        col_TOTO_RATES(i).r11,col_TOTO_RATES(i).r12,col_TOTO_RATES(i).r13,col_TOTO_RATES(i).r14,col_TOTO_RATES(i).r15,dDateStart);               
    
    commit;        
  end loop;
  --close c_toto_rates_oic;
  commit; 
END;
/declare
  px1 toto_outcomes_ok.rate_1%type;   px1i toto_outcomes_ok.rate_1%type;
  px2 toto_outcomes_ok.rate_2%type;   px2i toto_outcomes_ok.rate_2%type;
  px3 toto_outcomes_ok.rate_3%type;   px3i toto_outcomes_ok.rate_3%type;
  px4 toto_outcomes_ok.rate_4%type;   px4i toto_outcomes_ok.rate_4%type;
  px5 toto_outcomes_ok.rate_5%type;   px5i toto_outcomes_ok.rate_5%type;
  px6 toto_outcomes_ok.rate_6%type;   px6i toto_outcomes_ok.rate_6%type;
  px7 toto_outcomes_ok.rate_7%type;   px7i toto_outcomes_ok.rate_7%type;
  px8 toto_outcomes_ok.rate_8%type;   px8i toto_outcomes_ok.rate_8%type;
  px9 toto_outcomes_ok.rate_9%type;   px9i toto_outcomes_ok.rate_9%type;
  px10 toto_outcomes_ok.rate_10%type; px10i toto_outcomes_ok.rate_10%type;
  px11 toto_outcomes_ok.rate_11%type; px11i toto_outcomes_ok.rate_11%type;
  px12 toto_outcomes_ok.rate_12%type; px12i toto_outcomes_ok.rate_12%type;
  px13 toto_outcomes_ok.rate_13%type; px13i toto_outcomes_ok.rate_13%type;
  px14 toto_outcomes_ok.rate_14%type; px14i toto_outcomes_ok.rate_14%type;
  px15 toto_outcomes_ok.rate_15%type; px15i toto_outcomes_ok.rate_15%type;
  
  ps15 toto_outcomes_ok.sum15%type:=0;
  ps14 toto_outcomes_ok.sum14%type:=0;
  ps13 toto_outcomes_ok.sum13%type:=0;
  ps12 toto_outcomes_ok.sum12%type:=0;
  ps11 toto_outcomes_ok.sum11%type:=0;
  ps10 toto_outcomes_ok.sum10%type:=0;
  ps9 toto_outcomes_ok.sum9%type:=0;  
  
  TYPE t_idx IS TABLE OF toto_idx%ROWTYPE;
  col_TOTO_IDX t_idx; -- Коолекция для таблицы toto_idx. 
  
  TYPE t_rates IS TABLE OF toto_rates_oic%ROWTYPE;
  col_TOTO_RATES t_rates; -- Коолекция для таблицы toto_idx. 
    
  var_idx varchar2(4000);
  dDateStart timestamp(5);
  dDateStart2 timestamp(5);
  l number:=1;
  nType number:=1; --1 - делае update  без коллекций, 2 - update через коллекцию
BEGIN
  select * BULK COLLECT INTO col_TOTO_RATES from toto_rates_oic where cnt>=100 and rownum<2;
   
  FOR i IN col_TOTO_RATES.FIRST..col_TOTO_RATES.LAST loop
    dDateStart:= SYSTIMESTAMP;   
    --ps15:=0;ps14:=0;ps13:=0;ps12:=0;ps11:=0;ps10:=0;ps9:=0;
    
    select * BULK COLLECT INTO col_TOTO_IDX from toto_idx order by rate_cnt desc, date_ins desc
                                                                  --rate_1, rate_2, rate_3, rate_4, rate_5, rate_6, rate_7, rate_8
                                                                  --,rate_9, rate_10, rate_11, rate_12, rate_13, rate_14, rate_15
                                                                  ;
               
    FORALL j IN col_TOTO_IDX.FIRST..col_TOTO_IDX.LAST
        update toto_outcomes_ok tar
            set sum15=sum15 + case when col_TOTO_IDX(j).RATE_cnt=15 then col_TOTO_RATES(i).SUMMA else 0 end
              , sum14=sum14 + case when col_TOTO_IDX(j).RATE_cnt in (14,15) then col_TOTO_RATES(i).SUMMA else 0 end
              , sum13=sum13 + case when col_TOTO_IDX(j).RATE_cnt in (13,14,15) then col_TOTO_RATES(i).SUMMA else 0 end
              , sum12=sum12 + case when col_TOTO_IDX(j).RATE_cnt in (12,13,14,15) then col_TOTO_RATES(i).SUMMA else 0 end
              , sum11=sum11 + case when col_TOTO_IDX(j).RATE_cnt in (11,12,13,14,15) then col_TOTO_RATES(i).SUMMA else 0 end
              , sum10=sum10 + case when col_TOTO_IDX(j).RATE_cnt in (10,11,12,13,14,15) then col_TOTO_RATES(i).SUMMA else 0 end
              , sum9=sum9 + case when col_TOTO_IDX(j).RATE_cnt in (9,10,11,12,13,14,15) then col_TOTO_RATES(i).SUMMA else 0 end
              , upd_status = upd_status+1 
            where (tar.rate_1=(case when col_TOTO_IDX(j).RATE_1=1 then col_TOTO_RATES(i).R1 else case when col_TOTO_RATES(i).r1='1' then '2' when col_TOTO_RATES(i).r1='X' then '1' when col_TOTO_RATES(i).r1='2' then '1' end end) 
                    or tar.rate_1=(case when col_TOTO_IDX(j).RATE_1=1 then col_TOTO_RATES(i).r1 else case when col_TOTO_RATES(i).r1='1' then 'X' when col_TOTO_RATES(i).r1='X' then '2' when col_TOTO_RATES(i).r1='2' then 'X' end end))
                 and (tar.rate_2=(case when col_TOTO_IDX(j).RATE_2=1 then col_TOTO_RATES(i).r2 else case when col_TOTO_RATES(i).r2='1' then '2' when col_TOTO_RATES(i).r2='X' then '1' when col_TOTO_RATES(i).r2='2' then '1' end end)
                    or tar.rate_2=(case when col_TOTO_IDX(j).RATE_2=1 then col_TOTO_RATES(i).r2 else case when col_TOTO_RATES(i).r2='1' then '2' when col_TOTO_RATES(i).r2='X' then '1' when col_TOTO_RATES(i).r2='2' then '1' end end))
                 and (tar.rate_3=(case when col_TOTO_IDX(j).RATE_3=1 then col_TOTO_RATES(i).r3 else case when col_TOTO_RATES(i).r3='1' then '2' when col_TOTO_RATES(i).r3='X' then '1' when col_TOTO_RATES(i).r3='2' then '1' end end) 
                    or tar.rate_3=(case when col_TOTO_IDX(j).RATE_3=1 then col_TOTO_RATES(i).r3 else case when col_TOTO_RATES(i).r3='1' then 'X' when col_TOTO_RATES(i).r3='X' then '2' when col_TOTO_RATES(i).r3='2' then 'X' end end))
                 and (tar.rate_4=(case when col_TOTO_IDX(j).RATE_4=1 then col_TOTO_RATES(i).r4 else case when col_TOTO_RATES(i).r4='1' then '2' when col_TOTO_RATES(i).r4='X' then '1' when col_TOTO_RATES(i).r4='2' then '1' end end) 
                    or tar.rate_4=(case when col_TOTO_IDX(j).RATE_4=1 then col_TOTO_RATES(i).r4 else case when col_TOTO_RATES(i).r4='1' then 'X' when col_TOTO_RATES(i).r4='X' then '2' when col_TOTO_RATES(i).r4='2' then 'X' end end))
                 and (tar.rate_5=(case when col_TOTO_IDX(j).RATE_5=1 then col_TOTO_RATES(i).r5 else case when col_TOTO_RATES(i).r5='1' then '2' when col_TOTO_RATES(i).r5='X' then '1' when col_TOTO_RATES(i).r5='2' then '1' end end) 
                    or tar.rate_5=(case when col_TOTO_IDX(j).RATE_5=1 then col_TOTO_RATES(i).r5 else case when col_TOTO_RATES(i).r5='1' then 'X' when col_TOTO_RATES(i).r5='X' then '2' when col_TOTO_RATES(i).r5='2' then 'X' end end))
                 and (tar.rate_6=(case when col_TOTO_IDX(j).RATE_6=1 then col_TOTO_RATES(i).r6 else case when col_TOTO_RATES(i).r6='1' then '2' when col_TOTO_RATES(i).r6='X' then '1' when col_TOTO_RATES(i).r6='2' then '1' end end) 
                    or tar.rate_6=(case when col_TOTO_IDX(j).RATE_6=1 then col_TOTO_RATES(i).r6 else case when col_TOTO_RATES(i).r6='1' then 'X' when col_TOTO_RATES(i).r6='X' then '2' when col_TOTO_RATES(i).r6='2' then 'X' end end))
                 and (tar.rate_7=(case when col_TOTO_IDX(j).RATE_7=1 then col_TOTO_RATES(i).r7 else case when col_TOTO_RATES(i).r7='1' then '2' when col_TOTO_RATES(i).r7='X' then '1' when col_TOTO_RATES(i).r7='2' then '1' end end) 
                    or tar.rate_7=(case when col_TOTO_IDX(j).RATE_7=1 then col_TOTO_RATES(i).r7 else case when col_TOTO_RATES(i).r7='1' then 'X' when col_TOTO_RATES(i).r7='X' then '2' when col_TOTO_RATES(i).r7='2' then 'X' end end))
                 and (tar.rate_8=(case when col_TOTO_IDX(j).RATE_8=1 then col_TOTO_RATES(i).r8 else case when col_TOTO_RATES(i).r8='1' then '2' when col_TOTO_RATES(i).r8='X' then '1' when col_TOTO_RATES(i).r8='2' then '1' end end) 
                    or tar.rate_8=(case when col_TOTO_IDX(j).RATE_8=1 then col_TOTO_RATES(i).r8 else case when col_TOTO_RATES(i).r8='1' then 'X' when col_TOTO_RATES(i).r8='X' then '2' when col_TOTO_RATES(i).r8='2' then 'X' end end))
                 and (tar.rate_9=(case when col_TOTO_IDX(j).RATE_9=1 then col_TOTO_RATES(i).r9 else case when col_TOTO_RATES(i).r9='1' then '2' when col_TOTO_RATES(i).r9='X' then '1' when col_TOTO_RATES(i).r9='2' then '1' end end) 
                    or tar.rate_9=(case when col_TOTO_IDX(j).RATE_9=1 then col_TOTO_RATES(i).r9 else case when col_TOTO_RATES(i).r9='1' then 'X' when col_TOTO_RATES(i).r9='X' then '2' when col_TOTO_RATES(i).r9='2' then 'X' end end))
                 and (tar.rate_10=(case when col_TOTO_IDX(j).RATE_10=1 then col_TOTO_RATES(i).r10 else case when col_TOTO_RATES(i).r10='1' then '2' when col_TOTO_RATES(i).r10='X' then '1' when col_TOTO_RATES(i).r10='2' then '1' end end) 
                    or tar.rate_10=(case when col_TOTO_IDX(j).RATE_10=1 then col_TOTO_RATES(i).r10 else case when col_TOTO_RATES(i).r10='1' then 'X' when col_TOTO_RATES(i).r10='X' then '2' when col_TOTO_RATES(i).r10='2' then 'X' end end))
                 and (tar.rate_11=(case when col_TOTO_IDX(j).RATE_11=1 then col_TOTO_RATES(i).r11 else case when col_TOTO_RATES(i).r11='1' then '2' when col_TOTO_RATES(i).r11='X' then '1' when col_TOTO_RATES(i).r11='2' then '1' end end) 
                    or tar.rate_11=(case when col_TOTO_IDX(j).RATE_11=1 then col_TOTO_RATES(i).r11 else case when col_TOTO_RATES(i).r11='1' then 'X' when col_TOTO_RATES(i).r11='X' then '2' when col_TOTO_RATES(i).r11='2' then 'X' end end))
                 and (tar.rate_12=(case when col_TOTO_IDX(j).RATE_12=1 then col_TOTO_RATES(i).r12 else case when col_TOTO_RATES(i).r12='1' then '2' when col_TOTO_RATES(i).r12='X' then '1' when col_TOTO_RATES(i).r12='2' then '1' end end) 
                    or tar.rate_12=(case when col_TOTO_IDX(j).RATE_12=1 then col_TOTO_RATES(i).r12 else case when col_TOTO_RATES(i).r12='1' then 'X' when col_TOTO_RATES(i).r12='X' then '2' when col_TOTO_RATES(i).r12='2' then 'X' end end))
                 and (tar.rate_13=(case when col_TOTO_IDX(j).RATE_13=1 then col_TOTO_RATES(i).r13 else case when col_TOTO_RATES(i).r13='1' then '2' when col_TOTO_RATES(i).r13='X' then '1' when col_TOTO_RATES(i).r13='2' then '1' end end) 
                    or tar.rate_13=(case when col_TOTO_IDX(j).RATE_13=1 then col_TOTO_RATES(i).r13 else case when col_TOTO_RATES(i).r13='1' then 'X' when col_TOTO_RATES(i).r13='X' then '2' when col_TOTO_RATES(i).r13='2' then 'X' end end))
                 and (tar.rate_14=(case when col_TOTO_IDX(j).RATE_14=1 then col_TOTO_RATES(i).r14 else case when col_TOTO_RATES(i).r14='1' then '2' when col_TOTO_RATES(i).r14='X' then '1' when col_TOTO_RATES(i).r14='2' then '1' end end) 
                    or tar.rate_14=(case when col_TOTO_IDX(j).RATE_14=1 then col_TOTO_RATES(i).r14 else case when col_TOTO_RATES(i).r14='1' then 'X' when col_TOTO_RATES(i).r14='X' then '2' when col_TOTO_RATES(i).r14='2' then 'X' end end))
                 and (tar.rate_15=(case when col_TOTO_IDX(j).RATE_15=1 then col_TOTO_RATES(i).r15 else case when col_TOTO_RATES(i).r15='1' then '2' when col_TOTO_RATES(i).r15='X' then '1' when col_TOTO_RATES(i).r15='2' then '1' end end) 
                    or tar.rate_15=(case when col_TOTO_IDX(j).RATE_15=1 then col_TOTO_RATES(i).r15 else case when col_TOTO_RATES(i).r15='1' then 'X' when col_TOTO_RATES(i).r15='X' then '2' when col_TOTO_RATES(i).r15='2' then 'X' end end))
                 ;
    
    --это тоже просто лого
    insert into toto_outcomes_tmp (RATE_1,RATE_2,RATE_3,RATE_4,RATE_5,
                               RATE_6,RATE_7,RATE_8,RATE_9,RATE_10,
                               RATE_11,RATE_12,RATE_13,RATE_14,RATE_15,date_start)
                values (col_TOTO_RATES(i).r1,col_TOTO_RATES(i).r2,col_TOTO_RATES(i).r3,col_TOTO_RATES(i).r4,col_TOTO_RATES(i).r5,
                        col_TOTO_RATES(i).r6,col_TOTO_RATES(i).r7,col_TOTO_RATES(i).r8,col_TOTO_RATES(i).r9,col_TOTO_RATES(i).r10,
                        col_TOTO_RATES(i).r11,col_TOTO_RATES(i).r12,col_TOTO_RATES(i).r13,col_TOTO_RATES(i).r14,col_TOTO_RATES(i).r15,dDateStart);               
    
    commit; 
    DBMS_SESSION.FREE_UNUSED_USER_MEMORY;
  end loop;
  commit; 
END;
/declare
  TYPE t_idx IS TABLE OF toto_idx%ROWTYPE;
  col_TOTO_IDX t_idx; -- Коолекция для таблицы toto_idx. 
  
  TYPE t_rates IS TABLE OF toto_rates_oic%ROWTYPE;
  col_TOTO_RATES t_rates; -- Коолекция для таблицы toto_idx. 
    
  var_idx varchar2(4000);
  dDateStart timestamp(5);
  dDateStart2 timestamp(5);
  l number:=1;
  nType number:=1; --1 - делае update  без коллекций, 2 - update через коллекцию
BEGIN
  select * BULK COLLECT INTO col_TOTO_IDX from toto_idx where rownum<2
                          --rate_1, rate_2, rate_3, rate_4, rate_5, rate_6, rate_7, rate_8
                                                                --,rate_9, rate_10, rate_11, rate_12, rate_13, rate_14, rate_15
                                          order by rate_cnt desc, date_ins desc
                                                                --rate_1, rate_2, rate_3, rate_4, rate_5, rate_6, rate_7, rate_8
                                                                --,rate_9, rate_10, rate_11, rate_12, rate_13, rate_14, rate_15
                                                                ;
  FOR j IN col_TOTO_IDX.FIRST..col_TOTO_IDX.LAST LOOP
    dDateStart:= SYSTIMESTAMP;
    select * BULK COLLECT INTO col_TOTO_RATES from toto_rates_oic --where cnt>=10 --and rownum<20000
                                  --where r1='1' and r2='1' and r3='1' and r4='1' and r5='1' and r6='1'
                                  --   and r7='1' and r8='1' and r9='1' and r10='1' and r11='1'
                                  --   and r12='1' and r13='1' and r14='1' and r15='1'
                                  order by r1, r2, r3, r4, r5, r6, r7, r8, r9, r10, r11, r12, r13, r14, r15;
    
    FORALL i IN col_TOTO_RATES.FIRST..col_TOTO_RATES.LAST
        update toto_outcomes_ok tar
            set sum15=sum15 + case when col_TOTO_IDX(j).RATE_cnt=15 then col_TOTO_RATES(i).SUMMA else 0 end
              , sum14=sum14 + case when col_TOTO_IDX(j).RATE_cnt>=14 then col_TOTO_RATES(i).SUMMA else 0 end
              , sum13=sum13 + case when col_TOTO_IDX(j).RATE_cnt>=13 then col_TOTO_RATES(i).SUMMA else 0 end
              , sum12=sum12 + case when col_TOTO_IDX(j).RATE_cnt>=12 then col_TOTO_RATES(i).SUMMA else 0 end
              , sum11=sum11 + case when col_TOTO_IDX(j).RATE_cnt>=11 then col_TOTO_RATES(i).SUMMA else 0 end
              , sum10=sum10 + case when col_TOTO_IDX(j).RATE_cnt>=10 then col_TOTO_RATES(i).SUMMA else 0 end
              , sum9=sum9 + case when col_TOTO_IDX(j).RATE_cnt>=9 then col_TOTO_RATES(i).SUMMA else 0 end
              , upd_status = upd_status+1 
            where (tar.rate_1=(case when col_TOTO_IDX(j).RATE_1=1 then col_TOTO_RATES(i).R1 else case when col_TOTO_RATES(i).r1='1' then '2' when col_TOTO_RATES(i).r1='X' then '1' when col_TOTO_RATES(i).r1='2' then '1' end end) 
                    or tar.rate_1=(case when col_TOTO_IDX(j).RATE_1=1 then col_TOTO_RATES(i).r1 else case when col_TOTO_RATES(i).r1='1' then 'X' when col_TOTO_RATES(i).r1='X' then '2' when col_TOTO_RATES(i).r1='2' then 'X' end end))
                 and (tar.rate_2=(case when col_TOTO_IDX(j).RATE_2=1 then col_TOTO_RATES(i).r2 else case when col_TOTO_RATES(i).r2='1' then '2' when col_TOTO_RATES(i).r2='X' then '1' when col_TOTO_RATES(i).r2='2' then '1' end end)
                    or tar.rate_2=(case when col_TOTO_IDX(j).RATE_2=1 then col_TOTO_RATES(i).r2 else case when col_TOTO_RATES(i).r2='1' then '2' when col_TOTO_RATES(i).r2='X' then '1' when col_TOTO_RATES(i).r2='2' then '1' end end))
                 and (tar.rate_3=(case when col_TOTO_IDX(j).RATE_3=1 then col_TOTO_RATES(i).r3 else case when col_TOTO_RATES(i).r3='1' then '2' when col_TOTO_RATES(i).r3='X' then '1' when col_TOTO_RATES(i).r3='2' then '1' end end) 
                    or tar.rate_3=(case when col_TOTO_IDX(j).RATE_3=1 then col_TOTO_RATES(i).r3 else case when col_TOTO_RATES(i).r3='1' then 'X' when col_TOTO_RATES(i).r3='X' then '2' when col_TOTO_RATES(i).r3='2' then 'X' end end))
                 and (tar.rate_4=(case when col_TOTO_IDX(j).RATE_4=1 then col_TOTO_RATES(i).r4 else case when col_TOTO_RATES(i).r4='1' then '2' when col_TOTO_RATES(i).r4='X' then '1' when col_TOTO_RATES(i).r4='2' then '1' end end) 
                    or tar.rate_4=(case when col_TOTO_IDX(j).RATE_4=1 then col_TOTO_RATES(i).r4 else case when col_TOTO_RATES(i).r4='1' then 'X' when col_TOTO_RATES(i).r4='X' then '2' when col_TOTO_RATES(i).r4='2' then 'X' end end))
                 and (tar.rate_5=(case when col_TOTO_IDX(j).RATE_5=1 then col_TOTO_RATES(i).r5 else case when col_TOTO_RATES(i).r5='1' then '2' when col_TOTO_RATES(i).r5='X' then '1' when col_TOTO_RATES(i).r5='2' then '1' end end) 
                    or tar.rate_5=(case when col_TOTO_IDX(j).RATE_5=1 then col_TOTO_RATES(i).r5 else case when col_TOTO_RATES(i).r5='1' then 'X' when col_TOTO_RATES(i).r5='X' then '2' when col_TOTO_RATES(i).r5='2' then 'X' end end))
                 and (tar.rate_6=(case when col_TOTO_IDX(j).RATE_6=1 then col_TOTO_RATES(i).r6 else case when col_TOTO_RATES(i).r6='1' then '2' when col_TOTO_RATES(i).r6='X' then '1' when col_TOTO_RATES(i).r6='2' then '1' end end) 
                    or tar.rate_6=(case when col_TOTO_IDX(j).RATE_6=1 then col_TOTO_RATES(i).r6 else case when col_TOTO_RATES(i).r6='1' then 'X' when col_TOTO_RATES(i).r6='X' then '2' when col_TOTO_RATES(i).r6='2' then 'X' end end))
                 and (tar.rate_7=(case when col_TOTO_IDX(j).RATE_7=1 then col_TOTO_RATES(i).r7 else case when col_TOTO_RATES(i).r7='1' then '2' when col_TOTO_RATES(i).r7='X' then '1' when col_TOTO_RATES(i).r7='2' then '1' end end) 
                    or tar.rate_7=(case when col_TOTO_IDX(j).RATE_7=1 then col_TOTO_RATES(i).r7 else case when col_TOTO_RATES(i).r7='1' then 'X' when col_TOTO_RATES(i).r7='X' then '2' when col_TOTO_RATES(i).r7='2' then 'X' end end))
                 and (tar.rate_8=(case when col_TOTO_IDX(j).RATE_8=1 then col_TOTO_RATES(i).r8 else case when col_TOTO_RATES(i).r8='1' then '2' when col_TOTO_RATES(i).r8='X' then '1' when col_TOTO_RATES(i).r8='2' then '1' end end) 
                    or tar.rate_8=(case when col_TOTO_IDX(j).RATE_8=1 then col_TOTO_RATES(i).r8 else case when col_TOTO_RATES(i).r8='1' then 'X' when col_TOTO_RATES(i).r8='X' then '2' when col_TOTO_RATES(i).r8='2' then 'X' end end))
                 and (tar.rate_9=(case when col_TOTO_IDX(j).RATE_9=1 then col_TOTO_RATES(i).r9 else case when col_TOTO_RATES(i).r9='1' then '2' when col_TOTO_RATES(i).r9='X' then '1' when col_TOTO_RATES(i).r9='2' then '1' end end) 
                    or tar.rate_9=(case when col_TOTO_IDX(j).RATE_9=1 then col_TOTO_RATES(i).r9 else case when col_TOTO_RATES(i).r9='1' then 'X' when col_TOTO_RATES(i).r9='X' then '2' when col_TOTO_RATES(i).r9='2' then 'X' end end))
                 and (tar.rate_10=(case when col_TOTO_IDX(j).RATE_10=1 then col_TOTO_RATES(i).r10 else case when col_TOTO_RATES(i).r10='1' then '2' when col_TOTO_RATES(i).r10='X' then '1' when col_TOTO_RATES(i).r10='2' then '1' end end) 
                    or tar.rate_10=(case when col_TOTO_IDX(j).RATE_10=1 then col_TOTO_RATES(i).r10 else case when col_TOTO_RATES(i).r10='1' then 'X' when col_TOTO_RATES(i).r10='X' then '2' when col_TOTO_RATES(i).r10='2' then 'X' end end))
                 and (tar.rate_11=(case when col_TOTO_IDX(j).RATE_11=1 then col_TOTO_RATES(i).r11 else case when col_TOTO_RATES(i).r11='1' then '2' when col_TOTO_RATES(i).r11='X' then '1' when col_TOTO_RATES(i).r11='2' then '1' end end) 
                    or tar.rate_11=(case when col_TOTO_IDX(j).RATE_11=1 then col_TOTO_RATES(i).r11 else case when col_TOTO_RATES(i).r11='1' then 'X' when col_TOTO_RATES(i).r11='X' then '2' when col_TOTO_RATES(i).r11='2' then 'X' end end))
                 and (tar.rate_12=(case when col_TOTO_IDX(j).RATE_12=1 then col_TOTO_RATES(i).r12 else case when col_TOTO_RATES(i).r12='1' then '2' when col_TOTO_RATES(i).r12='X' then '1' when col_TOTO_RATES(i).r12='2' then '1' end end) 
                    or tar.rate_12=(case when col_TOTO_IDX(j).RATE_12=1 then col_TOTO_RATES(i).r12 else case when col_TOTO_RATES(i).r12='1' then 'X' when col_TOTO_RATES(i).r12='X' then '2' when col_TOTO_RATES(i).r12='2' then 'X' end end))
                 and (tar.rate_13=(case when col_TOTO_IDX(j).RATE_13=1 then col_TOTO_RATES(i).r13 else case when col_TOTO_RATES(i).r13='1' then '2' when col_TOTO_RATES(i).r13='X' then '1' when col_TOTO_RATES(i).r13='2' then '1' end end) 
                    or tar.rate_13=(case when col_TOTO_IDX(j).RATE_13=1 then col_TOTO_RATES(i).r13 else case when col_TOTO_RATES(i).r13='1' then 'X' when col_TOTO_RATES(i).r13='X' then '2' when col_TOTO_RATES(i).r13='2' then 'X' end end))
                 and (tar.rate_14=(case when col_TOTO_IDX(j).RATE_14=1 then col_TOTO_RATES(i).r14 else case when col_TOTO_RATES(i).r14='1' then '2' when col_TOTO_RATES(i).r14='X' then '1' when col_TOTO_RATES(i).r14='2' then '1' end end) 
                    or tar.rate_14=(case when col_TOTO_IDX(j).RATE_14=1 then col_TOTO_RATES(i).r14 else case when col_TOTO_RATES(i).r14='1' then 'X' when col_TOTO_RATES(i).r14='X' then '2' when col_TOTO_RATES(i).r14='2' then 'X' end end))
                 and (tar.rate_15=(case when col_TOTO_IDX(j).RATE_15=1 then col_TOTO_RATES(i).r15 else case when col_TOTO_RATES(i).r15='1' then '2' when col_TOTO_RATES(i).r15='X' then '1' when col_TOTO_RATES(i).r15='2' then '1' end end) 
                    or tar.rate_15=(case when col_TOTO_IDX(j).RATE_15=1 then col_TOTO_RATES(i).r15 else case when col_TOTO_RATES(i).r15='1' then 'X' when col_TOTO_RATES(i).r15='X' then '2' when col_TOTO_RATES(i).r15='2' then 'X' end end))
                 ;
    
    --это тоже просто лого
    insert into toto_outcomes_tmp (RATE_1,RATE_2,RATE_3,RATE_4,RATE_5,
                               RATE_6,RATE_7,RATE_8,RATE_9,RATE_10,
                               RATE_11,RATE_12,RATE_13,RATE_14,RATE_15,date_start)
                values (col_TOTO_IDX(j).rate_1,col_TOTO_IDX(j).rate_2,col_TOTO_IDX(j).rate_3,col_TOTO_IDX(j).rate_4,col_TOTO_IDX(j).rate_5,
                        col_TOTO_IDX(j).rate_6,col_TOTO_IDX(j).rate_7,col_TOTO_IDX(j).rate_8,col_TOTO_IDX(j).rate_9,col_TOTO_IDX(j).rate_10,
                        col_TOTO_IDX(j).rate_11,col_TOTO_IDX(j).rate_12,col_TOTO_IDX(j).rate_13,col_TOTO_IDX(j).rate_14,col_TOTO_IDX(j).rate_15,dDateStart);               
    
    commit; 
    DBMS_SESSION.FREE_UNUSED_USER_MEMORY;
  end loop;
  commit; 
END;
/DECLARE
  p_R1 CHAR(1);   p_R1 CHAR(1);
  p_R2 CHAR(1);   p_R2 CHAR(1);
  p_R3 CHAR(1);   p_R3 CHAR(1);
  p_R4 CHAR(1);   p_R4 CHAR(1);
  p_R5 CHAR(1);   p_R5 CHAR(1);
  p_R6 CHAR(1);   p_R6 CHAR(1);
  p_R7 CHAR(1);   p_R7 CHAR(1);
  p_R8 CHAR(1);   p_R8 CHAR(1);
  p_R9 CHAR(1);   p_R9 CHAR(1);
  p_R10 CHAR(1);  p_R10 CHAR(1);
  p_R11 CHAR(1);  p_R11 CHAR(1);
  p_R12 CHAR(1);  p_R12 CHAR(1);
  p_R13 CHAR(1);  p_R13 CHAR(1);
  p_R14 CHAR(1);  p_R14 CHAR(1);
  p_R15 CHAR(1);  p_R15 CHAR(1);

BEGIN
  For ic in (select R1, R2, R3, R4, R5, R6,R7, R8, R9, R10, R11, R12, R13, R14, R15, COUNT(*) cnt, SUM(SUMMA) SUMMA
              from toto_rates t where R1='1' and R2='1' and R3='1' and R4='1' and R5='1' 
                                          and R6='1' and R7='1' and R8='1' and R9='1' and R10='1' 
                                          and R11='1' and R12='1' and R13='1' and R14='1' and R15='1' --rownum<2
              group by R1, R2, R3, R4, R5, R6,R7, R8, R9, R10, R11, R12, R13, R14, R15
    ) LOOP 
      For ForComp in (select * 
                     from toto_idx where rate_cnt>=9 --and rownum<4
                     order by rate_cnt desc, id
                     ) loop
               
        MERGE INTO TOTO_OUTCOMES_FULL tar 
          USING (select (CASE WHEN ForComp.Rate_1=1 THEN ic.R1 ELSE DECODE(ic.R1, '1', '2', '2', 'X', 'X', '1') END) as R1
                      ,(CASE WHEN ForComp.Rate_2=1 THEN ic.R2 ELSE DECODE(ic.R2, '1', '2', '2', 'X', 'X', '1') END) as R2
                      ,(CASE WHEN ForComp.Rate_3=1 THEN ic.R3 ELSE DECODE(ic.R3, '1', '2', '2', 'X', 'X', '1') END) as R3
                      ,(CASE WHEN ForComp.Rate_4=1 THEN ic.R4 ELSE DECODE(ic.R4, '1', '2', '2', 'X', 'X', '1') END) as R4
                      ,(CASE WHEN ForComp.Rate_5=1 THEN ic.R5 ELSE DECODE(ic.R5, '1', '2', '2', 'X', 'X', '1') END) as R5
                      ,(CASE WHEN ForComp.Rate_6=1 THEN ic.R6 ELSE DECODE(ic.R6, '1', '2', '2', 'X', 'X', '1') END) as R6
                      ,(CASE WHEN ForComp.Rate_7=1 THEN ic.R7 ELSE DECODE(ic.R7, '1', '2', '2', 'X', 'X', '1') END) as R7
                      ,(CASE WHEN ForComp.Rate_8=1 THEN ic.R8 ELSE DECODE(ic.R8, '1', '2', '2', 'X', 'X', '1') END) as R8
                      ,(CASE WHEN ForComp.Rate_9=1 THEN ic.R9 ELSE DECODE(ic.R9, '1', '2', '2', 'X', 'X', '1') END) as R9
                      ,(CASE WHEN ForComp.Rate_10=1 THEN ic.R10 ELSE DECODE(ic.R10, '1', '2', '2', 'X', 'X', '1') END) as R10
                      ,(CASE WHEN ForComp.Rate_11=1 THEN ic.R11 ELSE DECODE(ic.R11, '1', '2', '2', 'X', 'X', '1') END) as R11
                      ,(CASE WHEN ForComp.Rate_12=1 THEN ic.R12 ELSE DECODE(ic.R12, '1', '2', '2', 'X', 'X', '1') END) as R12
                      ,(CASE WHEN ForComp.Rate_13=1 THEN ic.R13 ELSE DECODE(ic.R13, '1', '2', '2', 'X', 'X', '1') END) as R13
                      ,(CASE WHEN ForComp.Rate_14=1 THEN ic.R14 ELSE DECODE(ic.R14, '1', '2', '2', 'X', 'X', '1') END) as R14
                      ,(CASE WHEN ForComp.Rate_15=1 THEN ic.R15 ELSE DECODE(ic.R15, '1', '2', '2', 'X', 'X', '1') END) as R15
                      ,ic.SUMMA as SUMMA 
                      ,ic.CNT as CNT
                    from dual
                  UNION
                    select (CASE WHEN ForComp.Rate_1=1 THEN ic.R1 ELSE DECODE(ic.R1, '1', 'X', '2', '1', 'X', '2') END) as R1
                        ,(CASE WHEN ForComp.Rate_2=1 THEN ic.R2 ELSE DECODE(ic.R2, '1', 'X', '2', '1', 'X', '2') END) as R2
                        ,(CASE WHEN ForComp.Rate_3=1 THEN ic.R3 ELSE DECODE(ic.R3, '1', 'X', '2', '1', 'X', '2') END) as R3
                        ,(CASE WHEN ForComp.Rate_4=1 THEN ic.R4 ELSE DECODE(ic.R4, '1', 'X', '2', '1', 'X', '2') END) as R4
                        ,(CASE WHEN ForComp.Rate_5=1 THEN ic.R5 ELSE DECODE(ic.R5, '1', 'X', '2', '1', 'X', '2') END) as R5
                        ,(CASE WHEN ForComp.Rate_6=1 THEN ic.R6 ELSE DECODE(ic.R6, '1', 'X', '2', '1', 'X', '2') END) as R6
                        ,(CASE WHEN ForComp.Rate_7=1 THEN ic.R7 ELSE DECODE(ic.R7, '1', 'X', '2', '1', 'X', '2') END) as R7
                        ,(CASE WHEN ForComp.Rate_8=1 THEN ic.R8 ELSE DECODE(ic.R8, '1', 'X', '2', '1', 'X', '2') END) as R8
                        ,(CASE WHEN ForComp.Rate_9=1 THEN ic.R9 ELSE DECODE(ic.R9, '1', 'X', '2', '1', 'X', '2') END) as R9
                        ,(CASE WHEN ForComp.Rate_10=1 THEN ic.R10 ELSE DECODE(ic.R10, '1', 'X', '2', '1', 'X', '2') END) as R10
                        ,(CASE WHEN ForComp.Rate_11=1 THEN ic.R11 ELSE DECODE(ic.R11, '1', 'X', '2', '1', 'X', '2') END) as R11
                        ,(CASE WHEN ForComp.Rate_12=1 THEN ic.R12 ELSE DECODE(ic.R12, '1', 'X', '2', '1', 'X', '2') END) as R12
                        ,(CASE WHEN ForComp.Rate_13=1 THEN ic.R13 ELSE DECODE(ic.R13, '1', 'X', '2', '1', 'X', '2') END) as R13
                        ,(CASE WHEN ForComp.Rate_14=1 THEN ic.R14 ELSE DECODE(ic.R14, '1', 'X', '2', '1', 'X', '2') END) as R14
                        ,(CASE WHEN ForComp.Rate_15=1 THEN ic.R15 ELSE DECODE(ic.R15, '1', 'X', '2', '1', 'X', '2') END) as R15
                        ,ic.SUMMA as SUMMA
                        ,ic.CNT as CNT
                      from dual
                ) upd
          ON (tar.RATE_1=upd.R1 and tar.RATE_2=upd.R2 and tar.RATE_3=upd.R3
            and tar.RATE_4=upd.R4 and tar.RATE_5=upd.R5 and tar.RATE_6=upd.R6
            and tar.RATE_7=upd.R7 and tar.RATE_8=upd.R8 and tar.RATE_9=upd.R9
            and tar.RATE_10=upd.R10 and tar.RATE_11=upd.R11 and tar.RATE_12=upd.R12
            and tar.RATE_13=upd.R13 and tar.RATE_14=upd.R14 and tar.RATE_15=upd.R15)
          WHEN MATCHED THEN
            UPDATE SET tar.SUM15 = tar.SUM15 + upd.SUMMA
                      ,tar.SUM14 = tar.SUM14 + upd.SUMMA
                      ,tar.SUM13 = tar.SUM13 + upd.SUMMA
                      ,tar.SUM12 = tar.SUM12 + upd.SUMMA
                      ,tar.SUM11 = tar.SUM11 + upd.SUMMA
                      ,tar.SUM10 = tar.SUM10 + upd.SUMMA
                      ,tar.SUM9 = tar.SUM9 + upd.SUMMA
                      ,tar.UPD_STATUS = tar.UPD_STATUS + upd.CNT        ;
        COMMIT;
      END LOOP;
      
  END LOOP;
  Commit;
END;
/


create or replace TYPE    "T_NUMBER_COLLECTION"  is table of NUMBER;
