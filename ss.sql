ALTER TABLE TOTO_RATES RENAME COLUMN RATE_1 TO R1;

ALTER TABLE TOTO_RATES RENAME COLUMN RATE_2 TO R2;

ALTER TABLE TOTO_RATES RENAME COLUMN RATE_3 TO R3;

ALTER TABLE TOTO_RATES RENAME COLUMN RATE_4 TO R4;

ALTER TABLE TOTO_RATES RENAME COLUMN RATE_5 TO R5;

ALTER TABLE TOTO_RATES RENAME COLUMN RATE_6 TO R6;

ALTER TABLE TOTO_RATES RENAME COLUMN RATE_7 TO R7;

ALTER TABLE TOTO_RATES RENAME COLUMN RATE_8 TO R8;

ALTER TABLE TOTO_RATES RENAME COLUMN RATE_9 TO R9;

ALTER TABLE TOTO_RATES RENAME COLUMN RATE_10 TO R10;

ALTER TABLE TOTO_RATES RENAME COLUMN RATE_11 TO R11;

ALTER TABLE TOTO_RATES RENAME COLUMN RATE_12 TO R12;

ALTER TABLE TOTO_RATES RENAME COLUMN RATE_13 TO R13;

ALTER TABLE TOTO_RATES RENAME COLUMN RATE_14 TO R14;

ALTER TABLE TOTO_RATES RENAME COLUMN RATE_15 TO R15;


CREATE INDEX TOTO_RATES_PK ON TOTO_RATES("R1", "R2", "R3", "R4", "R5", "R6", "R7", "R8", "R9", "R10", "R11", "R12", "R13", "R14", "R15");

CREATE INDEX TOTO_OUTCOMES_FULL_PK ON TOTO_OUTCOMES_FULL("RATE_1", "RATE_2", "RATE_3", "RATE_4", "RATE_5", "RATE_6", "RATE_7", "RATE_8", "RATE_9", "RATE_10", "RATE_11", "RATE_12", "RATE_13", "RATE_14", "RATE_15") ;
CREATE INDEX TOTO_OUTCOMES_FULL_PK_REV ON TOTO_OUTCOMES_FULL("RATE_15", "RATE_14", "RATE_13", "RATE_12", "RATE_11", "RATE_10", "RATE_9", "RATE_8", "RATE_7", "RATE_6", "RATE_5", "RATE_4", "RATE_3", "RATE_2", "RATE_1") ;


CREATE INDEX TOTO_RATES_PK_1_5 ON TOTO_RATES("R1", "R2", "R3", "R4", "R5");
CREATE INDEX TOTO_RATES_PK_6_10 ON TOTO_RATES("R6", "R7", "R8", "R9", "R10");
CREATE INDEX TOTO_RATES_PK_11_15 ON TOTO_RATES("R11", "R12", "R13", "R14", "R15");

CREATE INDEX TOTO_RATES_IDX_TXT ON TOTO_RATES ( R ) INDEXTYPE IS ctxsys.context;--1. БИГ таблица(в которой хранятся 14 млн записей)
CREATE TABLE TOTO_OUTCOMES

--Делаем еще таблицу TOTO_OUTCOMES_OK, организованную по индексу (это копия таблицы TOTO_OUTCOMES)
--Эту таблицу можно использовать для поиска (запросы к ней быстрее чем к 
CREATE TABLE TOTO_OUTCOMES_OK


--2. Таблица для хранения сумм (180 тыс записей)
--заполняем таблицу данными из текстового файла TOTO_RATES.TXT, который я приложил в архиве)
CREATE TABLE TOTO_RATES


--Созадем таблицу уникальных вариантов совпадений по 9,10,11,12,13,14,15 полям
CREATE TABLE TOTO_IDX

----------------------
--таблица ниже нужно для логирования посчитанных вариантов (замена операции update в таблице TOTO_OUTCOMES)
--таблица для накопления 
CREATE TABLE TOTO_OUTCOMES_TMP

--Таблица-лог. В неее логируем все запросы к таблице TOTO_RATES
CREATE TABLE TOTO_UPDATE_LOG

--Модифицировання таблица TOTO_RATES на базе таблицы организованной по индексу (по ней поиск делать быстрее)
CREATE TABLE TOTO_rates_OICCREATE OR REPLACE PACKAGE tote
--Расчет тотализатора
AS      
--функция парсинга строчки ставок в уникальные ставки  
  FUNCTION parse_string (
    StringValue IN dbase.t_varchar2_collection,
    nSumma number, --сумма ставки
    nTotoId number, --идентификатор уникального розыгрыша
    nTypeRequest number:=0 --тип запроса (0-парсинг ставок в уникальные, 1 - генерация данных для таблицы TOTO_OUTCOMES, 2 -  генерация вариантов для таблицы TOTO_IDX)
) RETURN dbase.t_varchar2_collection; 
 
END tote;
/

---

CREATE OR REPLACE PACKAGE BODY tote
AS

FUNCTION parse_string (
    StringValue IN dbase.t_varchar2_collection,
    nSumma number, --сумма
    nTotoId number, --идентификатор
    nTypeRequest number:=0 --тип запроса (0-парсинг ставок в уникальные, 1 - генерация данных для таблицы TOTO_OUTCOMES, 2 -  генерация вариантов для таблицы TOTO_IDX)
)
RETURN dbase.t_varchar2_collection
IS
k number:=1;

col_position         t_integer_collection;
col_piece            t_varchar2_collection;
col_record_number    t_integer_collection;

col_Temp             t_varchar2_collection;

BEGIN  


   FOR i IN COALESCE(StringValue.FIRST, 1) .. COALESCE(StringValue.LAST, 0) LOOP  
        
       select level, regexp_substr(u.s,'[^,]+', 1, level), regexp_instr(u.s,'[^,]+', 1, level)
       BULK COLLECT INTO col_record_number, col_piece, col_position
           from (select StringValue(i) s from dual) u
           where length(regexp_substr(u.s,'[^,]+', 1, level))>1 
           and rownum=1
           connect by regexp_substr(u.s, '[^,]+', 1, level) is not null;
       
       if col_piece.COUNT>0 then 
            select REGEXP_REPLACE(StringValue(i),col_piece(1), regexp_substr(s,'.{1}', 1, level),col_position(1),1) 
            BULK COLLECT INTO col_Temp
                 from (select col_piece(1) s from dual)
                 connect by regexp_substr(s, '.{1}', 1, level) is not null;
       
            if col_Temp.COUNT>0 then
                col_Temp:=tote.parse_string(col_Temp,nSumma,nTotoId,nTypeRequest);
            end if;
       else
           col_Temp:=StringValue;

           case when nTypeRequest = 0 then
                insert into TOTO_RATES (TOTO_ID, SUMMA,
                                          RATE_1,
                                          RATE_2,
                                          RATE_3,
                                          RATE_4,
                                          RATE_5,
                                          RATE_6,
                                          RATE_7,
                                          RATE_8,
                                          RATE_9,
                                          RATE_10,
                                          RATE_11,
                                          RATE_12,
                                          RATE_13,
                                          RATE_14,
                                          RATE_15)
                values (nTotoId, nSumma, 
                   substr(StringValue(i),1,1),
                   substr(StringValue(i),3,1),
                   substr(StringValue(i),5,1),
                   substr(StringValue(i),7,1),
                   substr(StringValue(i),9,1),
                   substr(StringValue(i),11,1),
                   substr(StringValue(i),13,1),
                   substr(StringValue(i),15,1),
                   substr(StringValue(i),17,1),
                   substr(StringValue(i),19,1),
                   substr(StringValue(i),21,1),
                   substr(StringValue(i),23,1),
                   substr(StringValue(i),25,1),
                   substr(StringValue(i),27,1),
                   substr(StringValue(i),29,1)
                );   
                
                when nTypeRequest = 1 then
                    insert into TOTO_OUTCOMES (RATE_1,
                                            RATE_2,
                                            RATE_3,
                                            RATE_4,
                                            RATE_5,
                                            RATE_6,
                                            RATE_7,
                                            RATE_8,
                                            RATE_9,
                                            RATE_10,
                                            RATE_11,
                                            RATE_12,
                                            RATE_13,
                                            RATE_14,
                                            RATE_15)
                values (substr(StringValue(i),1,1),
                   substr(StringValue(i),3,1),
                   substr(StringValue(i),5,1),
                   substr(StringValue(i),7,1),
                   substr(StringValue(i),9,1),
                   substr(StringValue(i),11,1),
                   substr(StringValue(i),13,1),
                   substr(StringValue(i),15,1),
                   substr(StringValue(i),17,1),
                   substr(StringValue(i),19,1),
                   substr(StringValue(i),21,1),
                   substr(StringValue(i),23,1),
                   substr(StringValue(i),25,1),
                   substr(StringValue(i),27,1),
                   substr(StringValue(i),29,1)
                );
                commit;
                when nTypeRequest = 2 then  
                
                if to_number(substr(StringValue(i),1,1))+
                   to_number(substr(StringValue(i),3,1))+
                   to_number(substr(StringValue(i),5,1))+
                   to_number(substr(StringValue(i),7,1))+
                   to_number(substr(StringValue(i),9,1))+
                   to_number(substr(StringValue(i),11,1))+
                   to_number(substr(StringValue(i),13,1))+
                   to_number(substr(StringValue(i),15,1))+
                   to_number(substr(StringValue(i),17,1))+
                   to_number(substr(StringValue(i),19,1))+
                   to_number(substr(StringValue(i),21,1))+
                   to_number(substr(StringValue(i),23,1))+
                   to_number(substr(StringValue(i),25,1))+
                   to_number(substr(StringValue(i),27,1))+
                   to_number(substr(StringValue(i),29,1)) >=9 then
                   
                        insert into TOTO_IDX (rate_cnt,
                                                  RATE_1,
                                                  RATE_2,
                                                  RATE_3,
                                                  RATE_4,
                                                  RATE_5,
                                                  RATE_6,
                                                  RATE_7,
                                                  RATE_8,
                                                  RATE_9,
                                                  RATE_10,
                                                  RATE_11,
                                                  RATE_12,
                                                  RATE_13,
                                                  RATE_14,
                                                  RATE_15)
                        values (to_number(substr(StringValue(i),1,1))+
                               to_number(substr(StringValue(i),3,1))+
                               to_number(substr(StringValue(i),5,1))+
                               to_number(substr(StringValue(i),7,1))+
                               to_number(substr(StringValue(i),9,1))+
                               to_number(substr(StringValue(i),11,1))+
                               to_number(substr(StringValue(i),13,1))+
                               to_number(substr(StringValue(i),15,1))+
                               to_number(substr(StringValue(i),17,1))+
                               to_number(substr(StringValue(i),19,1))+
                               to_number(substr(StringValue(i),21,1))+
                               to_number(substr(StringValue(i),23,1))+
                               to_number(substr(StringValue(i),25,1))+
                               to_number(substr(StringValue(i),27,1))+
                               to_number(substr(StringValue(i),29,1)), 
                           to_number(substr(StringValue(i),1,1)),
                           to_number(substr(StringValue(i),3,1)),
                           to_number(substr(StringValue(i),5,1)),
                           to_number(substr(StringValue(i),7,1)),
                           to_number(substr(StringValue(i),9,1)),
                           to_number(substr(StringValue(i),11,1)),
                           to_number(substr(StringValue(i),13,1)),
                           to_number(substr(StringValue(i),15,1)),
                           to_number(substr(StringValue(i),17,1)),
                           to_number(substr(StringValue(i),19,1)),
                           to_number(substr(StringValue(i),21,1)),
                           to_number(substr(StringValue(i),23,1)),
                           to_number(substr(StringValue(i),25,1)),
                           to_number(substr(StringValue(i),27,1)),
                           to_number(substr(StringValue(i),29,1))
                        );   
                        commit;   
                end if;
           end case;
           
                                                    
       end if; 
       
        
   END LOOP;

  RETURN col_Temp;
END parse_string;

END tote;
/

--таблица ниже нужно для логирования посчитанных вариантов (замена операции update в таблице TOTO_OUTCOMES)
--таблица для накопления 
CREATE TABLE TOTO_OUTCOMES_TMP
(
  RATE_1      CHAR(1 BYTE),
  RATE_2      CHAR(1 BYTE),
  RATE_3      CHAR(1 BYTE),
  RATE_4      CHAR(1 BYTE),
  RATE_5      CHAR(1 BYTE),
  RATE_6      CHAR(1 BYTE),
  RATE_7      CHAR(1 BYTE),
  RATE_8      CHAR(1 BYTE),
  RATE_9      CHAR(1 BYTE),
  RATE_10     CHAR(1 BYTE),
  RATE_11     CHAR(1 BYTE),
  RATE_12     CHAR(1 BYTE),
  RATE_13     CHAR(1 BYTE),
  RATE_14     CHAR(1 BYTE),
  RATE_15     CHAR(1 BYTE),
  SUM9        NUMBER,
  SUM10       NUMBER,
  SUM11       NUMBER,
  SUM12       NUMBER,
  SUM13       NUMBER,
  SUM14       NUMBER,
  SUM15       NUMBER,
  DATE_INS    TIMESTAMP(5)                      DEFAULT systimestamp,
  DATE_START  TIMESTAMP(5)
)
TABLESPACE USERS
RESULT_CACHE (MODE DEFAULT)
PCTUSED    0
PCTFREE    10
INITRANS   1
MAXTRANS   255
STORAGE    (
            INITIAL          64K
            NEXT             1M
            MAXSIZE          UNLIMITED
            MINEXTENTS       1
            MAXEXTENTS       UNLIMITED
            PCTINCREASE      0
            BUFFER_POOL      DEFAULT
            FLASH_CACHE      DEFAULT
            CELL_FLASH_CACHE DEFAULT
           )
NOLOGGING 
NOCOMPRESS 
NOCACHE
PARALLEL ( DEGREE DEFAULT INSTANCES DEFAULT )
MONITORING;
/

--Таблица-лог. В неее логируем все запросы к таблице TOTO_RATES
CREATE TABLE TOTO_UPDATE_LOG
(
  TOTO_SCRIPT  VARCHAR2(4000 BYTE),
  START_DATE   TIMESTAMP(3),
  STOP_DATE    TIMESTAMP(3)                     DEFAULT SYSTIMESTAMP,
  RATE_CNT     NUMBER,
  UCNT         NUMBER,
  ID           INTEGER,
  SEC          NUMBER,
  SUMMA        NUMBER
)
TABLESPACE USERS
RESULT_CACHE (MODE DEFAULT)
PCTUSED    0
PCTFREE    10
INITRANS   1
MAXTRANS   255
STORAGE    (
            INITIAL          64K
            NEXT             1M
            MAXSIZE          UNLIMITED
            MINEXTENTS       1
            MAXEXTENTS       UNLIMITED
            PCTINCREASE      0
            BUFFER_POOL      DEFAULT
            FLASH_CACHE      DEFAULT
            CELL_FLASH_CACHE DEFAULT
           )
LOGGING 
NOCOMPRESS 
NOCACHE
NOPARALLEL
MONITORING;
/

--Модифицировання таблица TOTO_RATES на базе таблицы организованной по индексу (по ней поиск делать быстрее)
CREATE TABLE SL.TOTO_rates_OIC
(
  RATE_1 ,  RATE_2 ,  RATE_3 ,  RATE_4 ,  RATE_5 ,  RATE_6 ,  RATE_7 ,  RATE_8 ,  RATE_9 ,  RATE_10,
  RATE_11,  RATE_12,  RATE_13,  RATE_14,  RATE_15,
  SUMMA  ,
  CNT,
  CONSTRAINT TOTO_rates_OIC_PK 
  primary key
  (RATE_1, RATE_2, RATE_3, RATE_4, RATE_5, RATE_6, RATE_7, RATE_8, RATE_9, RATE_10, RATE_11, RATE_12, RATE_13, RATE_14, RATE_15)
  ENABLE VALIDATE
)
ORGANIZATION INDEX COMPRESS

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
            BUFFER_POOL      DEFAULT
            FLASH_CACHE      DEFAULT
            CELL_FLASH_CACHE DEFAULT
           )
NOLOGGING 
NOPARALLEL
NOMONITORING
as select RATE_1 ,  RATE_2 ,  RATE_3 ,  RATE_4 ,  RATE_5 ,  RATE_6 ,  RATE_7 , RATE_8 ,  RATE_9 ,  RATE_10,
  RATE_11,  RATE_12,  RATE_13,  RATE_14,  RATE_15,
  sum(summa) as summa,
  count(*) as cnt
from toto_rates
group by RATE_1 ,  RATE_2 ,  RATE_3 ,  RATE_4 ,  RATE_5 ,  RATE_6 ,  RATE_7 ,  RATE_8 ,  RATE_9 ,  RATE_10,  
RATE_11,  RATE_12,  RATE_13,  RATE_14,  RATE_15;
/

CREATE TABLE SL.TOTO_rates_OIC
(
  R1 ,  R2 ,  R3 ,  R4 ,  R5 ,  R6 ,  R7 ,  R8 ,  R9 ,  R10,
  R11,  R12,  R13,  R14,  R15,
  SUMMA  ,
  CNT,
  CONSTRAINT TOTO_rates_OIC_PK 
  primary key
  (R1, R2, R3, R4, R5, R6, R7, R8, R9, R10, R11, R12, R13, R14, R15)
  ENABLE VALIDATE
)
ORGANIZATION INDEX COMPRESS

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
            BUFFER_POOL      DEFAULT
            FLASH_CACHE      DEFAULT
            CELL_FLASH_CACHE DEFAULT
           )
NOLOGGING 
NOPARALLEL
NOMONITORING
as select RATE_1 ,  RATE_2 ,  RATE_3 ,  RATE_4 ,  RATE_5 ,  RATE_6 ,  RATE_7 , RATE_8 ,  RATE_9 ,  RATE_10,
  RATE_11,  RATE_12,  RATE_13,  RATE_14,  RATE_15,
  sum(summa) as summa,
  count(*) as cnt
from toto_rates
group by RATE_1 ,  RATE_2 ,  RATE_3 ,  RATE_4 ,  RATE_5 ,  RATE_6 ,  RATE_7 ,  RATE_8 ,  RATE_9 ,  RATE_10,  
RATE_11,  RATE_12,  RATE_13,  RATE_14,  RATE_15;
/

--Процедура (обратный способ) поиска всех summa в таблице TOTO_RATES для каждой записи в таблице TOTO_OUTCOMES_OK (или TOTO_OUTCOMES)
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
  For ForRate in (select * from toto_outcomes_ok t where rownum<20 --id<=20
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
              ' from toto_rates_oic '||
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
              commit;
            
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
              ' from toto_rates_oic '||
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
  --commit; 
END;--1. БИГ таблица(в которой хранятся 14 млн записей)
CREATE TABLE SL.TOTO_OUTCOMES
(
  ID          NUMBER,
  RATE_1      CHAR(1 BYTE), RATE_2      CHAR(1 BYTE),  RATE_3      CHAR(1 BYTE),  RATE_4      CHAR(1 BYTE),  RATE_5      CHAR(1 BYTE),
  RATE_6      CHAR(1 BYTE),  RATE_7      CHAR(1 BYTE),  RATE_8      CHAR(1 BYTE),  RATE_9      CHAR(1 BYTE),  RATE_10     CHAR(1 BYTE),
  RATE_11     CHAR(1 BYTE),  RATE_12     CHAR(1 BYTE),  RATE_13     CHAR(1 BYTE),  RATE_14     CHAR(1 BYTE),  RATE_15     CHAR(1 BYTE),
  DATE_INS    TIMESTAMP(3)                      DEFAULT SYSTIMESTAMP,
  SUM9        NUMBER                            DEFAULT 0,
  SUM10       NUMBER                            DEFAULT 0,
  SUM11       NUMBER                            DEFAULT 0,
  SUM12       NUMBER                            DEFAULT 0,
  SUM13       NUMBER                            DEFAULT 0,
  SUM14       NUMBER                            DEFAULT 0,
  SUM15       NUMBER                            DEFAULT 0,
  UPD_STATUS  NUMBER                            DEFAULT 0
)
TABLESPACE USERS
RESULT_CACHE (MODE DEFAULT)
PCTUSED    0
PCTFREE    10
INITRANS   1
MAXTRANS   255
STORAGE    (
            INITIAL          64K
            NEXT             1M
            MAXSIZE          UNLIMITED
            MINEXTENTS       1
            MAXEXTENTS       UNLIMITED
            PCTINCREASE      0
            BUFFER_POOL      DEFAULT
            FLASH_CACHE      DEFAULT
            CELL_FLASH_CACHE DEFAULT
           )
NOLOGGING 
NOCOMPRESS 
NOCACHE
NOPARALLEL
MONITORING;
/

--Заполняем таблицу TOTO_OUTCOMES  данными (используется пакет tote):
--Генерация 14348907 записей в таблицу TOTO_OUTCOMES (генерация может занять 2-3 часа)
declare
--TYPE T_VARCHAR2_COLLECTION  IS TABLE OF VARCHAR2(2000);
col_start          t_varchar2_collection:=t_varchar2_collection('1X2,1X2,1X2,1X2,1X2,1X2,1X2,1X2,1X2,1X2,1X2,1X2,1X2,1X2,1X2');   
col_end            t_varchar2_collection; 
nTotoId number; 
                  
begin
nTotoId:=0;

    --определяем начальное значение для парсинга
   
    col_end:=tote.parse_string(col_start,0,nTotoId,1);
    --очищаем коллекции для парсинга очередной порции данных
    col_start.delete;
    col_end.delete;    
    
    commit;
end; 
/

--Делаем еще таблицу TOTO_OUTCOMES_OK, организованную по индексу (это копия таблицы TOTO_OUTCOMES)
--Эту таблицу можно использовать для поиска (запросы к ней быстрее чем к 
CREATE TABLE SL.TOTO_OUTCOMES_OK
(
  ID,
  RATE_1,  RATE_2,  RATE_3,  RATE_4,  RATE_5,  RATE_6,  RATE_7,  RATE_8,  RATE_9,  RATE_10,
  RATE_11,  RATE_12,  RATE_13,  RATE_14,  RATE_15,  DATE_INS,  SUM9,  SUM10,  SUM11,  SUM12,  SUM13,  SUM14,  SUM15,
  UPD_STATUS, 
  CONSTRAINT TOTO_OUTCOMES_OK_PK
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
            BUFFER_POOL      DEFAULT
            FLASH_CACHE      DEFAULT
            CELL_FLASH_CACHE DEFAULT
           )
NOLOGGING 
COMPRESS 14
NOPARALLEL
NOMONITORING
as select * from TOTO_OUTCOMES;
/

--2. Таблица для хранения сумм (180 тыс записей)
--заполняем таблицу данными из текстового файла TOTO_RATES.TXT, который я приложил в архиве)
CREATE TABLE TOTO_RATES
(
  TOTO_ID        NUMBER, 
  SUMMA          NUMBER,
  RATE_1         CHAR(1 BYTE),  RATE_2         CHAR(1 BYTE),  RATE_3         CHAR(1 BYTE),  RATE_4         CHAR(1 BYTE),  RATE_5         CHAR(1 BYTE),
  RATE_6         CHAR(1 BYTE),  RATE_7         CHAR(1 BYTE),  RATE_8         CHAR(1 BYTE),  RATE_9         CHAR(1 BYTE),  RATE_10        CHAR(1 BYTE),
  RATE_11        CHAR(1 BYTE),  RATE_12        CHAR(1 BYTE),  RATE_13        CHAR(1 BYTE),  RATE_14        CHAR(1 BYTE),  RATE_15        CHAR(1 BYTE),
  DATE_INS       DATE                           DEFAULT sysdate,
  TOTO_RATES_ID  INTEGER
)
TABLESPACE USERS
RESULT_CACHE (MODE DEFAULT)
PCTUSED    0
PCTFREE    10
INITRANS   1
MAXTRANS   255
STORAGE    (
            INITIAL          64K
            NEXT             1M
            MAXSIZE          UNLIMITED
            MINEXTENTS       1
            MAXEXTENTS       UNLIMITED
            PCTINCREASE      0
            BUFFER_POOL      DEFAULT
            FLASH_CACHE      DEFAULT
            CELL_FLASH_CACHE DEFAULT
           )
NOLOGGING 
NOCOMPRESS 
NOCACHE
NOPARALLEL
MONITORING;
/

--Созадем таблицу уникальных вариантов совпадений по 9,10,11,12,13,14,15 полям
CREATE TABLE TOTO_IDX
(
  ID        NUMBER,  RATE_CNT  NUMBER,  
  RATE_1    NUMBER,  RATE_2    NUMBER,  RATE_3    NUMBER,  RATE_4    NUMBER,  RATE_5    NUMBER,
  RATE_6    NUMBER,  RATE_7    NUMBER,  RATE_8    NUMBER,  RATE_9    NUMBER,  RATE_10   NUMBER,  
  RATE_11   NUMBER,  RATE_12   NUMBER,  RATE_13   NUMBER,  RATE_14   NUMBER,  RATE_15   NUMBER,
  DATE_INS  DATE                                DEFAULT sysdate,
  IDX_OK    NUMBER
)
TABLESPACE USERS
RESULT_CACHE (MODE DEFAULT)
PCTUSED    0
PCTFREE    10
INITRANS   1
MAXTRANS   255
STORAGE    (
            INITIAL          64K
            NEXT             1M
            MAXSIZE          UNLIMITED
            MINEXTENTS       1
            MAXEXTENTS       UNLIMITED
            PCTINCREASE      0
            BUFFER_POOL      DEFAULT
            FLASH_CACHE      DEFAULT
            CELL_FLASH_CACHE DEFAULT
           )
NOLOGGING 
NOCOMPRESS 
NOCACHE
NOPARALLEL
MONITORING;
/

--Заполняем таблицуTOTO_IDX
declare
--TYPE T_VARCHAR2_COLLECTION  IS TABLE OF VARCHAR2(2000);
col_start          t_varchar2_collection:=t_varchar2_collection('01,01,01,01,01,01,01,01,01,01,01,01,01,01,01');   
col_end            t_varchar2_collection; 
nTotoId number; 
                  
begin
nTotoId:=0;

    --определяем начальное значение для парсинга
   
    col_end:=tote.parse_string(col_start,0,nTotoId,2);
    --очищаем коллекции для парсинга очередной порции данных
    col_start.delete;
    col_end.delete;    
    
    commit;
end;
/


CREATE TABLE SL.TOTO_OUTCOMES_FULL
(
  ID          NUMBER,
  RATE_1      CHAR(1 BYTE), RATE_2      CHAR(1 BYTE),  RATE_3      CHAR(1 BYTE),  RATE_4      CHAR(1 BYTE),  RATE_5      CHAR(1 BYTE),
  RATE_6      CHAR(1 BYTE),  RATE_7      CHAR(1 BYTE),  RATE_8      CHAR(1 BYTE),  RATE_9      CHAR(1 BYTE),  RATE_10     CHAR(1 BYTE),
  RATE_11     CHAR(1 BYTE),  RATE_12     CHAR(1 BYTE),  RATE_13     CHAR(1 BYTE),  RATE_14     CHAR(1 BYTE),  RATE_15     CHAR(1 BYTE)
)
TABLESPACE USERS
RESULT_CACHE (MODE DEFAULT)
PCTUSED    0
PCTFREE    10
INITRANS   1
MAXTRANS   255
STORAGE    (
            INITIAL          64K
            NEXT             1M
            MAXSIZE          UNLIMITED
            MINEXTENTS       1
            MAXEXTENTS       UNLIMITED
            PCTINCREASE      0
            BUFFER_POOL      DEFAULT
            FLASH_CACHE      DEFAULT
            CELL_FLASH_CACHE DEFAULT
           )
NOLOGGING 
NOCOMPRESS 
NOCACHE
NOPARALLEL
MONITORING;
/
