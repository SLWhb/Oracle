--------------------------------------------------------
--  File created - среда-Июль-06-2016   
--------------------------------------------------------
DROP TABLE "AFS"."AFS_HISTORY";
DROP TABLE "AFS"."AFS_LOG_CALL";
DROP TABLE "AFS"."AFS_LOG_ERROR";
DROP TABLE "AFS"."D_FRAUD_OUTNUMBER";
DROP TABLE "AFS"."D_FRAUD_RULE";
DROP SEQUENCE "AFS"."AFS_PROC_SEQ";
DROP SEQUENCE "AFS"."FRAUD_RULES_SEQ";
DROP VIEW "AFS"."V_C_REQUEST_REACT_CREDIT";
DROP VIEW "AFS"."V_C_REQUEST_SPR_SHOTNAME";
DROP VIEW "AFS"."VIEW_ADDRESS_HISTORY";
DROP VIEW "AFS"."VIEW_AFS_LOG_ERROR";
DROP VIEW "AFS"."VIEW_BIRTH";
DROP VIEW "AFS"."VIEW_DOCUMENTS_HISTORY";
DROP VIEW "AFS"."VIEW_FIO_HISTORY";
DROP VIEW "AFS"."VIEW_REQUEST_PERSON_INFO";
DROP VIEW "AFS"."VR_DOC";
DROP VIEW "AFS"."VR_DOC_FB";
DROP VIEW "AFS"."V_REQUEST_PERSON_INFO";
DROP VIEW "AFS"."VR_FIO_BIRTH";
DROP VIEW "AFS"."VR_FMG";
DROP VIEW "AFS"."VR_FMG_FB";
DROP VIEW "AFS"."VR_FMG_FB_PH";
DROP VIEW "AFS"."VR_FMG_PH";
DROP VIEW "AFS"."VR_MOB";
DROP VIEW "AFS"."VR_MOB_FB";
DROP VIEW "AFS"."VR_PMG";
DROP VIEW "AFS"."VR_PMG_FB";
DROP VIEW "AFS"."VR_PMG_FB_PH";
DROP VIEW "AFS"."VR_PMG_PH";
DROP FUNCTION "AFS"."FN_DIST_LEV";
DROP FUNCTION "AFS"."FN_IS_FAMILY_CONT";
DROP FUNCTION "AFS"."FN_IS_FAMILY_REL";
DROP FUNCTION "AFS"."FN$IS_REQUEST_FOR_FRAUD";
DROP FUNCTION "AFS"."FN$RID_FILTER_FOR_FRAUD";
DROP PACKAGE "AFS"."AFS_FILTER";
DROP PACKAGE "AFS"."AFS_RULES";
DROP PACKAGE "AFS"."AFS_RULES_ADR";
DROP PACKAGE "AFS"."AFS_RULES_OBJECTS_ID";
DROP PACKAGE "AFS"."AFS_RULES_PASP";
DROP PACKAGE "AFS"."AFS_RULES_PHONE";
DROP PACKAGE "AFS"."AFS_RULES_REQUEST";
DROP PACKAGE "AFS"."AFS_RULES_TEST";
DROP PACKAGE "AFS"."AFS_SYS";
DROP PACKAGE BODY "AFS"."AFS_FILTER";
DROP PACKAGE BODY "AFS"."AFS_RULES";
DROP PACKAGE BODY "AFS"."AFS_RULES_PASP";
DROP PACKAGE BODY "AFS"."AFS_RULES_PHONE";
DROP PACKAGE BODY "AFS"."AFS_RULES_REQUEST";
DROP PACKAGE BODY "AFS"."AFS_RULES_TEST";
DROP PACKAGE BODY "AFS"."AFS_SYS";
DROP PROCEDURE "AFS"."ROUTE_FRAUD_RULE_BY_ID";
DROP PROCEDURE "AFS"."ROUTE_FRAUD_RULE_BY_ID_TST";
DROP PROCEDURE "AFS"."ROUTE_FRAUD_RULE_OFFLINE";
DROP PROCEDURE "AFS"."ROUTE_FRAUD_RULE_ONLINE";
DROP PROCEDURE "AFS"."ROUTE_FRAUD_RULE_TEST";
DROP PROCEDURE "AFS"."TEST0_";
DROP PROCEDURE "AFS"."TEST1_";
--------------------------------------------------------
--  DDL for Sequence AFS_PROC_SEQ
--------------------------------------------------------

   CREATE SEQUENCE  "AFS"."AFS_PROC_SEQ"  MINVALUE 1 MAXVALUE 9999999999999999999999999999 INCREMENT BY 1 START WITH 7688307 CACHE 20 NOORDER  NOCYCLE ;
--------------------------------------------------------
--  DDL for Sequence FRAUD_RULES_SEQ
--------------------------------------------------------

   CREATE SEQUENCE  "AFS"."FRAUD_RULES_SEQ"  MINVALUE 1 MAXVALUE 9999999999999999999999999999 INCREMENT BY 1 START WITH 21 CACHE 20 NOORDER  NOCYCLE ;
--------------------------------------------------------
--  DDL for Table AFS_HISTORY
--------------------------------------------------------

  CREATE TABLE "AFS"."AFS_HISTORY" 
   (	"PROC_ID" NUMBER, 
	"DATE_FRAUD" TIMESTAMP (6) DEFAULT SYSTIMESTAMP, 
	"REQUEST_ID" NUMBER, 
	"PERSON_ID" NUMBER, 
	"REQUEST_DATE" DATE, 
	"FRAUD_ID" NUMBER, 
	"FRAUD_OUTNUMBER" NUMBER, 
	"FIO" VARCHAR2(500 BYTE), 
	"DR" VARCHAR2(30 BYTE), 
	"DAY_BETWEEN" NUMBER, 
	"REQUEST_ID_REL" NUMBER, 
	"PERSON_ID_REL" NUMBER, 
	"REQUEST_DATE_REL" DATE, 
	"FIO_REL" VARCHAR2(500 BYTE), 
	"DR_REL" VARCHAR2(30 BYTE), 
	"INFO_EQUAL" VARCHAR2(4000 BYTE), 
	"INFO_NOT_EQUAL" VARCHAR2(4000 BYTE), 
	"INFO_NOT_EQUAL_REL" VARCHAR2(4000 BYTE)
   ) SEGMENT CREATION IMMEDIATE 
  PCTFREE 10 PCTUSED 40 INITRANS 1 MAXTRANS 255 
 NOCOMPRESS LOGGING
  STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "AFS" ;
  GRANT SELECT ON "AFS"."AFS_HISTORY" TO "MVKARELIN";
--------------------------------------------------------
--  DDL for Table AFS_LOG_CALL
--------------------------------------------------------

  CREATE TABLE "AFS"."AFS_LOG_CALL" 
   (	"PROC_ID" NUMBER, 
	"FRAUD_ID" NUMBER, 
	"DATE_CALL" TIMESTAMP (6) DEFAULT SYSTIMESTAMP, 
	"REQUEST_ID" NUMBER, 
	"PERSON_ID" NUMBER, 
	"OUTCODE" VARCHAR2(50 BYTE), 
	"INARRAY" VARCHAR2(4000 BYTE), 
	"OUTNUMBER" NUMBER, 
	"OUTCHAR" VARCHAR2(255 BYTE), 
	"OUTARRAY" VARCHAR2(4000 BYTE), 
	"ERRORCODE" NUMBER DEFAULT 0, 
	"ERRORTEXT" VARCHAR2(4000 BYTE), 
	"DATE_CALL_END" TIMESTAMP (6), 
	"FRAUD_RULE_NM" VARCHAR2(255 BYTE)
   ) SEGMENT CREATION IMMEDIATE 
  PCTFREE 10 PCTUSED 40 INITRANS 1 MAXTRANS 255 
 NOCOMPRESS LOGGING
  STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "AFS" ;

   COMMENT ON COLUMN "AFS"."AFS_LOG_CALL"."DATE_CALL_END" IS 'Дата завершения вызова процедуры';
   COMMENT ON COLUMN "AFS"."AFS_LOG_CALL"."FRAUD_RULE_NM" IS 'Имя вызванной процедуры';
--------------------------------------------------------
--  DDL for Table AFS_LOG_ERROR
--------------------------------------------------------

  CREATE TABLE "AFS"."AFS_LOG_ERROR" 
   (	"PROC_ID" NUMBER, 
	"DATE_ERROR" TIMESTAMP (6) DEFAULT SYSTIMESTAMP, 
	"ERRORCODE" NUMBER, 
	"ERRORTEXT" VARCHAR2(4000 BYTE), 
	"ERROR_TYPE" VARCHAR2(255 BYTE), 
	"ERROR_INFO" VARCHAR2(4000 BYTE)
   ) SEGMENT CREATION IMMEDIATE 
  PCTFREE 10 PCTUSED 40 INITRANS 1 MAXTRANS 255 
 NOCOMPRESS LOGGING
  STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "AFS" ;

   COMMENT ON COLUMN "AFS"."AFS_LOG_ERROR"."ERROR_INFO" IS 'Дополнительная информация по ошибке';
--------------------------------------------------------
--  DDL for Table D_FRAUD_OUTNUMBER
--------------------------------------------------------

  CREATE TABLE "AFS"."D_FRAUD_OUTNUMBER" 
   (	"FRAUD_OUTNUMBER" NUMBER, 
	"FRAUD_OUTNUMBER_DESC" VARCHAR2(1000 BYTE)
   ) SEGMENT CREATION IMMEDIATE 
  PCTFREE 10 PCTUSED 40 INITRANS 1 MAXTRANS 255 
 NOCOMPRESS LOGGING
  STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "AFS" ;

   COMMENT ON COLUMN "AFS"."D_FRAUD_OUTNUMBER"."FRAUD_OUTNUMBER" IS 'Код возвращаемый фрод процедурами фрод-правил';
   COMMENT ON COLUMN "AFS"."D_FRAUD_OUTNUMBER"."FRAUD_OUTNUMBER_DESC" IS 'Описание возвращаемого кода';
   COMMENT ON TABLE "AFS"."D_FRAUD_OUTNUMBER"  IS 'Справочник возвращаемых кодов в прописанных процедурах фрод-правил';
--------------------------------------------------------
--  DDL for Table D_FRAUD_RULE
--------------------------------------------------------

  CREATE TABLE "AFS"."D_FRAUD_RULE" 
   (	"FRAUD_RULE_ID" NUMBER, 
	"FRAUD_RULE_NM" VARCHAR2(255 BYTE), 
	"RULE_NAME" VARCHAR2(255 BYTE), 
	"RULE_DESC" VARCHAR2(2000 BYTE), 
	"RULE_STATUS" VARCHAR2(100 BYTE), 
	"RULE_DATE" DATE, 
	"RULE_ATTR_KEY" VARCHAR2(3000 BYTE), 
	"RULE_ATTR_OTH" VARCHAR2(3000 BYTE), 
	"RULE_COMM" VARCHAR2(2000 BYTE), 
	"RULE_AKT" NUMBER, 
	"RULE_CODE" VARCHAR2(255 BYTE)
   ) SEGMENT CREATION IMMEDIATE 
  PCTFREE 10 PCTUSED 40 INITRANS 1 MAXTRANS 255 
 NOCOMPRESS LOGGING
  STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "AFS" ;

   COMMENT ON COLUMN "AFS"."D_FRAUD_RULE"."FRAUD_RULE_ID" IS 'ID правила';
   COMMENT ON COLUMN "AFS"."D_FRAUD_RULE"."FRAUD_RULE_NM" IS 'Имя реализованной процедуры для данного фрод правила';
   COMMENT ON COLUMN "AFS"."D_FRAUD_RULE"."RULE_NAME" IS 'Краткое имя для фрод правила';
   COMMENT ON COLUMN "AFS"."D_FRAUD_RULE"."RULE_DESC" IS 'Описание фрод-правила';
   COMMENT ON COLUMN "AFS"."D_FRAUD_RULE"."RULE_STATUS" IS 'Значения MIS, DEV, TEST, ОK, HOLD';
   COMMENT ON COLUMN "AFS"."D_FRAUD_RULE"."RULE_DATE" IS 'Дата внедрения фрод правила';
   COMMENT ON COLUMN "AFS"."D_FRAUD_RULE"."RULE_ATTR_KEY" IS 'Используемые ключевые атрибуты по заявке';
   COMMENT ON COLUMN "AFS"."D_FRAUD_RULE"."RULE_ATTR_OTH" IS 'Используемые дополнительные атрибуты по заявке';
   COMMENT ON COLUMN "AFS"."D_FRAUD_RULE"."RULE_COMM" IS 'Доп. комментарий';
   COMMENT ON COLUMN "AFS"."D_FRAUD_RULE"."RULE_AKT" IS 'Актуальность правила:0 или 1';
   COMMENT ON COLUMN "AFS"."D_FRAUD_RULE"."RULE_CODE" IS 'Бизнес код правила для пользователей';
   COMMENT ON TABLE "AFS"."D_FRAUD_RULE"  IS 'Справочник фрод правил';
  GRANT SELECT ON "AFS"."D_FRAUD_RULE" TO "MVKARELIN";
--------------------------------------------------------
--  DDL for View V_C_REQUEST_REACT_CREDIT
--------------------------------------------------------

  CREATE OR REPLACE FORCE VIEW "AFS"."V_C_REQUEST_REACT_CREDIT" ("REQUEST_ID", "OBJECTS_ID", "CREATED_GROUP_ID", "CREATED_USER_ID", "SCORE_TREE_ROUTE_ID", "TYPE_REQUEST_ID", "OBJECTS_TYPE", "CREATED_DATE", "MODIFICATION_DATE", "REQUEST_REACT_ID", "REQUEST_CREDIT_ID", "REACT_ID", "REACT_USER_ID", "REACT_GROUP_ID", "REQUEST_OLD_STATUS_ID", "REQUEST_NEW_STATUS_ID", "REACT_DATE", "SCHEMS_ID", "TYPE_CREDIT_ID", "TARGET_ID", "PERIOD", "SUMMA", "PERCENT", "PERCENT_TYPE", "ORG_PARTNER_CODE", "SRC_SUMMA") AS 
  select v_src.request_id
    , v_src.objects_id
    , v_src.CREATED_GROUP_ID
    , v_src.CREATED_USER_ID
    , v_src.SCORE_TREE_ROUTE_ID
    , v_src.TYPE_REQUEST_ID
    , v_src.OBJECTS_TYPE
    , v_src.CREATED_DATE
    , v_src.MODIFICATION_DATE
    , v_crr.request_react_id
    , v_crr.request_credit_id
    , v_crr.REACT_ID
    , v_crr.REACT_USER_ID
    , v_crr.REACT_GROUP_ID
    , v_crr.REQUEST_OLD_STATUS_ID
    , v_crr.REQUEST_NEW_STATUS_ID
    , v_crr.REACT_DATE
    , v_crc.SCHEMS_ID
    , v_crc.TYPE_CREDIT_ID
    , v_crc.TARGET_ID
    , v_crc.PERIOD
, v_crc.SUMMA
    , v_crc.PERCENT
    , v_crc.PERCENT_TYPE
    , v_crc.ORG_PARTNER_CODE
    , v_crc.summa as src_summa
  from kredit.c_request v_src
  inner join kredit.c_request_react v_crr 
    on v_crr.request_id = v_src.request_id
  inner join kredit.c_request_credit v_crc 
    on v_crc.request_credit_id = v_crr.request_credit_id
  /*WHERE v_src.REQUEST_ID BETWEEN 55242345 AND 55242345+10000 AND CREATED_GROUP_ID=11455
    AND REACT_GROUP_ID NOT IN (2267, 11455)*/
  /*ORDER BY REQUEST_ID, REQUEST_REACT_ID*/;
--------------------------------------------------------
--  DDL for View V_C_REQUEST_SPR_SHOTNAME
--------------------------------------------------------

  CREATE OR REPLACE FORCE VIEW "AFS"."V_C_REQUEST_SPR_SHOTNAME" ("REQUEST_ID", "COMMENT_ID", "SPR_VALUE_ID", "SPR_NAMES_ID", "SHOTNAME") AS 
  select crs.request_id,
      crs.COMMENT_ID,
      crs.spr_value_id,
      crs.spr_names_id,
      nsn.shotname
					--sum(distinct crs.spr_value_id) over (partition by  crs.request_id) as sum_target_id,
					--to_char(wm_concat(distinct nsn.shotname)over (partition by crs.request_id )) as target_request,
					/*regexp_replace( LISTAGG(nsn.shotname, ';') WITHIN GROUP (ORDER BY nsn.shotname) OVER (PARTITION BY crs.request_id) 
						, '([^;]+)(;\1)+', '\1') as target_request*/
					--,row_number() over(partition by crs.request_id order by crs.spr_value_id desc) as r_num
				from kredit.c_request_spr crs
				inner join kredit.spr_names nsn on nsn.id = crs.spr_value_id and crs.spr_names_id in (838390,43542106)
      --WHERE crs.REQUEST_ID=59941879;
--------------------------------------------------------
--  DDL for View VIEW_ADDRESS_HISTORY
--------------------------------------------------------

  CREATE OR REPLACE FORCE VIEW "AFS"."VIEW_ADDRESS_HISTORY" ("OBJECTS_ID", "OBJECTS_TYPE", "ADDRESS_ID", "POSTOFFICE", "COUNTRIES_ID", "COUNTRIES_SHOTNAME", "COUNTRIES_LONGNAME", "REGIONS_UID", "REGIONS_NAMES", "REGIONS_TYPE", "REGIONS_NTYPE", "AREAS_UID", "AREAS_NAMES", "AREAS_TYPE", "AREAS_NTYPE", "CITIES_UID", "CITIES_NAMES", "CITIES_TYPE", "CITIES_NTYPE", "STREETS_UID", "STREETS_NAMES", "STREETS_TYPE", "STREETS_NTYPE", "BUILD", "HOUSE", "FLAT", "ADDRESS_STR", "PHONES_ID", "PHONES_NUMBER", "PHONES_CODE", "PHONES", "ADDRESS_AKT", "ADDRESS_CREATED", "MODIFICATION_DATE", "ADDRESS_HISTORY_ID") AS 
  SELECT 
ah.objects_id as objects_id,
ah.objects_type as objects_type,
va.address_id,
va.postoffice,
va.countries_id,
va.countries_shotname,
va.countries_longname,
va.regions_uid,
va.regions_names,
va.regions_type,
va.regions_ntype,
va.areas_uid,
va.areas_names,
va.areas_type,
va.areas_ntype,
va.cities_uid,
va.cities_names,
va.cities_type,
va.cities_ntype,
va.streets_uid,
va.streets_names,
va.streets_type,
va.streets_ntype,
va.build,
va.house,
va.flat,
DECODE(NVL(TRIM(va.REGIONS_NAMES), '-'), '-', '', (va.REGIONS_NAMES))
  ||DECODE(NVL(TRIM(va.AREAS_NAMES), '-'), '-', '', (', РАЙОН '||va.AREAS_NAMES)) 
  ||DECODE(NVL(TRIM(va.CITIES_NAMES), '-'), '-', '', (', ' || va.cities_ntype ||' '|| va.CITIES_NAMES))
  ||DECODE(NVL(TRIM(va.STREETS_NAMES), '-'), '-', '', (', ' || va.STREETS_NAMES)) 
  ||DECODE(NVL(TRIM(va.HOUSE), '-'), '-', '', (', Д.'||va.HOUSE)) 
  ||DECODE(NVL(TRIM(va.BUILD), '-'), '-', '', (', КОРП.'||va.BUILD)) 
  ||DECODE(NVL(TRIM(va.FLAT),  '-'), '-', '', (', КВ.'||va.FLAT)) AS ADDRESS_STR,
--va.address_price,
va.phones_id,
va.phones_number,
va.phones_code,
va.phones,
ah.address_akt as address_akt,
ah.address_created,
ah.MODIFICATION_DATE,
ah.ADDRESS_HISTORY_ID
/*,DENSE_RANK() OVER (PARTITION BY ah.objects_id, ah.OBJECTS_TYPE
                ORDER BY ah.address_akt DESC, ah.address_CREATED DESC, ah.MODIFICATION_DATE DESC
                  ,ah.ADDRESS_HISTORY_ID DESC) -1 RANK*/
FROM CPD.address_history ah, KREDIT.view_address va
WHERE ah.address_id=va.address_id;
--------------------------------------------------------
--  DDL for View VIEW_AFS_LOG_ERROR
--------------------------------------------------------

  CREATE OR REPLACE FORCE VIEW "AFS"."VIEW_AFS_LOG_ERROR" ("PROC_ID", "FRAUD_ID", "DATE_CALL", "REQUEST_ID", "PERSON_ID", "OUTCODE", "INARRAY", "OUTNUMBER", "OUTCHAR", "OUTARRAY", "ERRORCODE", "ERRORTEXT", "DATE_CALL_END", "FRAUD_RULE_NM") AS 
  SELECT cal."PROC_ID",cal."FRAUD_ID",cal."DATE_CALL",cal."REQUEST_ID",cal."PERSON_ID",cal."OUTCODE",cal."INARRAY",cal."OUTNUMBER",cal."OUTCHAR",cal."OUTARRAY",cal."ERRORCODE",cal."ERRORTEXT",cal."DATE_CALL_END",cal."FRAUD_RULE_NM"
    FROM AFS.AFS_LOG_CALL cal
    WHERE ERRORCODE>0
  UNION ALL
  SELECT cal."PROC_ID",cal."FRAUD_ID",cal."DATE_CALL",cal."REQUEST_ID",cal."PERSON_ID",cal."OUTCODE",cal."INARRAY",cal."OUTNUMBER",cal."OUTCHAR",cal."OUTARRAY",cal."ERRORCODE",cal."ERRORTEXT",cal."DATE_CALL_END",cal."FRAUD_RULE_NM"
    FROM AFS.AFS_LOG_CALL cal
    WHERE ERRORCODE<0
  --ORDER BY PROC_ID DESC;
--------------------------------------------------------
--  DDL for View VIEW_BIRTH
--------------------------------------------------------

  CREATE OR REPLACE FORCE VIEW "AFS"."VIEW_BIRTH" ("OBJECTS_ID", "OBJECTS_TYPE", "BIRTH", "BIRTH_AKT", "BIRTH_CREATED", "MODIFICATION_DATE", "BIRTH_PK") AS 
  SELECT b.objects_id
       , b.objects_type
       , TO_CHAR(b.birth, 'dd.mm.yyyy') birth
       , b.birth_akt
       --, TO_CHAR(b.birth_created, 'YYYY-MM-DD HH24:MI:SS') birth_created
       , b.birth_created
       , TO_CHAR(b.MODIFICATION_DATE, 'YYYY-MM-DD HH24:MI:SS') MODIFICATION_DATE
       , b.birth_pk
    FROM CPD.birth b;
--------------------------------------------------------
--  DDL for View VIEW_DOCUMENTS_HISTORY
--------------------------------------------------------

  CREATE OR REPLACE FORCE VIEW "AFS"."VIEW_DOCUMENTS_HISTORY" ("OBJECTS_ID", "OBJECTS_TYPE", "DOCUMENTS_AKT", "DOC_OBJECTS_ID", "DOC_OBJECTS_TYPE", "DOCUMENTS_SERIAL", "DOCUMENTS_NUMBER", "DOCUMENTS_DATE", "DOCUMENTS_ORGS", "DOCUMENTS_ORGS_NAME", "DOCUMENTS_TYPE", "DOCUMENTS_NAME", "DOCUMENTS_VID", "DOCUMENTS_CREATED", "MODIFICATION_DATE", "DOCUMENTS_HISTORY_ID", "DOCUMENTS_DEPT_CODE") AS 
  SELECT
dh.objects_id,
dh.objects_type,
dh.documents_akt,
vd.documents_id as doc_objects_id,
vd.doc_objects_type,
vd.documents_serial,
vd.documents_number,
documents_date,
vd.documents_orgs,
vd.documents_orgs_name,
vd.documents_type,
vd.documents_name,
vd.documents_vid,
dh.documents_created,
dh.modification_date,
dh.documents_history_id,
vd.documents_dept_code
FROM CPD.documents_history dh
INNER JOIN KREDIT.view_documents vd ON dh.documents_id = vd.documents_id;
--------------------------------------------------------
--  DDL for View VIEW_FIO_HISTORY
--------------------------------------------------------

  CREATE OR REPLACE FORCE VIEW "AFS"."VIEW_FIO_HISTORY" ("FIO_OBJECTS_ID", "FIO_OBJECTS_TYPE", "FIO_ID", "FIO_AKT", "LNAME", "FNAME", "MNAME", "FIO_CREATED", "MODIFICATION_DATE", "LNAME4SEARCH", "FIO4SEARCH", "FIO_HISTORY_PK") AS 
  SELECT --/*+ INDEX(MNAME) */

fh.objects_id as fio_objects_id,
fh.objects_type as fio_objects_type,
fh.fio_id,
fh.fio_akt,
ln.lname as lname,
fn.fname as fname,
mn.mname as mname,
fh.fio_created as fio_created,
fh.MODIFICATION_DATE,
ln.lname4search,
f.fio4search,
fh.FIO_HISTORY_PK

FROM CPD.fio_history fh, CPD.fio f, CPD.mname mn, CPD.lname ln, CPD.fname fn
where
      f.mname_id = mn.mname_id and
      f.fname_id = fn.fname_id and
      f.lname_id = ln.lname_id and
      fh.fio_id = f.fio_id;
--------------------------------------------------------
--  DDL for View VIEW_REQUEST_PERSON_INFO
--------------------------------------------------------

  CREATE OR REPLACE FORCE VIEW "AFS"."VIEW_REQUEST_PERSON_INFO" ("REQUEST_ID", "OBJECTS_ID", "CREATED_DATE", "MODIFICATION_DATE", "FIO", "FIO_CREATED", "FIO_MOD", "BIRTH", "BIRTH_CREATED", "BIRTH_MOD", "DOCUMENTS_SERIAL", "DOCUMENTS_NUMBER", "DOCUMENTS_AKT", "DOCUMENTS_DATE", "ADDRESS_AKT_FMG", "ADDRESS_CREATED_FMG", "ADDRESS_MOD_FMG", "ADDRESS_FMG", "REGIONS_NAMES_FMG", "AREAS_NAMES_FMG", "CITIES_NAMES_FMG", "STREETS_NAMES_FMG", "BUILD_FMG", "HOUSE_FMG", "FLAT_FMG", "PHONE_FMG", "DOC_OBJECTS_ID", "ADDRESS_AKT_PMG", "ADDRESS_CREATED_PMG", "ADDRESS_MOD_PMG", "ADDRESS_PMG", "REGIONS_NAMES_PMG", "AREAS_NAMES_PMG", "CITIES_NAMES_PMG", "STREETS_NAMES_PMG", "BUILD_PMG", "HOUSE_PMG", "FLAT_PMG", "PHONE_PMG", "PHONE_MOB") AS 
  SELECT cr.REQUEST_ID
    ,cr.OBJECTS_ID
    ,cr.CREATED_DATE
    ,cr.MODIFICATION_DATE
    ,vfio.FIO4SEARCH AS FIO
    ,vfio.FIO_CREATED
    ,vfio.MODIFICATION_DATE AS FIO_MOD
    ,vb.BIRTH
    ,vb.BIRTH_CREATED
    ,vb.MODIFICATION_DATE AS BIRTH_MOD
    ,vdh.DOCUMENTS_SERIAL
    ,vdh.DOCUMENTS_NUMBER
    ,vdh.DOCUMENTS_AKT
    ,vdh.DOCUMENTS_DATE
    /*,vdh.DOCUMENTS_TYPE
    ,vdh.DOCUMENTS_CREATED
    ,vdh.MODIFICATION_DATE AS DOCUMENTS_MOD*/
    ,vah_FMG.ADDRESS_AKT as ADDRESS_AKT_FMG
    ,vah_FMG.ADDRESS_CREATED AS ADDRESS_CREATED_FMG
    ,vah_FMG.MODIFICATION_DATE AS ADDRESS_MOD_FMG
    ,vah_FMG.ADDRESS_STR AS ADDRESS_FMG
    ,vah_FMG.regions_names AS REGIONS_NAMES_FMG
    ,vah_FMG.areas_names AS areas_names_FMG
    ,vah_FMG.cities_names AS cities_names_FMG
    ,vah_FMG.streets_names AS streets_names_FMG
    ,vah_FMG.build AS build_FMG
    ,vah_FMG.house AS house_FMG
    ,vah_FMG.flat AS flat_FMG
    ,ph_fmg.PHONE AS PHONE_FMG
    ,vdh.DOC_OBJECTS_ID
    ,vah_PMG.ADDRESS_AKT as ADDRESS_AKT_PMG
    ,vah_PMG.ADDRESS_CREATED AS ADDRESS_CREATED_PMG
    ,vah_PMG.MODIFICATION_DATE AS ADDRESS_MOD_PMG
    ,vah_PMG.ADDRESS_STR AS ADDRESS_PMG
    ,vah_PMG.regions_names AS REGIONS_NAMES_PMG
    ,vah_PMG.areas_names AS areas_names_PMG
    ,vah_PMG.cities_names AS cities_names_PMG
    ,vah_PMG.streets_names AS streets_names_PMG
    ,vah_PMG.build AS build_PMG
    ,vah_PMG.house AS house_PMG
    ,vah_PMG.flat AS flat_PMG
    ,ph_pmg.PHONE AS PHONE_PMG
    ,ph_mob.PHONE AS PHONE_MOB
    /*,DENSE_RANK() OVER (PARTITION BY cr.REQUEST_ID, vfio.FIO_OBJECTS_ID 
                ORDER BY vfio.fio_akt DESC, vfio.FIO_CREATED DESC, vfio.MODIFICATION_DATE DESC) -1 
      +DENSE_RANK() OVER (PARTITION BY cr.REQUEST_ID, vb.OBJECTS_ID 
                    ORDER BY vb.BIRTH_AKT DESC, NVL(vb.MODIFICATION_DATE, SYSDATE) DESC
                      , vb.BIRTH_CREATED DESC ) -1
      +DENSE_RANK() OVER (PARTITION BY cr.REQUEST_ID, vdh.OBJECTS_ID 
                ORDER BY vdh.DOCUMENTS_AKT DESC, NVL(vdh.MODIFICATION_DATE, SYSDATE) DESC
                  , vdh.DOCUMENTS_CREATED DESC, vdh.DOCUMENTS_HISTORY_ID DESC ) -1
      +DENSE_RANK() OVER (PARTITION BY cr.REQUEST_ID, vah_FMG.objects_id, vah_FMG.OBJECTS_TYPE
                ORDER BY vah_FMG.address_akt DESC, vah_FMG.address_created DESC
                  , NVL(vah_FMG.MODIFICATION_DATE, SYSDATE) DESC, vah_FMG.ADDRESS_HISTORY_ID DESC) -1  
      +DENSE_RANK() OVER (PARTITION BY cr.REQUEST_ID, vah_PMG.objects_id, vah_PMG.OBJECTS_TYPE
                ORDER BY vah_PMG.address_akt DESC, vah_PMG.address_created DESC
                  , NVL(vah_PMG.MODIFICATION_DATE, SYSDATE) DESC, vah_PMG.ADDRESS_HISTORY_ID DESC) -1  
      +DENSE_RANK() OVER(PARTITION BY ph_fmg.OBJECTS_ID   --по каждому адресу последний телефон
                ORDER BY ph_fmg.PHONES_AKT DESC, COALESCE(ph_fmg.MODIFICATION_DATE, SYSDATE) DESC, ph_fmg.PHONES_CREATED DESC) -1
      +DENSE_RANK() OVER(PARTITION BY ph_pmg.OBJECTS_ID   --по каждому адресу последний телефон
                ORDER BY ph_pmg.PHONES_AKT DESC, COALESCE(ph_pmg.MODIFICATION_DATE, SYSDATE) DESC, ph_pmg.PHONES_CREATED DESC) -1
      +DENSE_RANK() OVER(PARTITION BY ph_mob.OBJECTS_ID   --по каждому адресу последний телефон
                ORDER BY ph_mob.PHONES_AKT DESC, COALESCE(ph_mob.MODIFICATION_DATE, SYSDATE) DESC, ph_mob.PHONES_CREATED DESC) -1
      as RANK*/
  FROM KREDIT.C_REQUEST cr
  INNER JOIN AFS.VIEW_FIO_HISTORY vfio
    ON vfio.FIO_OBJECTS_ID = cr.OBJECTS_ID AND vfio.FIO_CREATED < cr.CREATED_DATE+1
  INNER JOIN AFS.VIEW_BIRTH vb
    ON vb.OBJECTS_ID = cr.OBJECTS_ID AND cr.CREATED_DATE+1>vb.BIRTH_CREATED
  LEFT JOIN AFS.VIEW_DOCUMENTS_HISTORY vdh
    ON cr.OBJECTS_ID=vdh.OBJECTS_ID AND vdh.DOCUMENTS_TYPE=21 AND cr.CREATED_DATE+1>vdh.DOCUMENTS_CREATED
       AND cr.CREATED_DATE+1>vdh.DOCUMENTS_DATE
  LEFT JOIN AFS.VIEW_ADDRESS_HISTORY vah_FMG
    ON vah_FMG.OBJECTS_ID=cr.OBJECTS_ID AND vah_FMG.OBJECTS_TYPE=2 AND cr.CREATED_DATE+1>vah_FMG.address_created
  LEFT JOIN AFS.VIEW_ADDRESS_HISTORY vah_PMG --присоединяем адрес прописки
    ON vah_PMG.OBJECTS_ID=vdh.DOC_OBJECTS_ID AND vah_PMG.OBJECTS_TYPE=5 AND cr.CREATED_DATE+1>vah_PMG.address_created
  LEFT JOIN CPD.PHONES ph_fmg
    ON ph_fmg.OBJECTS_ID=vah_FMG.ADDRESS_ID AND ph_fmg.OBJECTS_TYPE=8 AND cr.CREATED_DATE+1>ph_fmg.phones_created
  LEFT JOIN CPD.PHONES ph_pmg
    ON ph_pmg.OBJECTS_ID=vah_PMG.ADDRESS_ID AND ph_pmg.OBJECTS_TYPE=8 AND cr.CREATED_DATE+1>ph_pmg.phones_created
  LEFT JOIN CPD.PHONES ph_mob
    ON ph_mob.OBJECTS_ID=cr.OBJECTS_ID AND ph_mob.OBJECTS_TYPE=2 AND cr.CREATED_DATE+1>ph_mob.phones_created
  --WHERE cr.OBJECTS_ID=22477470;
--------------------------------------------------------
--  DDL for View VR_DOC
--------------------------------------------------------

  CREATE OR REPLACE FORCE VIEW "AFS"."VR_DOC" ("REQUEST_ID", "OBJECTS_ID", "CREATED_DATE", "MODIFICATION_DATE", "CREATED_GROUP_ID", "STATUS_ID", "SCORE_TREE_ROUTE_ID", "TYPE_REQUEST_ID", "DOCUMENTS_SERIAL", "DOCUMENTS_NUMBER", "DOCUMENTS_AKT", "DOCUMENTS_TYPE", "DOCUMENTS_DATE", "DOCUMENTS_CREATED", "DOCUMENTS_MOD", "DOCUMENTS_HISTORY_ID") AS 
  SELECT cr.REQUEST_ID
    ,cr.OBJECTS_ID
    ,cr.CREATED_DATE
    ,cr.MODIFICATION_DATE
    ,cr.CREATED_GROUP_ID
,cr.STATUS_ID
,cr.SCORE_TREE_ROUTE_ID
,cr.TYPE_REQUEST_ID
    /*,vfio.FIO4SEARCH AS FIO
    ,vfio.FIO_AKT
    ,vfio.FIO_CREATED
    ,vfio.MODIFICATION_DATE AS FIO_MOD
    ,vb.BIRTH
    ,vb.BIRTH_AKT
    ,vb.BIRTH_CREATED
    ,vb.MODIFICATION_DATE AS BIRTH_MOD*/
    ,vdh.DOCUMENTS_SERIAL
    ,vdh.DOCUMENTS_NUMBER
    ,vdh.DOCUMENTS_AKT
    ,vdh.DOCUMENTS_TYPE
    ,vdh.DOCUMENTS_DATE
    ,vdh.DOCUMENTS_CREATED
    ,vdh.MODIFICATION_DATE AS DOCUMENTS_MOD
	,vdh.DOCUMENTS_HISTORY_ID
    /*,vah_PMG.ADDRESS_HISTORY_ID
    ,vah_PMG.OBJECTS_TYPE
    ,vah_PMG.ADDRESS_AKT as ADDRESS_AKT_PMG
    ,vah_PMG.ADDRESS_CREATED AS ADDRESS_CREATED_PMG
    ,vah_PMG.MODIFICATION_DATE AS ADDRESS_MOD_PMG
    ,vah_PMG.ADDRESS_STR AS ADDRESS_PMG
    ,vah_PMG.regions_names AS REGIONS_NAMES_PMG
    ,vah_PMG.areas_names AS areas_names_PMG
    ,vah_PMG.cities_names AS cities_names_PMG
    ,vah_PMG.streets_names AS streets_names_PMG
    ,vah_PMG.build AS build_PMG
    ,vah_PMG.house AS house_PMG
    ,vah_PMG.flat AS flat_PMG
    ,COALESCE(TO_CHAR((SELECT LAST_VALUE(PHONE) OVER(PARTITION BY ph_pmg.OBJECTS_ID   --по каждому адресу последний телефон
                ORDER BY ph_pmg.PHONES_AKT DESC, COALESCE(ph_pmg.MODIFICATION_DATE, SYSDATE) DESC, ph_pmg.PHONES_CREATED DESC) -1 RANK
            FROM CPD.PHONES ph_pmg 
            WHERE OBJECTS_ID=vah_PMG.ADDRESS_ID 
              AND ph_pmg.OBJECTS_TYPE=8 AND cr.CREATED_DATE+1>ph_pmg.phones_created AND ROWNUM=1))
          ,'-') AS PHONE_PMG*/
    /*,DENSE_RANK() OVER (PARTITION BY cr.REQUEST_ID, vfio.FIO_OBJECTS_ID 
                ORDER BY vfio.fio_akt DESC, vfio.FIO_CREATED DESC, vfio.MODIFICATION_DATE DESC) -1 
      +DENSE_RANK() OVER (PARTITION BY cr.REQUEST_ID, vb.OBJECTS_ID 
                    ORDER BY vb.BIRTH_AKT DESC, NVL(vb.MODIFICATION_DATE, SYSDATE) DESC
                      , vb.BIRTH_CREATED DESC ) -1
      +DENSE_RANK() OVER (PARTITION BY cr.REQUEST_ID, vdh.OBJECTS_ID 
                ORDER BY vdh.DOCUMENTS_AKT DESC, NVL(vdh.MODIFICATION_DATE, SYSDATE) DESC
                  , vdh.DOCUMENTS_CREATED DESC, vdh.DOCUMENTS_HISTORY_ID DESC ) -1
      +DENSE_RANK() OVER (PARTITION BY cr.REQUEST_ID, vah_PMG.objects_id, vah_PMG.OBJECTS_TYPE
                ORDER BY vah_PMG.address_akt DESC, vah_PMG.address_created DESC
                  , NVL(vah_PMG.MODIFICATION_DATE, SYSDATE) DESC, vah_PMG.ADDRESS_HISTORY_ID DESC) -1  
      as RANK*/
  FROM KREDIT.C_REQUEST cr
  /*INNER JOIN AFS.VIEW_FIO_HISTORY vfio
    ON vfio.FIO_OBJECTS_ID = cr.OBJECTS_ID AND vfio.FIO_CREATED < cr.CREATED_DATE+1
  INNER JOIN AFS.VIEW_BIRTH vb
    ON vb.OBJECTS_ID = cr.OBJECTS_ID AND cr.CREATED_DATE+1>vb.BIRTH_CREATED*/
  INNER JOIN AFS.VIEW_DOCUMENTS_HISTORY vdh
    ON cr.OBJECTS_ID=vdh.OBJECTS_ID AND vdh.DOCUMENTS_TYPE=21 AND cr.CREATED_DATE+1>vdh.DOCUMENTS_CREATED
       AND cr.CREATED_DATE+1>vdh.DOCUMENTS_DATE
 /* INNER JOIN AFS.VIEW_ADDRESS_HISTORY vah_PMG --присоединяем адрес прописки
    ON vah_PMG.OBJECTS_ID=vdh.DOC_OBJECTS_ID AND vah_PMG.OBJECTS_TYPE=5 AND cr.CREATED_DATE+1>vah_PMG.address_created*/
  --WHERE cr.OBJECTS_ID=22477470;
--------------------------------------------------------
--  DDL for View VR_DOC_FB
--------------------------------------------------------

  CREATE OR REPLACE FORCE VIEW "AFS"."VR_DOC_FB" ("REQUEST_ID", "OBJECTS_ID", "CREATED_DATE", "MODIFICATION_DATE", "CREATED_GROUP_ID", "STATUS_ID", "SCORE_TREE_ROUTE_ID", "TYPE_REQUEST_ID", "FIO", "FIO_AKT", "FIO_CREATED", "FIO_MOD", "BIRTH", "BIRTH_AKT", "BIRTH_CREATED", "BIRTH_MOD", "DOCUMENTS_SERIAL", "DOCUMENTS_NUMBER", "DOCUMENTS_AKT", "DOCUMENTS_TYPE", "DOCUMENTS_DATE", "DOCUMENTS_CREATED", "DOCUMENTS_MOD", "DOCUMENTS_HISTORY_ID") AS 
  SELECT cr.REQUEST_ID
    ,cr.OBJECTS_ID
    ,cr.CREATED_DATE
    ,cr.MODIFICATION_DATE
    ,cr.CREATED_GROUP_ID
,cr.STATUS_ID
,cr.SCORE_TREE_ROUTE_ID
,cr.TYPE_REQUEST_ID
    ,vfio.FIO4SEARCH AS FIO
    ,vfio.FIO_AKT
    ,vfio.FIO_CREATED
    ,vfio.MODIFICATION_DATE AS FIO_MOD
    ,vb.BIRTH
    ,vb.BIRTH_AKT
    ,vb.BIRTH_CREATED
    ,vb.MODIFICATION_DATE AS BIRTH_MOD
    ,vdh.DOCUMENTS_SERIAL
    ,vdh.DOCUMENTS_NUMBER
    ,vdh.DOCUMENTS_AKT
    ,vdh.DOCUMENTS_TYPE
    ,vdh.DOCUMENTS_DATE
    ,vdh.DOCUMENTS_CREATED
    ,vdh.MODIFICATION_DATE AS DOCUMENTS_MOD
	,vdh.DOCUMENTS_HISTORY_ID
    /*,vah_PMG.ADDRESS_HISTORY_ID
    ,vah_PMG.OBJECTS_TYPE
    ,vah_PMG.ADDRESS_AKT as ADDRESS_AKT_PMG
    ,vah_PMG.ADDRESS_CREATED AS ADDRESS_CREATED_PMG
    ,vah_PMG.MODIFICATION_DATE AS ADDRESS_MOD_PMG
    ,vah_PMG.ADDRESS_STR AS ADDRESS_PMG
    ,vah_PMG.regions_names AS REGIONS_NAMES_PMG
    ,vah_PMG.areas_names AS areas_names_PMG
    ,vah_PMG.cities_names AS cities_names_PMG
    ,vah_PMG.streets_names AS streets_names_PMG
    ,vah_PMG.build AS build_PMG
    ,vah_PMG.house AS house_PMG
    ,vah_PMG.flat AS flat_PMG
    ,COALESCE(TO_CHAR((SELECT LAST_VALUE(PHONE) OVER(PARTITION BY ph_pmg.OBJECTS_ID   --по каждому адресу последний телефон
                ORDER BY ph_pmg.PHONES_AKT DESC, COALESCE(ph_pmg.MODIFICATION_DATE, SYSDATE) DESC, ph_pmg.PHONES_CREATED DESC) -1 RANK
            FROM CPD.PHONES ph_pmg 
            WHERE OBJECTS_ID=vah_PMG.ADDRESS_ID 
              AND ph_pmg.OBJECTS_TYPE=8 AND cr.CREATED_DATE+1>ph_pmg.phones_created AND ROWNUM=1))
          ,'-') AS PHONE_PMG*/
    /*,DENSE_RANK() OVER (PARTITION BY cr.REQUEST_ID, vfio.FIO_OBJECTS_ID 
                ORDER BY vfio.fio_akt DESC, vfio.FIO_CREATED DESC, vfio.MODIFICATION_DATE DESC) -1 
      +DENSE_RANK() OVER (PARTITION BY cr.REQUEST_ID, vb.OBJECTS_ID 
                    ORDER BY vb.BIRTH_AKT DESC, NVL(vb.MODIFICATION_DATE, SYSDATE) DESC
                      , vb.BIRTH_CREATED DESC ) -1
      +DENSE_RANK() OVER (PARTITION BY cr.REQUEST_ID, vdh.OBJECTS_ID 
                ORDER BY vdh.DOCUMENTS_AKT DESC, NVL(vdh.MODIFICATION_DATE, SYSDATE) DESC
                  , vdh.DOCUMENTS_CREATED DESC, vdh.DOCUMENTS_HISTORY_ID DESC ) -1
      +DENSE_RANK() OVER (PARTITION BY cr.REQUEST_ID, vah_PMG.objects_id, vah_PMG.OBJECTS_TYPE
                ORDER BY vah_PMG.address_akt DESC, vah_PMG.address_created DESC
                  , NVL(vah_PMG.MODIFICATION_DATE, SYSDATE) DESC, vah_PMG.ADDRESS_HISTORY_ID DESC) -1  
      as RANK*/
  FROM KREDIT.C_REQUEST cr
  INNER JOIN AFS.VIEW_FIO_HISTORY vfio
    ON vfio.FIO_OBJECTS_ID = cr.OBJECTS_ID AND vfio.FIO_CREATED < cr.CREATED_DATE+1
  INNER JOIN AFS.VIEW_BIRTH vb
    ON vb.OBJECTS_ID = cr.OBJECTS_ID AND cr.CREATED_DATE+1>vb.BIRTH_CREATED
  INNER JOIN AFS.VIEW_DOCUMENTS_HISTORY vdh
    ON cr.OBJECTS_ID=vdh.OBJECTS_ID AND vdh.DOCUMENTS_TYPE=21 AND cr.CREATED_DATE+1>vdh.DOCUMENTS_CREATED
       AND cr.CREATED_DATE+1>vdh.DOCUMENTS_DATE
 /* INNER JOIN AFS.VIEW_ADDRESS_HISTORY vah_PMG --присоединяем адрес прописки
    ON vah_PMG.OBJECTS_ID=vdh.DOC_OBJECTS_ID AND vah_PMG.OBJECTS_TYPE=5 AND cr.CREATED_DATE+1>vah_PMG.address_created*/
  --WHERE cr.OBJECTS_ID=22477470;
--------------------------------------------------------
--  DDL for View V_REQUEST_PERSON_INFO
--------------------------------------------------------

  CREATE OR REPLACE FORCE VIEW "AFS"."V_REQUEST_PERSON_INFO" ("REQUEST_ID", "OBJECTS_ID", "CREATED_DATE", "MODIFICATION_DATE", "FIO", "FIO_CREATED", "FIO_MOD", "BIRTH", "BIRTH_CREATED", "BIRTH_MOD", "DOCUMENTS_SERIAL", "DOCUMENTS_NUMBER", "DOCUMENTS_AKT", "DOCUMENTS_DATE", "ADDRESS_AKT_FMG", "ADDRESS_CREATED_FMG", "ADDRESS_MOD_FMG", "ADDRESS_FMG", "REGIONS_NAMES_FMG", "AREAS_NAMES_FMG", "CITIES_NAMES_FMG", "STREETS_NAMES_FMG", "BUILD_FMG", "HOUSE_FMG", "FLAT_FMG", "PHONES_FMG", "DOC_OBJECTS_ID", "ADDRESS_AKT_PMG", "ADDRESS_CREATED_PMG", "ADDRESS_MOD_PMG", "ADDRESS_PMG", "REGIONS_NAMES_PMG", "AREAS_NAMES_PMG", "CITIES_NAMES_PMG", "STREETS_NAMES_PMG", "BUILD_PMG", "HOUSE_PMG", "FLAT_PMG", "PHONES_PMG", "PHONES_MOB", "RANK") AS 
  SELECT cr.REQUEST_ID
    ,cr.OBJECTS_ID
    ,cr.CREATED_DATE
    ,cr.MODIFICATION_DATE
    ,vfio.FIO4SEARCH AS FIO
    ,vfio.FIO_CREATED
    ,vfio.MODIFICATION_DATE AS FIO_MOD
    ,vb.BIRTH
    ,vb.BIRTH_CREATED
    ,vb.MODIFICATION_DATE AS BIRTH_MOD
    ,vdh.DOCUMENTS_SERIAL
    ,vdh.DOCUMENTS_NUMBER
    ,vdh.DOCUMENTS_AKT
    ,vdh.DOCUMENTS_DATE
    /*,vdh.DOCUMENTS_TYPE
    ,vdh.DOCUMENTS_CREATED
    ,vdh.MODIFICATION_DATE AS DOCUMENTS_MOD*/
    ,vah_FMG.ADDRESS_AKT as ADDRESS_AKT_FMG
    ,vah_FMG.ADDRESS_CREATED AS ADDRESS_CREATED_FMG
    ,vah_FMG.MODIFICATION_DATE AS ADDRESS_MOD_FMG
    ,vah_FMG.ADDRESS_STR AS ADDRESS_FMG
    ,vah_FMG.regions_names AS REGIONS_NAMES_FMG
    ,vah_FMG.areas_names AS areas_names_FMG
    ,vah_FMG.cities_names AS cities_names_FMG
    ,vah_FMG.streets_names AS streets_names_FMG
    ,vah_FMG.build AS build_FMG
    ,vah_FMG.house AS house_FMG
    ,vah_FMG.flat AS flat_FMG
    ,ph_fmg.PHONE AS PHONES_FMG
    ,vdh.DOC_OBJECTS_ID
    ,vah_PMG.ADDRESS_AKT as ADDRESS_AKT_PMG
    ,vah_PMG.ADDRESS_CREATED AS ADDRESS_CREATED_PMG
    ,vah_PMG.MODIFICATION_DATE AS ADDRESS_MOD_PMG
    ,vah_PMG.ADDRESS_STR AS ADDRESS_PMG
    ,vah_PMG.regions_names AS REGIONS_NAMES_PMG
    ,vah_PMG.areas_names AS areas_names_PMG
    ,vah_PMG.cities_names AS cities_names_PMG
    ,vah_PMG.streets_names AS streets_names_PMG
    ,vah_PMG.build AS build_PMG
    ,vah_PMG.house AS house_PMG
    ,vah_PMG.flat AS flat_PMG
    ,ph_pmg.PHONE AS PHONES_PMG
    ,ph_mob.PHONE AS PHONES_MOB
    ,DENSE_RANK() OVER (PARTITION BY cr.REQUEST_ID, vfio.FIO_OBJECTS_ID 
                ORDER BY vfio.fio_akt DESC, vfio.FIO_CREATED DESC, vfio.MODIFICATION_DATE DESC) -1 
      +DENSE_RANK() OVER (PARTITION BY cr.REQUEST_ID, vb.OBJECTS_ID 
                    ORDER BY vb.BIRTH_AKT DESC, NVL(vb.MODIFICATION_DATE, SYSDATE) DESC
                      , vb.BIRTH_CREATED DESC ) -1
      +DENSE_RANK() OVER (PARTITION BY cr.REQUEST_ID, vdh.OBJECTS_ID 
                ORDER BY vdh.DOCUMENTS_AKT DESC, NVL(vdh.MODIFICATION_DATE, SYSDATE) DESC
                  , vdh.DOCUMENTS_CREATED DESC, vdh.DOCUMENTS_HISTORY_ID DESC ) -1
      +DENSE_RANK() OVER (PARTITION BY cr.REQUEST_ID, vah_FMG.objects_id, vah_FMG.OBJECTS_TYPE
                ORDER BY vah_FMG.address_akt DESC, vah_FMG.address_created DESC
                  , NVL(vah_FMG.MODIFICATION_DATE, SYSDATE) DESC, vah_FMG.ADDRESS_HISTORY_ID DESC) -1  
      +DENSE_RANK() OVER (PARTITION BY cr.REQUEST_ID, vah_PMG.objects_id, vah_PMG.OBJECTS_TYPE
                ORDER BY vah_PMG.address_akt DESC, vah_PMG.address_created DESC
                  , NVL(vah_PMG.MODIFICATION_DATE, SYSDATE) DESC, vah_PMG.ADDRESS_HISTORY_ID DESC) -1  
      +DENSE_RANK() OVER(PARTITION BY ph_fmg.OBJECTS_ID   --по каждому адресу последний телефон
                ORDER BY ph_fmg.PHONES_AKT DESC, COALESCE(ph_fmg.MODIFICATION_DATE, SYSDATE) DESC, ph_fmg.PHONES_CREATED DESC) -1
      +DENSE_RANK() OVER(PARTITION BY ph_pmg.OBJECTS_ID   --по каждому адресу последний телефон
                ORDER BY ph_pmg.PHONES_AKT DESC, COALESCE(ph_pmg.MODIFICATION_DATE, SYSDATE) DESC, ph_pmg.PHONES_CREATED DESC) -1
      +DENSE_RANK() OVER(PARTITION BY ph_mob.OBJECTS_ID   --по каждому адресу последний телефон
                ORDER BY ph_mob.PHONES_AKT DESC, COALESCE(ph_mob.MODIFICATION_DATE, SYSDATE) DESC, ph_mob.PHONES_CREATED DESC) -1
      as RANK
  FROM KREDIT.C_REQUEST cr, AFS.VIEW_FIO_HISTORY vfio, AFS.VIEW_BIRTH vb, AFS.VIEW_DOCUMENTS_HISTORY vdh
      ,AFS.VIEW_ADDRESS_HISTORY vah_FMG, AFS.VIEW_ADDRESS_HISTORY vah_PMG --присоединяем адрес прописки
      ,CPD.PHONES ph_fmg, CPD.PHONES ph_pmg, CPD.PHONES ph_mob
    WHERE vfio.FIO_OBJECTS_ID = cr.OBJECTS_ID AND vfio.FIO_CREATED < cr.CREATED_DATE+1
    AND vb.OBJECTS_ID = cr.OBJECTS_ID AND cr.CREATED_DATE+1>vb.BIRTH_CREATED
    AND cr.OBJECTS_ID=vdh.OBJECTS_ID AND vdh.DOCUMENTS_TYPE=21 AND cr.CREATED_DATE+1>vdh.DOCUMENTS_CREATED
       AND cr.CREATED_DATE+1>vdh.DOCUMENTS_DATE
    AND vah_FMG.OBJECTS_ID=cr.OBJECTS_ID AND vah_FMG.OBJECTS_TYPE=2 AND cr.CREATED_DATE+1>vah_FMG.address_created
    AND vah_PMG.OBJECTS_ID=vdh.DOC_OBJECTS_ID AND vah_PMG.OBJECTS_TYPE=5 AND cr.CREATED_DATE+1>vah_PMG.address_created
    AND ph_fmg.OBJECTS_ID=vah_FMG.ADDRESS_ID AND ph_fmg.OBJECTS_TYPE=8 AND cr.CREATED_DATE+1>ph_fmg.phones_created
    AND ph_pmg.OBJECTS_ID=vah_PMG.ADDRESS_ID AND ph_pmg.OBJECTS_TYPE=8 AND cr.CREATED_DATE+1>ph_pmg.phones_created
    AND ph_mob.OBJECTS_ID=cr.OBJECTS_ID AND ph_mob.OBJECTS_TYPE=2 AND cr.CREATED_DATE+1>ph_mob.phones_created
  --WHERE cr.OBJECTS_ID=22477470;
--------------------------------------------------------
--  DDL for View VR_FIO_BIRTH
--------------------------------------------------------

  CREATE OR REPLACE FORCE VIEW "AFS"."VR_FIO_BIRTH" ("REQUEST_ID", "OBJECTS_ID", "CREATED_DATE", "MODIFICATION_DATE", "CREATED_GROUP_ID", "STATUS_ID", "SCORE_TREE_ROUTE_ID", "TYPE_REQUEST_ID", "FIO", "FIO_AKT", "FIO_CREATED", "FIO_MOD", "BIRTH", "BIRTH_AKT", "BIRTH_CREATED", "BIRTH_MOD") AS 
  SELECT cr.REQUEST_ID
    ,cr.OBJECTS_ID
    ,cr.CREATED_DATE
    ,cr.MODIFICATION_DATE
    ,cr.CREATED_GROUP_ID
,cr.STATUS_ID
,cr.SCORE_TREE_ROUTE_ID
,cr.TYPE_REQUEST_ID
    ,vfio.FIO4SEARCH AS FIO
    ,vfio.FIO_AKT
    ,vfio.FIO_CREATED
    ,vfio.MODIFICATION_DATE AS FIO_MOD
    ,vb.BIRTH
    ,vb.BIRTH_AKT
    ,vb.BIRTH_CREATED
    ,vb.MODIFICATION_DATE AS BIRTH_MOD
  FROM KREDIT.C_REQUEST cr
  INNER JOIN AFS.VIEW_FIO_HISTORY vfio
    ON vfio.FIO_OBJECTS_ID = cr.OBJECTS_ID AND vfio.FIO_CREATED < cr.CREATED_DATE+10
  INNER JOIN AFS.VIEW_BIRTH vb
    ON vb.OBJECTS_ID = cr.OBJECTS_ID AND cr.CREATED_DATE+10>vb.BIRTH_CREATED
  --WHERE cr.OBJECTS_ID=22477470;
--------------------------------------------------------
--  DDL for View VR_FMG
--------------------------------------------------------

  CREATE OR REPLACE FORCE VIEW "AFS"."VR_FMG" ("REQUEST_ID", "OBJECTS_ID", "CREATED_DATE", "MODIFICATION_DATE", "CREATED_GROUP_ID", "STATUS_ID", "SCORE_TREE_ROUTE_ID", "TYPE_REQUEST_ID", "ADDRESS_HISTORY_ID", "OBJECTS_TYPE", "ADDRESS_AKT", "ADDRESS_CREATED", "ADDRESS_MOD", "ADDRESS_STR", "REGIONS_NAMES", "AREAS_NAMES", "CITIES_NAMES", "STREETS_NAMES", "HOUSE", "BUILD", "FLAT") AS 
  SELECT cr.REQUEST_ID
    ,cr.OBJECTS_ID
    ,cr.CREATED_DATE
    ,cr.MODIFICATION_DATE
    ,cr.CREATED_GROUP_ID
,cr.STATUS_ID
,cr.SCORE_TREE_ROUTE_ID
,cr.TYPE_REQUEST_ID
    ,vah_FMG.ADDRESS_HISTORY_ID
    ,vah_FMG.OBJECTS_TYPE
    ,vah_FMG.ADDRESS_AKT
    ,vah_FMG.ADDRESS_CREATED
    ,vah_FMG.MODIFICATION_DATE AS ADDRESS_MOD
    ,vah_FMG.ADDRESS_STR 
    ,vah_FMG.regions_names
    ,vah_FMG.areas_names
    ,vah_FMG.cities_names
    ,vah_FMG.streets_names
    ,vah_FMG.house 
    ,vah_FMG.build
    ,vah_FMG.flat
    /*,DENSE_RANK() OVER (PARTITION BY cr.REQUEST_ID, vfio.FIO_OBJECTS_ID 
                ORDER BY vfio.fio_akt DESC, vfio.FIO_CREATED DESC, vfio.MODIFICATION_DATE DESC) -1 
      +DENSE_RANK() OVER (PARTITION BY cr.REQUEST_ID, vb.OBJECTS_ID 
                    ORDER BY vb.BIRTH_AKT DESC, NVL(vb.MODIFICATION_DATE, SYSDATE) DESC
                      , vb.BIRTH_CREATED DESC ) -1
      +DENSE_RANK() OVER (PARTITION BY cr.REQUEST_ID, vah_FMG.objects_id, vah_FMG.OBJECTS_TYPE
                ORDER BY vah_FMG.address_akt DESC, vah_FMG.address_created DESC
                  , NVL(vah_FMG.MODIFICATION_DATE, SYSDATE) DESC, vah_FMG.ADDRESS_HISTORY_ID DESC) -1
      as RANK*/
  FROM KREDIT.C_REQUEST cr
  INNER JOIN AFS.VIEW_ADDRESS_HISTORY vah_FMG
    ON vah_FMG.OBJECTS_ID=cr.OBJECTS_ID AND vah_FMG.OBJECTS_TYPE=2 AND cr.CREATED_DATE+10>vah_FMG.address_created
  --WHERE cr.OBJECTS_ID=22477470;
--------------------------------------------------------
--  DDL for View VR_FMG_FB
--------------------------------------------------------

  CREATE OR REPLACE FORCE VIEW "AFS"."VR_FMG_FB" ("REQUEST_ID", "OBJECTS_ID", "CREATED_DATE", "MODIFICATION_DATE", "CREATED_GROUP_ID", "STATUS_ID", "SCORE_TREE_ROUTE_ID", "TYPE_REQUEST_ID", "FIO", "FIO_AKT", "FIO_CREATED", "FIO_MOD", "BIRTH", "BIRTH_AKT", "BIRTH_CREATED", "BIRTH_MOD", "ADDRESS_HISTORY_ID", "OBJECTS_TYPE", "ADDRESS_AKT", "ADDRESS_CREATED", "ADDRESS_MOD", "ADDRESS_STR", "REGIONS_NAMES", "AREAS_NAMES", "CITIES_NAMES", "STREETS_NAMES", "HOUSE", "BUILD", "FLAT") AS 
  SELECT cr.REQUEST_ID
    ,cr.OBJECTS_ID
    ,cr.CREATED_DATE
    ,cr.MODIFICATION_DATE
    ,cr.CREATED_GROUP_ID
,cr.STATUS_ID
,cr.SCORE_TREE_ROUTE_ID
,cr.TYPE_REQUEST_ID
    ,vfio.FIO4SEARCH AS FIO
    ,vfio.FIO_AKT
    ,vfio.FIO_CREATED
    ,vfio.MODIFICATION_DATE AS FIO_MOD
    ,vb.BIRTH
    ,vb.BIRTH_AKT
    ,vb.BIRTH_CREATED
    ,vb.MODIFICATION_DATE AS BIRTH_MOD
    ,vah_FMG.ADDRESS_HISTORY_ID
    ,vah_FMG.OBJECTS_TYPE
    ,vah_FMG.ADDRESS_AKT
    ,vah_FMG.ADDRESS_CREATED
    ,vah_FMG.MODIFICATION_DATE AS ADDRESS_MOD
    ,vah_FMG.ADDRESS_STR 
    ,vah_FMG.regions_names
    ,vah_FMG.areas_names
    ,vah_FMG.cities_names
    ,vah_FMG.streets_names
    ,vah_FMG.house 
    ,vah_FMG.build
    ,vah_FMG.flat

    /*,DENSE_RANK() OVER (PARTITION BY cr.REQUEST_ID, vfio.FIO_OBJECTS_ID 
                ORDER BY vfio.fio_akt DESC, vfio.FIO_CREATED DESC, vfio.MODIFICATION_DATE DESC) -1 
      +DENSE_RANK() OVER (PARTITION BY cr.REQUEST_ID, vb.OBJECTS_ID 
                    ORDER BY vb.BIRTH_AKT DESC, NVL(vb.MODIFICATION_DATE, SYSDATE) DESC
                      , vb.BIRTH_CREATED DESC ) -1
      +DENSE_RANK() OVER (PARTITION BY cr.REQUEST_ID, vah_FMG.objects_id, vah_FMG.OBJECTS_TYPE
                ORDER BY vah_FMG.address_akt DESC, vah_FMG.address_created DESC
                  , NVL(vah_FMG.MODIFICATION_DATE, SYSDATE) DESC, vah_FMG.ADDRESS_HISTORY_ID DESC) -1
      as RANK*/
  FROM KREDIT.C_REQUEST cr
  INNER JOIN AFS.VIEW_FIO_HISTORY vfio
    ON vfio.FIO_OBJECTS_ID = cr.OBJECTS_ID AND vfio.FIO_CREATED < cr.CREATED_DATE+10
  INNER JOIN AFS.VIEW_BIRTH vb
    ON vb.OBJECTS_ID = cr.OBJECTS_ID AND cr.CREATED_DATE+10>vb.BIRTH_CREATED
  INNER JOIN AFS.VIEW_ADDRESS_HISTORY vah_FMG
    ON vah_FMG.OBJECTS_ID=cr.OBJECTS_ID AND vah_FMG.OBJECTS_TYPE=2 AND cr.CREATED_DATE+10>vah_FMG.address_created
  --WHERE cr.OBJECTS_ID=22477470;
--------------------------------------------------------
--  DDL for View VR_FMG_FB_PH
--------------------------------------------------------

  CREATE OR REPLACE FORCE VIEW "AFS"."VR_FMG_FB_PH" ("REQUEST_ID", "OBJECTS_ID", "CREATED_DATE", "MODIFICATION_DATE", "CREATED_GROUP_ID", "STATUS_ID", "SCORE_TREE_ROUTE_ID", "TYPE_REQUEST_ID", "FIO", "FIO_AKT", "FIO_CREATED", "FIO_MOD", "BIRTH", "BIRTH_AKT", "BIRTH_CREATED", "BIRTH_MOD", "ADDRESS_HISTORY_ID", "OBJECTS_TYPE", "ADDRESS_AKT", "ADDRESS_CREATED", "ADDRESS_MOD", "ADDRESS_STR", "REGIONS_NAMES", "AREAS_NAMES", "CITIES_NAMES", "STREETS_NAMES", "HOUSE", "BUILD", "FLAT", "PHONE_FMG") AS 
  SELECT cr.REQUEST_ID
    ,cr.OBJECTS_ID
    ,cr.CREATED_DATE
    ,cr.MODIFICATION_DATE
    ,cr.CREATED_GROUP_ID
,cr.STATUS_ID
,cr.SCORE_TREE_ROUTE_ID
,cr.TYPE_REQUEST_ID
    ,vfio.FIO4SEARCH AS FIO
    ,vfio.FIO_AKT
    ,vfio.FIO_CREATED
    ,vfio.MODIFICATION_DATE AS FIO_MOD
    ,vb.BIRTH
    ,vb.BIRTH_AKT
    ,vb.BIRTH_CREATED
    ,vb.MODIFICATION_DATE AS BIRTH_MOD
    ,vah_FMG.ADDRESS_HISTORY_ID
    ,vah_FMG.OBJECTS_TYPE
    ,vah_FMG.ADDRESS_AKT
    ,vah_FMG.ADDRESS_CREATED
    ,vah_FMG.MODIFICATION_DATE AS ADDRESS_MOD
    ,vah_FMG.ADDRESS_STR 
    ,vah_FMG.regions_names
    ,vah_FMG.areas_names
    ,vah_FMG.cities_names
    ,vah_FMG.streets_names
    ,vah_FMG.house 
    ,vah_FMG.build
    ,vah_FMG.flat
    ,COALESCE(TO_CHAR((SELECT LAST_VALUE(PHONE) OVER(PARTITION BY ph_fmg.OBJECTS_ID   --по каждому адресу последний телефон
                ORDER BY ph_fmg.PHONES_AKT DESC, COALESCE(ph_fmg.MODIFICATION_DATE, SYSDATE) DESC, ph_fmg.PHONES_CREATED DESC) -1 RANK
            FROM CPD.PHONES ph_fmg 
            WHERE OBJECTS_ID=vah_FMG.ADDRESS_ID 
              AND ph_fmg.OBJECTS_TYPE=8 AND cr.CREATED_DATE+1>ph_fmg.phones_created AND ROWNUM=1))
        ,'-') AS PHONE_FMG
    /*,DENSE_RANK() OVER (PARTITION BY cr.REQUEST_ID, vfio.FIO_OBJECTS_ID 
                ORDER BY vfio.fio_akt DESC, vfio.FIO_CREATED DESC, vfio.MODIFICATION_DATE DESC) -1 
      +DENSE_RANK() OVER (PARTITION BY cr.REQUEST_ID, vb.OBJECTS_ID 
                    ORDER BY vb.BIRTH_AKT DESC, NVL(vb.MODIFICATION_DATE, SYSDATE) DESC
                      , vb.BIRTH_CREATED DESC ) -1
      +DENSE_RANK() OVER (PARTITION BY cr.REQUEST_ID, vah_FMG.objects_id, vah_FMG.OBJECTS_TYPE
                ORDER BY vah_FMG.address_akt DESC, vah_FMG.address_created DESC
                  , NVL(vah_FMG.MODIFICATION_DATE, SYSDATE) DESC, vah_FMG.ADDRESS_HISTORY_ID DESC) -1
      as RANK*/
  FROM KREDIT.C_REQUEST cr
  INNER JOIN AFS.VIEW_FIO_HISTORY vfio
    ON vfio.FIO_OBJECTS_ID = cr.OBJECTS_ID AND vfio.FIO_CREATED < cr.CREATED_DATE+10
  INNER JOIN AFS.VIEW_BIRTH vb
    ON vb.OBJECTS_ID = cr.OBJECTS_ID AND cr.CREATED_DATE+10>vb.BIRTH_CREATED
  INNER JOIN AFS.VIEW_ADDRESS_HISTORY vah_FMG
    ON vah_FMG.OBJECTS_ID=cr.OBJECTS_ID AND vah_FMG.OBJECTS_TYPE=2 AND cr.CREATED_DATE+10>vah_FMG.address_created
  --WHERE cr.OBJECTS_ID=22477470;
--------------------------------------------------------
--  DDL for View VR_FMG_PH
--------------------------------------------------------

  CREATE OR REPLACE FORCE VIEW "AFS"."VR_FMG_PH" ("REQUEST_ID", "OBJECTS_ID", "CREATED_DATE", "MODIFICATION_DATE", "CREATED_GROUP_ID", "STATUS_ID", "SCORE_TREE_ROUTE_ID", "TYPE_REQUEST_ID", "ADDRESS_HISTORY_ID", "OBJECTS_TYPE", "ADDRESS_AKT", "ADDRESS_CREATED", "ADDRESS_MOD", "ADDRESS_STR", "REGIONS_NAMES", "AREAS_NAMES", "CITIES_NAMES", "STREETS_NAMES", "HOUSE", "BUILD", "FLAT", "PHONE_FMG") AS 
  SELECT cr.REQUEST_ID
    ,cr.OBJECTS_ID
    ,cr.CREATED_DATE
    ,cr.MODIFICATION_DATE
    ,cr.CREATED_GROUP_ID
,cr.STATUS_ID
,cr.SCORE_TREE_ROUTE_ID
,cr.TYPE_REQUEST_ID
    ,vah_FMG.ADDRESS_HISTORY_ID
    ,vah_FMG.OBJECTS_TYPE
    ,vah_FMG.ADDRESS_AKT
    ,vah_FMG.ADDRESS_CREATED
    ,vah_FMG.MODIFICATION_DATE AS ADDRESS_MOD
    ,vah_FMG.ADDRESS_STR 
    ,vah_FMG.regions_names
    ,vah_FMG.areas_names
    ,vah_FMG.cities_names
    ,vah_FMG.streets_names
    ,vah_FMG.house 
    ,vah_FMG.build
    ,vah_FMG.flat
    ,COALESCE(TO_CHAR((SELECT LAST_VALUE(PHONE) OVER(PARTITION BY ph_fmg.OBJECTS_ID   --по каждому адресу последний телефон
                ORDER BY ph_fmg.PHONES_AKT DESC, COALESCE(ph_fmg.MODIFICATION_DATE, SYSDATE) DESC, ph_fmg.PHONES_CREATED DESC) -1 RANK
            FROM CPD.PHONES ph_fmg 
            WHERE OBJECTS_ID=vah_FMG.ADDRESS_ID 
              AND ph_fmg.OBJECTS_TYPE=8 AND cr.CREATED_DATE+1>ph_fmg.phones_created AND ROWNUM=1))
        ,'-') AS PHONE_FMG
    /*,DENSE_RANK() OVER (PARTITION BY cr.REQUEST_ID, vfio.FIO_OBJECTS_ID 
                ORDER BY vfio.fio_akt DESC, vfio.FIO_CREATED DESC, vfio.MODIFICATION_DATE DESC) -1 
      +DENSE_RANK() OVER (PARTITION BY cr.REQUEST_ID, vb.OBJECTS_ID 
                    ORDER BY vb.BIRTH_AKT DESC, NVL(vb.MODIFICATION_DATE, SYSDATE) DESC
                      , vb.BIRTH_CREATED DESC ) -1
      +DENSE_RANK() OVER (PARTITION BY cr.REQUEST_ID, vah_FMG.objects_id, vah_FMG.OBJECTS_TYPE
                ORDER BY vah_FMG.address_akt DESC, vah_FMG.address_created DESC
                  , NVL(vah_FMG.MODIFICATION_DATE, SYSDATE) DESC, vah_FMG.ADDRESS_HISTORY_ID DESC) -1
      as RANK*/
  FROM KREDIT.C_REQUEST cr
  INNER JOIN AFS.VIEW_ADDRESS_HISTORY vah_FMG
    ON vah_FMG.OBJECTS_ID=cr.OBJECTS_ID AND vah_FMG.OBJECTS_TYPE=2 AND cr.CREATED_DATE+10>vah_FMG.address_created
  --WHERE cr.OBJECTS_ID=22477470;
--------------------------------------------------------
--  DDL for View VR_MOB
--------------------------------------------------------

  CREATE OR REPLACE FORCE VIEW "AFS"."VR_MOB" ("REQUEST_ID", "OBJECTS_ID", "CREATED_DATE", "MODIFICATION_DATE", "CREATED_GROUP_ID", "STATUS_ID", "SCORE_TREE_ROUTE_ID", "TYPE_REQUEST_ID", "PHONES_MOB", "PHONES_AKT", "PHONES_CREATED", "PHONES_MOD") AS 
  SELECT cr.REQUEST_ID
    ,cr.OBJECTS_ID
    ,cr.CREATED_DATE
    ,cr.MODIFICATION_DATE
    ,cr.CREATED_GROUP_ID
,cr.STATUS_ID
,cr.SCORE_TREE_ROUTE_ID
,cr.TYPE_REQUEST_ID
    /*,vfio.FIO4SEARCH AS FIO
    ,vfio.FIO_CREATED
    ,vfio.MODIFICATION_DATE AS FIO_MOD
    ,vb.BIRTH
    ,vb.BIRTH_CREATED
    ,vb.MODIFICATION_DATE AS BIRTH_MOD*/
    ,ph_mob.PHONE AS PHONES_MOB
    ,ph_mob.PHONES_AKT
    ,ph_mob.PHONES_CREATED
    ,ph_mob.MODIFICATION_DATE AS PHONES_MOD
    /*,DENSE_RANK() OVER(PARTITION BY ph_mob.OBJECTS_ID   --по каждому адресу последний телефон
                ORDER BY ph_mob.PHONES_AKT DESC, COALESCE(ph_mob.MODIFICATION_DATE, SYSDATE) DESC, ph_mob.PHONES_CREATED DESC) -1
      as RANK*/
  FROM CPD.PHONES ph_mob, KREDIT.C_REQUEST cr--, AFS.VIEW_FIO_HISTORY vfio, AFS.VIEW_BIRTH vb
  WHERE /*vfio.FIO_OBJECTS_ID = cr.OBJECTS_ID AND vfio.FIO_CREATED < cr.CREATED_DATE+10
      AND vb.OBJECTS_ID = cr.OBJECTS_ID AND cr.CREATED_DATE+10>vb.BIRTH_CREATED
      AND*/ ph_mob.OBJECTS_ID=cr.OBJECTS_ID AND ph_mob.OBJECTS_TYPE=2 AND cr.CREATED_DATE+10>ph_mob.phones_created
  --AND cr.OBJECTS_ID=22477470;
--------------------------------------------------------
--  DDL for View VR_MOB_FB
--------------------------------------------------------

  CREATE OR REPLACE FORCE VIEW "AFS"."VR_MOB_FB" ("REQUEST_ID", "OBJECTS_ID", "CREATED_DATE", "MODIFICATION_DATE", "CREATED_GROUP_ID", "STATUS_ID", "SCORE_TREE_ROUTE_ID", "TYPE_REQUEST_ID", "FIO", "FIO_AKT", "FIO_CREATED", "FIO_MOD", "BIRTH", "BIRTH_AKT", "BIRTH_CREATED", "BIRTH_MOD", "OBJECTS_TYPE", "PHONE_MOB", "PHONES_AKT", "PHONES_CREATED", "PHONES_MOD") AS 
  SELECT cr.REQUEST_ID
    ,cr.OBJECTS_ID
    ,cr.CREATED_DATE
    ,cr.MODIFICATION_DATE
    ,cr.CREATED_GROUP_ID
,cr.STATUS_ID
,cr.SCORE_TREE_ROUTE_ID
,cr.TYPE_REQUEST_ID
    ,vfio.FIO4SEARCH AS FIO
    ,vfio.FIO_AKT
    ,vfio.FIO_CREATED
    ,vfio.MODIFICATION_DATE AS FIO_MOD
    ,vb.BIRTH
    ,vb.BIRTH_AKT
    ,vb.BIRTH_CREATED
    ,vb.MODIFICATION_DATE AS BIRTH_MOD
    ,ph_mob.OBJECTS_TYPE
    ,ph_mob.PHONE AS PHONE_MOB
    ,ph_mob.PHONES_AKT
    ,ph_mob.PHONES_CREATED
    ,ph_mob.MODIFICATION_DATE AS PHONES_MOD
    /*,DENSE_RANK() OVER(PARTITION BY ph_mob.OBJECTS_ID   --по каждому адресу последний телефон
                ORDER BY ph_mob.PHONES_AKT DESC, COALESCE(ph_mob.MODIFICATION_DATE, SYSDATE) DESC, ph_mob.PHONES_CREATED DESC) -1
      as RANK*/
  FROM CPD.PHONES ph_mob, KREDIT.C_REQUEST cr, AFS.VIEW_FIO_HISTORY vfio, AFS.VIEW_BIRTH vb
  WHERE vfio.FIO_OBJECTS_ID = cr.OBJECTS_ID AND vfio.FIO_CREATED < cr.CREATED_DATE+10
      AND vb.OBJECTS_ID = cr.OBJECTS_ID AND cr.CREATED_DATE+10>vb.BIRTH_CREATED
      AND ph_mob.OBJECTS_ID=cr.OBJECTS_ID AND ph_mob.OBJECTS_TYPE=2 AND cr.CREATED_DATE+10>ph_mob.phones_created
  --AND cr.OBJECTS_ID=22477470;
--------------------------------------------------------
--  DDL for View VR_PMG
--------------------------------------------------------

  CREATE OR REPLACE FORCE VIEW "AFS"."VR_PMG" ("REQUEST_ID", "OBJECTS_ID", "CREATED_DATE", "MODIFICATION_DATE", "CREATED_GROUP_ID", "STATUS_ID", "SCORE_TREE_ROUTE_ID", "TYPE_REQUEST_ID", "DOCUMENTS_SERIAL", "DOCUMENTS_NUMBER", "DOCUMENTS_AKT", "DOCUMENTS_TYPE", "DOCUMENTS_DATE", "ADDRESS_HISTORY_ID", "OBJECTS_TYPE", "ADDRESS_AKT", "ADDRESS_CREATED", "ADDRESS_MOD_PMG", "ADDRESS_STR", "REGIONS_NAMES", "AREAS_NAMES", "CITIES_NAMES", "STREETS_NAMES", "HOUSE", "BUILD", "FLAT") AS 
  SELECT cr.REQUEST_ID
    ,cr.OBJECTS_ID
    ,cr.CREATED_DATE
    ,cr.MODIFICATION_DATE
    ,cr.CREATED_GROUP_ID
,cr.STATUS_ID
,cr.SCORE_TREE_ROUTE_ID
,cr.TYPE_REQUEST_ID
    ,vdh.DOCUMENTS_SERIAL
    ,vdh.DOCUMENTS_NUMBER
    ,vdh.DOCUMENTS_AKT
    ,vdh.DOCUMENTS_TYPE
    ,vdh.DOCUMENTS_DATE
    /*,vdh.DOCUMENTS_TYPE
    ,vdh.DOCUMENTS_CREATED
    ,vdh.MODIFICATION_DATE AS DOCUMENTS_MOD*/
    ,vah_PMG.ADDRESS_HISTORY_ID
    ,vah_PMG.OBJECTS_TYPE
    ,vah_PMG.ADDRESS_AKT
    ,vah_PMG.ADDRESS_CREATED
    ,vah_PMG.MODIFICATION_DATE AS ADDRESS_MOD_PMG
    ,vah_PMG.ADDRESS_STR
    ,vah_PMG.regions_names 
    ,vah_PMG.areas_names
    ,vah_PMG.cities_names 
    ,vah_PMG.streets_names
    ,vah_PMG.house
    ,vah_PMG.build 
    ,vah_PMG.flat
    /*,DENSE_RANK() OVER (PARTITION BY cr.REQUEST_ID, vfio.FIO_OBJECTS_ID 
                ORDER BY vfio.fio_akt DESC, vfio.FIO_CREATED DESC, vfio.MODIFICATION_DATE DESC) -1 
      +DENSE_RANK() OVER (PARTITION BY cr.REQUEST_ID, vb.OBJECTS_ID 
                    ORDER BY vb.BIRTH_AKT DESC, NVL(vb.MODIFICATION_DATE, SYSDATE) DESC
                      , vb.BIRTH_CREATED DESC ) -1
      +DENSE_RANK() OVER (PARTITION BY cr.REQUEST_ID, vdh.OBJECTS_ID 
                ORDER BY vdh.DOCUMENTS_AKT DESC, NVL(vdh.MODIFICATION_DATE, SYSDATE) DESC
                  , vdh.DOCUMENTS_CREATED DESC, vdh.DOCUMENTS_HISTORY_ID DESC ) -1
      +DENSE_RANK() OVER (PARTITION BY cr.REQUEST_ID, vah_PMG.objects_id, vah_PMG.OBJECTS_TYPE
                ORDER BY vah_PMG.address_akt DESC, vah_PMG.address_created DESC
                  , NVL(vah_PMG.MODIFICATION_DATE, SYSDATE) DESC, vah_PMG.ADDRESS_HISTORY_ID DESC) -1  
      as RANK*/
  FROM KREDIT.C_REQUEST cr
  INNER JOIN AFS.VIEW_DOCUMENTS_HISTORY vdh
    ON cr.OBJECTS_ID=vdh.OBJECTS_ID AND vdh.DOCUMENTS_TYPE=21 AND cr.CREATED_DATE+10>vdh.DOCUMENTS_CREATED
       AND cr.CREATED_DATE+10>vdh.DOCUMENTS_DATE
  INNER JOIN AFS.VIEW_ADDRESS_HISTORY vah_PMG --присоединяем адрес прописки
    ON vah_PMG.OBJECTS_ID=vdh.DOC_OBJECTS_ID AND vah_PMG.OBJECTS_TYPE=5 AND cr.CREATED_DATE+10>vah_PMG.address_created
  --WHERE cr.OBJECTS_ID=22477470;
--------------------------------------------------------
--  DDL for View VR_PMG_FB
--------------------------------------------------------

  CREATE OR REPLACE FORCE VIEW "AFS"."VR_PMG_FB" ("REQUEST_ID", "OBJECTS_ID", "CREATED_DATE", "MODIFICATION_DATE", "CREATED_GROUP_ID", "STATUS_ID", "SCORE_TREE_ROUTE_ID", "TYPE_REQUEST_ID", "FIO", "FIO_AKT", "FIO_CREATED", "FIO_MOD", "BIRTH", "BIRTH_AKT", "BIRTH_CREATED", "BIRTH_MOD", "DOCUMENTS_SERIAL", "DOCUMENTS_NUMBER", "DOCUMENTS_AKT", "DOCUMENTS_TYPE", "DOCUMENTS_DATE", "ADDRESS_HISTORY_ID", "OBJECTS_TYPE", "ADDRESS_AKT", "ADDRESS_CREATED", "ADDRESS_MOD_PMG", "ADDRESS_STR", "REGIONS_NAMES", "AREAS_NAMES", "CITIES_NAMES", "STREETS_NAMES", "HOUSE", "BUILD", "FLAT") AS 
  SELECT cr.REQUEST_ID
    ,cr.OBJECTS_ID
    ,cr.CREATED_DATE
    ,cr.MODIFICATION_DATE
    ,cr.CREATED_GROUP_ID
,cr.STATUS_ID
,cr.SCORE_TREE_ROUTE_ID
,cr.TYPE_REQUEST_ID
    ,vfio.FIO4SEARCH AS FIO
    ,vfio.FIO_AKT
    ,vfio.FIO_CREATED
    ,vfio.MODIFICATION_DATE AS FIO_MOD
    ,vb.BIRTH
    ,vb.BIRTH_AKT
    ,vb.BIRTH_CREATED
    ,vb.MODIFICATION_DATE AS BIRTH_MOD
    ,vdh.DOCUMENTS_SERIAL
    ,vdh.DOCUMENTS_NUMBER
    ,vdh.DOCUMENTS_AKT
    ,vdh.DOCUMENTS_TYPE
    ,vdh.DOCUMENTS_DATE
    /*,vdh.DOCUMENTS_TYPE
    ,vdh.DOCUMENTS_CREATED
    ,vdh.MODIFICATION_DATE AS DOCUMENTS_MOD*/
    ,vah_PMG.ADDRESS_HISTORY_ID
    ,vah_PMG.OBJECTS_TYPE
    ,vah_PMG.ADDRESS_AKT
    ,vah_PMG.ADDRESS_CREATED
    ,vah_PMG.MODIFICATION_DATE AS ADDRESS_MOD_PMG
    ,vah_PMG.ADDRESS_STR
    ,vah_PMG.regions_names 
    ,vah_PMG.areas_names
    ,vah_PMG.cities_names 
    ,vah_PMG.streets_names
    ,vah_PMG.house
    ,vah_PMG.build 
    ,vah_PMG.flat
    /*,DENSE_RANK() OVER (PARTITION BY cr.REQUEST_ID, vfio.FIO_OBJECTS_ID 
                ORDER BY vfio.fio_akt DESC, vfio.FIO_CREATED DESC, vfio.MODIFICATION_DATE DESC) -1 
      +DENSE_RANK() OVER (PARTITION BY cr.REQUEST_ID, vb.OBJECTS_ID 
                    ORDER BY vb.BIRTH_AKT DESC, NVL(vb.MODIFICATION_DATE, SYSDATE) DESC
                      , vb.BIRTH_CREATED DESC ) -1
      +DENSE_RANK() OVER (PARTITION BY cr.REQUEST_ID, vdh.OBJECTS_ID 
                ORDER BY vdh.DOCUMENTS_AKT DESC, NVL(vdh.MODIFICATION_DATE, SYSDATE) DESC
                  , vdh.DOCUMENTS_CREATED DESC, vdh.DOCUMENTS_HISTORY_ID DESC ) -1
      +DENSE_RANK() OVER (PARTITION BY cr.REQUEST_ID, vah_PMG.objects_id, vah_PMG.OBJECTS_TYPE
                ORDER BY vah_PMG.address_akt DESC, vah_PMG.address_created DESC
                  , NVL(vah_PMG.MODIFICATION_DATE, SYSDATE) DESC, vah_PMG.ADDRESS_HISTORY_ID DESC) -1  
      as RANK*/
  FROM KREDIT.C_REQUEST cr
  INNER JOIN AFS.VIEW_FIO_HISTORY vfio
    ON vfio.FIO_OBJECTS_ID = cr.OBJECTS_ID AND vfio.FIO_CREATED < cr.CREATED_DATE+10
  INNER JOIN AFS.VIEW_BIRTH vb
    ON vb.OBJECTS_ID = cr.OBJECTS_ID AND cr.CREATED_DATE+10>vb.BIRTH_CREATED
  INNER JOIN AFS.VIEW_DOCUMENTS_HISTORY vdh
    ON cr.OBJECTS_ID=vdh.OBJECTS_ID AND vdh.DOCUMENTS_TYPE=21 AND cr.CREATED_DATE+10>vdh.DOCUMENTS_CREATED
       AND cr.CREATED_DATE+1>vdh.DOCUMENTS_DATE
  INNER JOIN AFS.VIEW_ADDRESS_HISTORY vah_PMG --присоединяем адрес прописки
    ON vah_PMG.OBJECTS_ID=vdh.DOC_OBJECTS_ID AND vah_PMG.OBJECTS_TYPE=5 AND cr.CREATED_DATE+10>vah_PMG.address_created
  --WHERE cr.OBJECTS_ID=22477470;
--------------------------------------------------------
--  DDL for View VR_PMG_FB_PH
--------------------------------------------------------

  CREATE OR REPLACE FORCE VIEW "AFS"."VR_PMG_FB_PH" ("REQUEST_ID", "OBJECTS_ID", "CREATED_DATE", "MODIFICATION_DATE", "CREATED_GROUP_ID", "STATUS_ID", "SCORE_TREE_ROUTE_ID", "TYPE_REQUEST_ID", "FIO", "FIO_AKT", "FIO_CREATED", "FIO_MOD", "BIRTH", "BIRTH_AKT", "BIRTH_CREATED", "BIRTH_MOD", "DOCUMENTS_SERIAL", "DOCUMENTS_NUMBER", "DOCUMENTS_AKT", "DOCUMENTS_TYPE", "DOCUMENTS_DATE", "ADDRESS_HISTORY_ID", "OBJECTS_TYPE", "ADDRESS_AKT", "ADDRESS_CREATED", "ADDRESS_MOD_PMG", "ADDRESS_STR", "REGIONS_NAMES", "AREAS_NAMES", "CITIES_NAMES", "STREETS_NAMES", "HOUSE", "BUILD", "FLAT", "PHONE_PMG") AS 
  SELECT cr.REQUEST_ID
    ,cr.OBJECTS_ID
    ,cr.CREATED_DATE
    ,cr.MODIFICATION_DATE
    ,cr.CREATED_GROUP_ID
,cr.STATUS_ID
,cr.SCORE_TREE_ROUTE_ID
,cr.TYPE_REQUEST_ID
    ,vfio.FIO4SEARCH AS FIO
    ,vfio.FIO_AKT
    ,vfio.FIO_CREATED
    ,vfio.MODIFICATION_DATE AS FIO_MOD
    ,vb.BIRTH
    ,vb.BIRTH_AKT
    ,vb.BIRTH_CREATED
    ,vb.MODIFICATION_DATE AS BIRTH_MOD
    ,vdh.DOCUMENTS_SERIAL
    ,vdh.DOCUMENTS_NUMBER
    ,vdh.DOCUMENTS_AKT
    ,vdh.DOCUMENTS_TYPE
    ,vdh.DOCUMENTS_DATE
    /*,vdh.DOCUMENTS_TYPE
    ,vdh.DOCUMENTS_CREATED
    ,vdh.MODIFICATION_DATE AS DOCUMENTS_MOD*/
    ,vah_PMG.ADDRESS_HISTORY_ID
    ,vah_PMG.OBJECTS_TYPE
    ,vah_PMG.ADDRESS_AKT
    ,vah_PMG.ADDRESS_CREATED
    ,vah_PMG.MODIFICATION_DATE AS ADDRESS_MOD_PMG
    ,vah_PMG.ADDRESS_STR
    ,vah_PMG.regions_names 
    ,vah_PMG.areas_names
    ,vah_PMG.cities_names 
    ,vah_PMG.streets_names
    ,vah_PMG.house
    ,vah_PMG.build 
    ,vah_PMG.flat
    ,COALESCE(TO_CHAR((SELECT LAST_VALUE(PHONE) OVER(PARTITION BY ph_pmg.OBJECTS_ID   --по каждому адресу последний телефон
                ORDER BY ph_pmg.PHONES_AKT DESC, COALESCE(ph_pmg.MODIFICATION_DATE, SYSDATE) DESC, ph_pmg.PHONES_CREATED DESC) -1 RANK
            FROM CPD.PHONES ph_pmg 
            WHERE OBJECTS_ID=vah_PMG.ADDRESS_ID 
              AND ph_pmg.OBJECTS_TYPE=8 AND cr.CREATED_DATE+1>ph_pmg.phones_created AND ROWNUM=1))
          ,'-') AS PHONE_PMG
    /*,DENSE_RANK() OVER (PARTITION BY cr.REQUEST_ID, vfio.FIO_OBJECTS_ID 
                ORDER BY vfio.fio_akt DESC, vfio.FIO_CREATED DESC, vfio.MODIFICATION_DATE DESC) -1 
      +DENSE_RANK() OVER (PARTITION BY cr.REQUEST_ID, vb.OBJECTS_ID 
                    ORDER BY vb.BIRTH_AKT DESC, NVL(vb.MODIFICATION_DATE, SYSDATE) DESC
                      , vb.BIRTH_CREATED DESC ) -1
      +DENSE_RANK() OVER (PARTITION BY cr.REQUEST_ID, vdh.OBJECTS_ID 
                ORDER BY vdh.DOCUMENTS_AKT DESC, NVL(vdh.MODIFICATION_DATE, SYSDATE) DESC
                  , vdh.DOCUMENTS_CREATED DESC, vdh.DOCUMENTS_HISTORY_ID DESC ) -1
      +DENSE_RANK() OVER (PARTITION BY cr.REQUEST_ID, vah_PMG.objects_id, vah_PMG.OBJECTS_TYPE
                ORDER BY vah_PMG.address_akt DESC, vah_PMG.address_created DESC
                  , NVL(vah_PMG.MODIFICATION_DATE, SYSDATE) DESC, vah_PMG.ADDRESS_HISTORY_ID DESC) -1  
      as RANK*/
  FROM KREDIT.C_REQUEST cr
  INNER JOIN AFS.VIEW_FIO_HISTORY vfio
    ON vfio.FIO_OBJECTS_ID = cr.OBJECTS_ID AND vfio.FIO_CREATED < cr.CREATED_DATE+10
  INNER JOIN AFS.VIEW_BIRTH vb
    ON vb.OBJECTS_ID = cr.OBJECTS_ID AND cr.CREATED_DATE+10>vb.BIRTH_CREATED
  INNER JOIN AFS.VIEW_DOCUMENTS_HISTORY vdh
    ON cr.OBJECTS_ID=vdh.OBJECTS_ID AND vdh.DOCUMENTS_TYPE=21 AND cr.CREATED_DATE+10>vdh.DOCUMENTS_CREATED
       AND cr.CREATED_DATE+1>vdh.DOCUMENTS_DATE
  INNER JOIN AFS.VIEW_ADDRESS_HISTORY vah_PMG --присоединяем адрес прописки
    ON vah_PMG.OBJECTS_ID=vdh.DOC_OBJECTS_ID AND vah_PMG.OBJECTS_TYPE=5 AND cr.CREATED_DATE+10>vah_PMG.address_created
  --WHERE cr.OBJECTS_ID=22477470;
--------------------------------------------------------
--  DDL for View VR_PMG_PH
--------------------------------------------------------

  CREATE OR REPLACE FORCE VIEW "AFS"."VR_PMG_PH" ("REQUEST_ID", "OBJECTS_ID", "CREATED_DATE", "MODIFICATION_DATE", "CREATED_GROUP_ID", "STATUS_ID", "SCORE_TREE_ROUTE_ID", "TYPE_REQUEST_ID", "DOCUMENTS_SERIAL", "DOCUMENTS_NUMBER", "DOCUMENTS_AKT", "DOCUMENTS_TYPE", "DOCUMENTS_DATE", "ADDRESS_HISTORY_ID", "OBJECTS_TYPE", "ADDRESS_AKT", "ADDRESS_CREATED", "ADDRESS_MOD_PMG", "ADDRESS_STR", "REGIONS_NAMES", "AREAS_NAMES", "CITIES_NAMES", "STREETS_NAMES", "HOUSE", "BUILD", "FLAT", "PHONE_PMG") AS 
  SELECT cr.REQUEST_ID
    ,cr.OBJECTS_ID
    ,cr.CREATED_DATE
    ,cr.MODIFICATION_DATE
    ,cr.CREATED_GROUP_ID
,cr.STATUS_ID
,cr.SCORE_TREE_ROUTE_ID
,cr.TYPE_REQUEST_ID
    ,vdh.DOCUMENTS_SERIAL
    ,vdh.DOCUMENTS_NUMBER
    ,vdh.DOCUMENTS_AKT
    ,vdh.DOCUMENTS_TYPE
    ,vdh.DOCUMENTS_DATE
    /*,vdh.DOCUMENTS_TYPE
    ,vdh.DOCUMENTS_CREATED
    ,vdh.MODIFICATION_DATE AS DOCUMENTS_MOD*/
    ,vah_PMG.ADDRESS_HISTORY_ID
    ,vah_PMG.OBJECTS_TYPE
    ,vah_PMG.ADDRESS_AKT
    ,vah_PMG.ADDRESS_CREATED
    ,vah_PMG.MODIFICATION_DATE AS ADDRESS_MOD_PMG
    ,vah_PMG.ADDRESS_STR
    ,vah_PMG.regions_names 
    ,vah_PMG.areas_names
    ,vah_PMG.cities_names 
    ,vah_PMG.streets_names
    ,vah_PMG.house
    ,vah_PMG.build 
    ,vah_PMG.flat
    ,COALESCE(TO_CHAR((SELECT LAST_VALUE(PHONE) OVER(PARTITION BY ph_pmg.OBJECTS_ID   --по каждому адресу последний телефон
                ORDER BY ph_pmg.PHONES_AKT DESC, COALESCE(ph_pmg.MODIFICATION_DATE, SYSDATE) DESC, ph_pmg.PHONES_CREATED DESC) -1 RANK
            FROM CPD.PHONES ph_pmg 
            WHERE OBJECTS_ID=vah_PMG.ADDRESS_ID 
              AND ph_pmg.OBJECTS_TYPE=8 AND cr.CREATED_DATE+1>ph_pmg.phones_created AND ROWNUM=1))
          ,'-') AS PHONE_PMG
    /*,DENSE_RANK() OVER (PARTITION BY cr.REQUEST_ID, vfio.FIO_OBJECTS_ID 
                ORDER BY vfio.fio_akt DESC, vfio.FIO_CREATED DESC, vfio.MODIFICATION_DATE DESC) -1 
      +DENSE_RANK() OVER (PARTITION BY cr.REQUEST_ID, vb.OBJECTS_ID 
                    ORDER BY vb.BIRTH_AKT DESC, NVL(vb.MODIFICATION_DATE, SYSDATE) DESC
                      , vb.BIRTH_CREATED DESC ) -1
      +DENSE_RANK() OVER (PARTITION BY cr.REQUEST_ID, vdh.OBJECTS_ID 
                ORDER BY vdh.DOCUMENTS_AKT DESC, NVL(vdh.MODIFICATION_DATE, SYSDATE) DESC
                  , vdh.DOCUMENTS_CREATED DESC, vdh.DOCUMENTS_HISTORY_ID DESC ) -1
      +DENSE_RANK() OVER (PARTITION BY cr.REQUEST_ID, vah_PMG.objects_id, vah_PMG.OBJECTS_TYPE
                ORDER BY vah_PMG.address_akt DESC, vah_PMG.address_created DESC
                  , NVL(vah_PMG.MODIFICATION_DATE, SYSDATE) DESC, vah_PMG.ADDRESS_HISTORY_ID DESC) -1  
      as RANK*/
  FROM KREDIT.C_REQUEST cr
  INNER JOIN AFS.VIEW_DOCUMENTS_HISTORY vdh
    ON cr.OBJECTS_ID=vdh.OBJECTS_ID AND vdh.DOCUMENTS_TYPE=21 AND cr.CREATED_DATE+10>vdh.DOCUMENTS_CREATED
       AND cr.CREATED_DATE+10>vdh.DOCUMENTS_DATE
  INNER JOIN AFS.VIEW_ADDRESS_HISTORY vah_PMG --присоединяем адрес прописки
    ON vah_PMG.OBJECTS_ID=vdh.DOC_OBJECTS_ID AND vah_PMG.OBJECTS_TYPE=5 AND cr.CREATED_DATE+10>vah_PMG.address_created
  --WHERE cr.OBJECTS_ID=22477470;
--------------------------------------------------------
--  DDL for Index I_AFS_L_HIST_REQUEST_ID_REL
--------------------------------------------------------

  CREATE INDEX "AFS"."I_AFS_L_HIST_REQUEST_ID_REL" ON "AFS"."AFS_HISTORY" ("REQUEST_ID_REL") 
  PCTFREE 10 INITRANS 2 MAXTRANS 255 COMPUTE STATISTICS 
  STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "AFS" ;
--------------------------------------------------------
--  DDL for Index I_AFS_L_HIST_REQUEST_ID
--------------------------------------------------------

  CREATE INDEX "AFS"."I_AFS_L_HIST_REQUEST_ID" ON "AFS"."AFS_HISTORY" ("REQUEST_ID") 
  PCTFREE 10 INITRANS 2 MAXTRANS 255 COMPUTE STATISTICS 
  STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "AFS" ;
--------------------------------------------------------
--  DDL for Index I_AFS_L_CALL_REQUEST_ID
--------------------------------------------------------

  CREATE INDEX "AFS"."I_AFS_L_CALL_REQUEST_ID" ON "AFS"."AFS_LOG_CALL" ("REQUEST_ID") 
  PCTFREE 10 INITRANS 2 MAXTRANS 255 COMPUTE STATISTICS 
  STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "AFS" ;
--------------------------------------------------------
--  DDL for Index I_AFS_LOG_CALL_ERRORCODE
--------------------------------------------------------

  CREATE INDEX "AFS"."I_AFS_LOG_CALL_ERRORCODE" ON "AFS"."AFS_LOG_CALL" ("ERRORCODE") 
  PCTFREE 10 INITRANS 2 MAXTRANS 255 COMPUTE STATISTICS 
  STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "AFS" ;
--------------------------------------------------------
--  DDL for Index I_AFS_L_ERR_PROC_ID
--------------------------------------------------------

  CREATE INDEX "AFS"."I_AFS_L_ERR_PROC_ID" ON "AFS"."AFS_LOG_ERROR" ("PROC_ID") 
  PCTFREE 10 INITRANS 2 MAXTRANS 255 COMPUTE STATISTICS 
  STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "AFS" ;
--------------------------------------------------------
--  DDL for Index I_AFS_L_HIST_PROC_ID
--------------------------------------------------------

  CREATE INDEX "AFS"."I_AFS_L_HIST_PROC_ID" ON "AFS"."AFS_HISTORY" ("PROC_ID") 
  PCTFREE 10 INITRANS 2 MAXTRANS 255 COMPUTE STATISTICS 
  STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "AFS" ;
--------------------------------------------------------
--  DDL for Index I_AFS_L_HIST_PERSON_ID_REL
--------------------------------------------------------

  CREATE INDEX "AFS"."I_AFS_L_HIST_PERSON_ID_REL" ON "AFS"."AFS_HISTORY" ("PERSON_ID_REL") 
  PCTFREE 10 INITRANS 2 MAXTRANS 255 COMPUTE STATISTICS 
  STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "AFS" ;
--------------------------------------------------------
--  DDL for Index I_AFS_L_HIST_PERSON_ID
--------------------------------------------------------

  CREATE INDEX "AFS"."I_AFS_L_HIST_PERSON_ID" ON "AFS"."AFS_HISTORY" ("PERSON_ID") 
  PCTFREE 10 INITRANS 2 MAXTRANS 255 COMPUTE STATISTICS 
  STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "AFS" ;
--------------------------------------------------------
--  DDL for Index I_AFS_L_CALL_PERSON_ID
--------------------------------------------------------

  CREATE INDEX "AFS"."I_AFS_L_CALL_PERSON_ID" ON "AFS"."AFS_LOG_CALL" ("PERSON_ID") 
  PCTFREE 10 INITRANS 2 MAXTRANS 255 COMPUTE STATISTICS 
  STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "AFS" ;
--------------------------------------------------------
--  DDL for Index AFS_LOG_CALL_PK
--------------------------------------------------------

  CREATE UNIQUE INDEX "AFS"."AFS_LOG_CALL_PK" ON "AFS"."AFS_LOG_CALL" ("PROC_ID") 
  PCTFREE 10 INITRANS 2 MAXTRANS 255 COMPUTE STATISTICS 
  STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "AFS" ;
--------------------------------------------------------
--  Constraints for Table AFS_HISTORY
--------------------------------------------------------

  ALTER TABLE "AFS"."AFS_HISTORY" MODIFY ("PROC_ID" NOT NULL ENABLE);
  GRANT SELECT ON "AFS"."AFS_HISTORY" TO "MVKARELIN";
GRANT SELECT ON "AFS"."D_FRAUD_RULE" TO "MVKARELIN";
--------------------------------------------------------
--  Constraints for Table AFS_LOG_ERROR
--------------------------------------------------------

  ALTER TABLE "AFS"."AFS_LOG_ERROR" MODIFY ("PROC_ID" NOT NULL ENABLE);
--------------------------------------------------------
--  Constraints for Table AFS_LOG_CALL
--------------------------------------------------------

  ALTER TABLE "AFS"."AFS_LOG_CALL" ADD CONSTRAINT "AFS_LOG_CALL_PK" PRIMARY KEY ("PROC_ID")
  USING INDEX PCTFREE 10 INITRANS 2 MAXTRANS 255 COMPUTE STATISTICS 
  STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "AFS"  ENABLE;
  ALTER TABLE "AFS"."AFS_LOG_CALL" MODIFY ("PROC_ID" NOT NULL ENABLE);
GRANT SELECT ON "AFS"."AFS_HISTORY" TO "MVKARELIN";
GRANT SELECT ON "AFS"."D_FRAUD_RULE" TO "MVKARELIN";
--------------------------------------------------------
--  DDL for Trigger TRG_AFS_LOG_CALL_INS
--------------------------------------------------------

  CREATE OR REPLACE TRIGGER "AFS"."TRG_AFS_LOG_CALL_INS" BEFORE INSERT ON AFS_LOG_CALL
FOR EACH ROW 
BEGIN
  <<COLUMN_SEQUENCES>>
  BEGIN
    IF :NEW.PROC_ID IS NULL THEN
      SELECT AFS.AFS_PROC_SEQ.NEXTVAL INTO :NEW.PROC_ID FROM DUAL;
    END IF;
  END COLUMN_SEQUENCES;
END;
/
ALTER TRIGGER "AFS"."TRG_AFS_LOG_CALL_INS" ENABLE;
--------------------------------------------------------
--  DDL for Trigger TRG_AFS_LOG_CALL_UPD
--------------------------------------------------------

  CREATE OR REPLACE TRIGGER "AFS"."TRG_AFS_LOG_CALL_UPD" BEFORE UPDATE ON AFS_LOG_CALL
FOR EACH ROW 
BEGIN
  :NEW.DATE_CALL_END := SYSTIMESTAMP;
  --EXCEPTION WHEN OTHERS THEN NULL;
END;
/
ALTER TRIGGER "AFS"."TRG_AFS_LOG_CALL_UPD" ENABLE;
--------------------------------------------------------
--  DDL for Function FN_DIST_LEV
--------------------------------------------------------

  CREATE OR REPLACE FUNCTION "AFS"."FN_DIST_LEV" (
	as_src_i		IN VARCHAR2
	, as_trg_i		IN VARCHAR2
	, porog_ln IN NUMBER DEFAULT 1000
)
RETURN NUMBER
DETERMINISTIC
AS
/* Самописное расстояние Левенштейна (кол-во унарных операций вставки/удаления/замены символа) для нечеткого сравнения */
	ln_src_len				PLS_INTEGER := NVL(LENGTH(as_src_i), 0);
	ln_trg_len				PLS_INTEGER := NVL(LENGTH(as_trg_i), 0);
	ln_hlen					PLS_INTEGER;
	ln_cost					PLS_INTEGER;
	TYPE t_numtbl IS TABLE OF PLS_INTEGER INDEX BY BINARY_INTEGER;
	la_ldmatrix				t_numtbl;
BEGIN
	IF (ln_src_len = 0)
	THEN
		RETURN ln_trg_len;
	ELSIF (ln_trg_len = 0)
	THEN
		RETURN ln_src_len;
	END IF;
	IF ABS(ln_src_len-ln_trg_len)>porog_ln THEN --если разница длин больше установленной то прерывать
    RETURN NULL;
  END IF;

	ln_hlen := ln_src_len + 1;
	FOR h IN 0 .. ln_src_len
	LOOP
		la_ldmatrix(h) := h;
	END LOOP;

	FOR v IN 0 .. ln_trg_len
	LOOP
		la_ldmatrix(v * ln_hlen) := v;
	END LOOP;

	FOR h IN 1 .. ln_src_len
	LOOP
		FOR v IN 1 .. ln_trg_len
		LOOP
			IF (SUBSTR(as_src_i, h, 1) = SUBSTR(as_trg_i, v, 1))
		THEN
			ln_cost := 0;
		ELSE
			ln_cost := 1;
		END IF;
			la_ldmatrix(v * ln_hlen + h) :=
				LEAST(
					la_ldmatrix((v - 1) * ln_hlen + h    ) + 1
					, la_ldmatrix( v      * ln_hlen + h - 1) + 1
					, la_ldmatrix((v - 1) * ln_hlen + h - 1) + ln_cost
				);
		END LOOP;
	END LOOP;
	RETURN la_ldmatrix(ln_trg_len * ln_hlen + ln_src_len);
END FN_DIST_LEV ;

/

  GRANT EXECUTE ON "AFS"."FN_DIST_LEV" TO "MVKARELIN";
--------------------------------------------------------
--  DDL for Function FN_IS_FAMILY_CONT
--------------------------------------------------------

  CREATE OR REPLACE FUNCTION "AFS"."FN_IS_FAMILY_CONT" (inPID IN NUMBER, inPID_REL IN NUMBER) RETURN NUMBER
-- ========================================================================
-- ==	ФУНКЦИЯ     Проверка состоят ли PERSON_ID и PERSON_ID_REL в семейных отношениях.
-- ==	ОПИСАНИЕ:   Проверка явных (уровень 1) и неявных (уровень 2) семейных связей.
-- ==             В общем понимаю что не оптимально, но нагрузка планируется не очень большая
-- ==               , а правильные оптимизации будут очень затратны... Мб когда нибудь кто-нибудь доведет.
-- ========================================================================
-- ==	СОЗДАНИЕ:		  23.11.2015 (ТРАХАЧЕВ В.В.)
-- ==	МОДИФИКАЦИЯ:	23.11.2015 (ТРАХАЧЕВ В.В.)
-- ========================================================================
IS
 FLAG_FAMILY_CONT NUMBER;
BEGIN
  DBMS_OUTPUT.ENABLE;
           
  SELECT count(*) INTO FLAG_FAMILY_CONT FROM dual
      WHERE EXISTS(SELECT ph.*
                  /*,UPPER(TRIM(ph.PHONES_COMM)) as fio_200
                  ,f.FIO4SEARCH as FIO_2
                  ,UTL_MATCH.EDIT_DISTANCE(UPPER(TRIM(ph.PHONES_COMM)), f.FIO4SEARCH) as lev_Fio
                  ,UTL_MATCH.EDIT_DISTANCE(SUBSTR(UPPER(TRIM(ph.PHONES_COMM)), 1, INSTR(UPPER(TRIM(ph.PHONES_COMM)),' ')-1 )
                                          , SUBSTR(f.FIO4SEARCH, 1, INSTR(f.FIO4SEARCH,' ')-1 )) as lev_Fam*/
                  /*,fh.FIO_AKT 
                  ,ph.FAMILY_REL*/
  FROM CPD.PHONES ph 
  LEFT JOIN CPD.FIO_HISTORY fh 
    ON fh.OBJECTS_ID = inPID_REL AND NOT fh.OBJECTS_ID IS NULL
  LEFT JOIN CPD.FIO f
    ON f.FIO_ID=fh.FIO_ID AND NOT f.FIO_ID IS NULL
          
  WHERE  ph.OBJECTS_ID = inPID AND ph.OBJECTS_TYPE=200 
    AND ph.FAMILY_REL>0 AND NOT ph.FAMILY_REL IN(35, 44, 106) AND inPID ^= inPID_REL  
    -- 1 уровень - проверка на родственные связи в пределах допустимой погрешности
    AND ( (UTL_MATCH.EDIT_DISTANCE(UPPER(TRIM(ph.PHONES_COMM)), f.FIO4SEARCH) BETWEEN 0 AND 5)
    -- 2 уровень - проверка на одинаковость фамилии в пределах допустимой погрешности
        OR  UTL_MATCH.EDIT_DISTANCE(SUBSTR(UPPER(TRIM(ph.PHONES_COMM)), 1, INSTR(UPPER(TRIM(ph.PHONES_COMM)),' ')-1 )
                                  , SUBSTR(f.FIO4SEARCH, 1, INSTR(f.FIO4SEARCH,' ')-1 ))
              /LENGTH(SUBSTR(UPPER(TRIM(ph.PHONES_COMM)), 1, INSTR(UPPER(TRIM(ph.PHONES_COMM)),' ')-1 )) BETWEEN 0 AND 0.35 )
  );
      
  RETURN FLAG_FAMILY_CONT;

EXCEPTION
    WHEN OTHERS
    THEN 
        RETURN -1;

END;

/

  GRANT EXECUTE ON "AFS"."FN_IS_FAMILY_CONT" TO "MVKARELIN";
--------------------------------------------------------
--  DDL for Function FN_IS_FAMILY_REL
--------------------------------------------------------

  CREATE OR REPLACE FUNCTION "AFS"."FN_IS_FAMILY_REL" (inPID IN NUMBER, inPID_REL IN NUMBER) RETURN NUMBER
-- ========================================================================
-- ==	ФУНКЦИЯ     Проверка состоят ли PERSON_ID и PERSON_ID_REL в семейных отношениях.
-- ==	ОПИСАНИЕ:   Проверка в прямой таблице семейных связей
-- ========================================================================
-- ==	СОЗДАНИЕ:		  23.11.2015 (ТРАХАЧЕВ В.В.)
-- ==	МОДИФИКАЦИЯ:	23.11.2015 (ТРАХАЧЕВ В.В.)
-- ========================================================================
IS
 FLAG_FAMILY NUMBER;
BEGIN
  DBMS_OUTPUT.ENABLE;
           
  SELECT count(*) INTO FLAG_FAMILY FROM dual
      WHERE EXISTS(SELECT * FROM CPD.FAMILY fam
									WHERE fam.OBJECTS_ID=inPID AND fam.OB_ID=inPID_REL
										/*AND fam.FAMILY_AKT=1*/ /*AND fam.OBJECTS_TYPE=2 AND fam.OB_TYPE=2*/);
      
  RETURN FLAG_FAMILY;

EXCEPTION
    WHEN OTHERS
    THEN 
        RETURN -1;

END;

/

  GRANT EXECUTE ON "AFS"."FN_IS_FAMILY_REL" TO "MVKARELIN";
--------------------------------------------------------
--  DDL for Function FN$IS_REQUEST_FOR_FRAUD
--------------------------------------------------------

  CREATE OR REPLACE FUNCTION "AFS"."FN$IS_REQUEST_FOR_FRAUD" (v_RID IN NUMBER, v_EndRid IN NUMBER DEFAULT 1) RETURN NUMBER 
-- ФУНКЦИЯ для определения, является ли заявка актуальной для учета при анализе фрода
AS
  p_FlagAct NUMBER := 0; --флаг что реквест нужен
BEGIN 
  IF v_EndRid=1 THEN --если нам нужны для учета только завершенные, то учитываем статусы
      SELECT COUNT(*) INTO p_FlagAct FROM dual
		WHERE NOT EXISTS(SELECT REQUEST_ID FROM KREDIT.C_REQUEST_REACT
					WHERE REQUEST_ID = v_RID
            --убираем отказы на прескоринге
						AND request_old_status_id = 200 and request_new_status_id = 7)
			AND NOT EXISTS(SELECT REQUEST_ID FROM KREDIT.C_REQUEST
					WHERE REQUEST_ID = v_RID
            --убираем интернет-заявки и пакетную генерацию
						AND (CREATED_GROUP_ID IN(4862,4863,4865,4864,5736,5899,7674,11455,10165)
							--выбираем незавершенные заявки
							OR NOT (status_id IN(7,8,9,11,14,16,17,21))
							--исключить отказы по прескорингу
							OR (score_tree_route_id=6 and status_id in(7,8))
							--убираем мгновенные продажи
							OR (score_tree_route_id=1)) ) ;
    ELSE 
      SELECT COUNT(*) INTO p_FlagAct FROM dual
        WHERE NOT EXISTS(SELECT REQUEST_ID FROM KREDIT.C_REQUEST_REACT
              WHERE REQUEST_ID = v_RID
                --убираем отказы на прескоринге
                AND request_old_status_id = 200 and request_new_status_id = 7)
          AND NOT EXISTS(SELECT REQUEST_ID FROM KREDIT.C_REQUEST
              WHERE REQUEST_ID = v_RID
                --убираем интернет-заявки и пакетную генерацию
                AND (CREATED_GROUP_ID IN(4862,4863,4865,4864,5736,5899,7674,11455,10165) OR
                  --выбираем незавершенные заявки
                  --NOT (status_id IN(7,8,9,11,14,16,17,21)) OR
                  --исключить отказы по прескорингу
                  (score_tree_route_id=6 and status_id in(7,8)) OR
                  --убираем мгновенные продажи
                  (score_tree_route_id=1)) ) ;
    END IF;
  RETURN p_FlagAct;
  
  EXCEPTION WHEN OTHERS THEN RETURN -1;
END;

/

  GRANT EXECUTE ON "AFS"."FN$IS_REQUEST_FOR_FRAUD" TO "MVKARELIN";
--------------------------------------------------------
--  DDL for Function FN$RID_FILTER_FOR_FRAUD
--------------------------------------------------------

  CREATE OR REPLACE FUNCTION "AFS"."FN$RID_FILTER_FOR_FRAUD" (v_RID IN NUMBER, v_EndRid IN NUMBER DEFAULT 1) RETURN NUMBER 
-- ФУНКЦИЯ для определения, является ли заявка актуальной для учета при анализе фрода
AS
  p_FlagAct NUMBER := 0; --флаг что реквест нужен
BEGIN 
  IF v_EndRid=1 THEN --если нам нужны для учета только завершенные, то учитываем статусы
      SELECT COUNT(*) INTO p_FlagAct FROM dual
		WHERE NOT EXISTS(SELECT REQUEST_ID FROM KREDIT.C_REQUEST_REACT
					WHERE REQUEST_ID = v_RID
            --убираем отказы на прескоринге
						AND request_old_status_id = 200 and request_new_status_id = 7)
			AND NOT EXISTS(SELECT REQUEST_ID FROM KREDIT.C_REQUEST
					WHERE REQUEST_ID = v_RID
            --убираем интернет-заявки и пакетную генерацию
						AND (CREATED_GROUP_ID IN(4862,4863,4865,4864,5736,5899,7674,11455,10165)
							--выбираем незавершенные заявки
							OR NOT (status_id IN(7,8,9,11,14,16,17,21))
							--исключить отказы по пакетным генерациям
							OR (score_tree_route_id=6 and status_id in(7,8))
							--убираем мгновенные продажи
							OR (score_tree_route_id=1)) ) ;
    END IF;
  RETURN p_FlagAct;
  
  EXCEPTION WHEN OTHERS THEN RETURN -1;
END;

/
--------------------------------------------------------
--  DDL for Package AFS_FILTER
--------------------------------------------------------

  CREATE OR REPLACE PACKAGE "AFS"."AFS_FILTER" AS 

  --Функция проверки входящей заявки на соответсвие критерям фильтрации.
  ----Входной параметр v_RID - ID заявки
  FUNCTION CHECK_REQUEST_ID_MAIN (v_RID IN NUMBER) RETURN NUMBER ;
  
  --Функция проверки найденной заявки на соответсвие критерям фильтрации.
  ----Входной параметр v_RID - ID найденной по фроду заявки
  FUNCTION CHECK_REQUEST_ID_REL (v_RID IN NUMBER) RETURN NUMBER ;

END AFS_FILTER;

/
--------------------------------------------------------
--  DDL for Package AFS_RULES
--------------------------------------------------------

  CREATE OR REPLACE PACKAGE "AFS"."AFS_RULES" AS 
-- ========================================================================
-- ==    ПАКЕТ "СО списком фрод правил. Паспорт равен. Разл. ФИО в 1 симв"
-- ==    ОПИСАНИЕ:        Совпадает серия и номер паспорта. ФИО или ДР - отличие в 1 символе 
-- ==                    (SER+NUM = , ФИО+ДР^= в 1 симв.)
-- ========================================================================
-- ==	СОЗДАНИЕ:		  06.2016 (ТРАХАЧЕВ В.В.)
-- ==	МОДИФИКАЦИЯ:	06.2016 (ТРАХАЧЕВ В.В.)
-- ========================================================================
  --p_ErrorCode NUMBER := 0;
  --правило эталон-скелет
  /*Описание параметров для фрод правила.
        v_Request_ID IN NUMBER    - !входной параметр REQUEST_ID заявки из ПКК
        v_Person_ID IN NUMBER     - входной параметр PERSON_ID заявки из ПКК. 
        v_OutCode IN VARCHAR2     - Код внешней системы (для синхронизации между системами)
        v_InArray IN VARCHAR2     - входной параметр для расширения (массив данных)
 
        v_OutNumber OUT NUMBER    - !Выходные данные (результат по фрод-правилу)
                                      0   - фрода нет
                                      1   - фрод есть
                                      -1  - ошибка
                                      9  - нет информации. Т.к. будут использоваться данные ПКК, то считаю код неактуальным
        v_OutChar IN OUT VARCHAR2 - строковый эквивалент возвращаемого значения
        v_OutArray OUT VARCHAR2   - выходной параметр для расширения (массив данных)
        v_ErrorCode OUT NUMBER    - Код ошибки (внутренний код ошибки объекта, если более Нуля то значит возникла проблема при работе)
        v_ErrorText OUT VARCHAR2  - Описание ошибки (детальное описание ошибки)
  */
  PROCEDURE FRAUD_RULE_0000(v_Request_ID IN NUMBER, v_Person_ID IN NUMBER, v_OutCode IN VARCHAR2, v_InArray IN VARCHAR2
                          ,v_OutNumber OUT NUMBER, v_OutChar IN OUT VARCHAR2
                          ,v_OutArray OUT VARCHAR2
                          ,v_ErrorCode OUT NUMBER, v_ErrorText OUT VARCHAR2
                        );


  PROCEDURE FRAUD_RULE_0001(v_Request_ID IN NUMBER, v_Person_ID IN NUMBER, v_OutCode IN VARCHAR2, v_InArray IN VARCHAR2
                          ,v_OutNumber OUT NUMBER, v_OutChar IN OUT VARCHAR2
                          ,v_OutArray OUT VARCHAR2
                          ,v_ErrorCode OUT NUMBER, v_ErrorText OUT VARCHAR2
                        );
                        
  PROCEDURE FRAUD_RULE_0002(v_Request_ID IN NUMBER, v_Person_ID IN NUMBER, v_OutCode IN VARCHAR2, v_InArray IN VARCHAR2
                          ,v_OutNumber OUT NUMBER, v_OutChar IN OUT VARCHAR2
                          ,v_OutArray OUT VARCHAR2
                          ,v_ErrorCode OUT NUMBER, v_ErrorText OUT VARCHAR2
                        );
                        
  PROCEDURE FRAUD_RULE_0003(v_Request_ID IN NUMBER, v_Person_ID IN NUMBER, v_OutCode IN VARCHAR2, v_InArray IN VARCHAR2
                          ,v_OutNumber OUT NUMBER, v_OutChar IN OUT VARCHAR2
                          ,v_OutArray OUT VARCHAR2
                          ,v_ErrorCode OUT NUMBER, v_ErrorText OUT VARCHAR2
                        );
                        
  PROCEDURE FRAUD_RULE_0004(v_Request_ID IN NUMBER, v_Person_ID IN NUMBER, v_OutCode IN VARCHAR2, v_InArray IN VARCHAR2
                          ,v_OutNumber OUT NUMBER, v_OutChar IN OUT VARCHAR2
                          ,v_OutArray OUT VARCHAR2
                          ,v_ErrorCode OUT NUMBER, v_ErrorText OUT VARCHAR2
                        );
                        
  PROCEDURE FRAUD_RULE_0005(v_Request_ID IN NUMBER, v_Person_ID IN NUMBER, v_OutCode IN VARCHAR2, v_InArray IN VARCHAR2
                          ,v_OutNumber OUT NUMBER, v_OutChar IN OUT VARCHAR2
                          ,v_OutArray OUT VARCHAR2
                          ,v_ErrorCode OUT NUMBER, v_ErrorText OUT VARCHAR2
                        );
                             
END AFS_RULES;

/

  GRANT EXECUTE ON "AFS"."AFS_RULES" TO "L_KREDIT_SCORING";
--------------------------------------------------------
--  DDL for Package AFS_RULES_ADR
--------------------------------------------------------

  CREATE OR REPLACE PACKAGE "AFS"."AFS_RULES_ADR" AS 
-- ========================================================================
-- ==    ПАКЕТ со списком фрод правил. Сравнение по адресу"
-- ========================================================================
-- ==	СОЗДАНИЕ:		  06.2016 (ТРАХАЧЕВ В.В.)
-- ==	МОДИФИКАЦИЯ:	06.2016 (ТРАХАЧЕВ В.В.)
-- ========================================================================
  /*Описание параметров для фрод правила.
        v_Request_ID IN NUMBER    - !входной параметр REQUEST_ID заявки из ПКК
        v_Person_ID IN NUMBER     - входной параметр PERSON_ID заявки из ПКК. 
        v_OutCode IN VARCHAR2     - Код внешней системы (для синхронизации между системами)
        v_InArray IN VARCHAR2     - входной параметр для расширения (массив данных)
 
        v_OutNumber OUT NUMBER    - !Выходные данные (результат по фрод-правилу)
                                      0   - фрода нет
                                      1   - фрод есть
                                      -1  - ошибка
                                      9  - нет информации. Т.к. будут использоваться данные ПКК, то считаю код неактуальным
        v_OutChar IN OUT VARCHAR2 - строковый эквивалент возвращаемого значения
        v_OutArray OUT VARCHAR2   - выходной параметр для расширения (массив данных)
        v_ErrorCode OUT NUMBER    - Код ошибки (внутренний код ошибки объекта, если более Нуля то значит возникла проблема при работе)
        v_ErrorText OUT VARCHAR2  - Описание ошибки (детальное описание ошибки)
  */

  PROCEDURE FRAUD_0001(v_PROC_ID IN OUT NUMBER
                          ,v_Request_ID IN NUMBER, v_Person_ID IN NUMBER, v_OutCode IN VARCHAR2, v_InArray IN VARCHAR2
                          ,v_OutNumber OUT NUMBER, v_OutChar OUT VARCHAR2
                          ,v_OutArray OUT VARCHAR2
                          ,v_ErrorCode OUT NUMBER, v_ErrorText OUT VARCHAR2
                        );
                        
  PROCEDURE FRAUD_0002(v_PROC_ID IN OUT NUMBER
                          ,v_Request_ID IN NUMBER, v_Person_ID IN NUMBER, v_OutCode IN VARCHAR2, v_InArray IN VARCHAR2
                          ,v_OutNumber OUT NUMBER, v_OutChar OUT VARCHAR2
                          ,v_OutArray OUT VARCHAR2
                          ,v_ErrorCode OUT NUMBER, v_ErrorText OUT VARCHAR2
                        );
                        
  PROCEDURE FRAUD_0003(v_PROC_ID IN OUT NUMBER
                          ,v_Request_ID IN NUMBER, v_Person_ID IN NUMBER, v_OutCode IN VARCHAR2, v_InArray IN VARCHAR2
                          ,v_OutNumber OUT NUMBER, v_OutChar OUT VARCHAR2
                          ,v_OutArray OUT VARCHAR2
                          ,v_ErrorCode OUT NUMBER, v_ErrorText OUT VARCHAR2
                        );
                        
  PROCEDURE FRAUD_0004(v_PROC_ID IN OUT NUMBER
                          ,v_Request_ID IN NUMBER, v_Person_ID IN NUMBER, v_OutCode IN VARCHAR2, v_InArray IN VARCHAR2
                          ,v_OutNumber OUT NUMBER, v_OutChar OUT VARCHAR2
                          ,v_OutArray OUT VARCHAR2
                          ,v_ErrorCode OUT NUMBER, v_ErrorText OUT VARCHAR2
                        );
                             
END AFS_RULES_ADR;

/

  GRANT EXECUTE ON "AFS"."AFS_RULES_ADR" TO "MVKARELIN";
--------------------------------------------------------
--  DDL for Package AFS_RULES_OBJECTS_ID
--------------------------------------------------------

  CREATE OR REPLACE PACKAGE "AFS"."AFS_RULES_OBJECTS_ID" AS 
-- ========================================================================
-- ==    ПАКЕТ со списком фрод правил. Сравнение по адресу"
-- ========================================================================
-- ==	СОЗДАНИЕ:		  06.2016 (ТРАХАЧЕВ В.В.)
-- ==	МОДИФИКАЦИЯ:	06.2016 (ТРАХАЧЕВ В.В.)
-- ========================================================================
  /*Описание параметров для фрод правила.
        v_Request_ID IN NUMBER    - !входной параметр REQUEST_ID заявки из ПКК
        v_Person_ID IN NUMBER     - входной параметр PERSON_ID заявки из ПКК. 
        v_OutCode IN VARCHAR2     - Код внешней системы (для синхронизации между системами)
        v_InArray IN VARCHAR2     - входной параметр для расширения (массив данных)
 
        v_OutNumber OUT NUMBER    - !Выходные данные (результат по фрод-правилу)
                                      0   - фрода нет
                                      1   - фрод есть
                                      -1  - ошибка
                                      9  - нет информации. Т.к. будут использоваться данные ПКК, то считаю код неактуальным
        v_OutChar IN OUT VARCHAR2 - строковый эквивалент возвращаемого значения
        v_OutArray OUT VARCHAR2   - выходной параметр для расширения (массив данных)
        v_ErrorCode OUT NUMBER    - Код ошибки (внутренний код ошибки объекта, если более Нуля то значит возникла проблема при работе)
        v_ErrorText OUT VARCHAR2  - Описание ошибки (детальное описание ошибки)
  */

  PROCEDURE FRAUD_0001(v_PROC_ID IN OUT NUMBER
                          ,v_Request_ID IN NUMBER, v_Person_ID IN NUMBER, v_OutCode IN VARCHAR2, v_InArray IN VARCHAR2
                          ,v_OutNumber OUT NUMBER, v_OutChar OUT VARCHAR2
                          ,v_OutArray OUT VARCHAR2
                          ,v_ErrorCode OUT NUMBER, v_ErrorText OUT VARCHAR2
                        );
                        
  PROCEDURE FRAUD_0002(v_PROC_ID IN OUT NUMBER
                          ,v_Request_ID IN NUMBER, v_Person_ID IN NUMBER, v_OutCode IN VARCHAR2, v_InArray IN VARCHAR2
                          ,v_OutNumber OUT NUMBER, v_OutChar OUT VARCHAR2
                          ,v_OutArray OUT VARCHAR2
                          ,v_ErrorCode OUT NUMBER, v_ErrorText OUT VARCHAR2
                        );
                        
  PROCEDURE FRAUD_0003(v_PROC_ID IN OUT NUMBER
                          ,v_Request_ID IN NUMBER, v_Person_ID IN NUMBER, v_OutCode IN VARCHAR2, v_InArray IN VARCHAR2
                          ,v_OutNumber OUT NUMBER, v_OutChar OUT VARCHAR2
                          ,v_OutArray OUT VARCHAR2
                          ,v_ErrorCode OUT NUMBER, v_ErrorText OUT VARCHAR2
                        );
                        
  PROCEDURE FRAUD_0004(v_PROC_ID IN OUT NUMBER
                          ,v_Request_ID IN NUMBER, v_Person_ID IN NUMBER, v_OutCode IN VARCHAR2, v_InArray IN VARCHAR2
                          ,v_OutNumber OUT NUMBER, v_OutChar OUT VARCHAR2
                          ,v_OutArray OUT VARCHAR2
                          ,v_ErrorCode OUT NUMBER, v_ErrorText OUT VARCHAR2
                        );
                             
END AFS_RULES_OBJECTS_ID;

/

  GRANT EXECUTE ON "AFS"."AFS_RULES_OBJECTS_ID" TO "MVKARELIN";
--------------------------------------------------------
--  DDL for Package AFS_RULES_PASP
--------------------------------------------------------

  CREATE OR REPLACE PACKAGE "AFS"."AFS_RULES_PASP" AS 
-- ========================================================================
-- ==    ПАКЕТ со списком фрод правил. Базируется на паспортных данных"
-- ========================================================================
-- ==	СОЗДАНИЕ:		  06.2016 (ТРАХАЧЕВ В.В.)
-- ==	МОДИФИКАЦИЯ:	06.2016 (ТРАХАЧЕВ В.В.)
-- ========================================================================
  /*Описание параметров для фрод правила.
        v_Request_ID IN NUMBER    - !входной параметр REQUEST_ID заявки из ПКК
        v_Person_ID IN NUMBER     - входной параметр PERSON_ID заявки из ПКК. 
        v_OutCode IN VARCHAR2     - Код внешней системы (для синхронизации между системами)
        v_InArray IN VARCHAR2     - входной параметр для расширения (массив данных)
 
        v_OutNumber OUT NUMBER    - !Выходные данные (результат по фрод-правилу)
                                      0   - фрода нет
                                      1   - фрод есть
                                      -1  - ошибка
                                      9  - нет информации. Т.к. будут использоваться данные ПКК, то считаю код неактуальным
        v_OutChar IN OUT VARCHAR2 - строковый эквивалент возвращаемого значения
        v_OutArray OUT VARCHAR2   - выходной параметр для расширения (массив данных)
        v_ErrorCode OUT NUMBER    - Код ошибки (внутренний код ошибки объекта, если более Нуля то значит возникла проблема при работе)
        v_ErrorText OUT VARCHAR2  - Описание ошибки (детальное описание ошибки)
  */

  PROCEDURE FRAUD_0001(v_PROC_ID IN OUT NUMBER
                          ,v_Request_ID IN NUMBER, v_Person_ID IN NUMBER, v_OutCode IN VARCHAR2, v_InArray IN VARCHAR2
                          ,v_OutNumber OUT NUMBER, v_OutChar OUT VARCHAR2
                          ,v_OutArray OUT VARCHAR2
                          ,v_ErrorCode OUT NUMBER, v_ErrorText OUT VARCHAR2
                        );
                        
  PROCEDURE FRAUD_0002(v_PROC_ID IN OUT NUMBER 
                          ,v_Request_ID IN NUMBER, v_Person_ID IN NUMBER, v_OutCode IN VARCHAR2, v_InArray IN VARCHAR2
                          ,v_OutNumber OUT NUMBER, v_OutChar OUT VARCHAR2
                          ,v_OutArray OUT VARCHAR2
                          ,v_ErrorCode OUT NUMBER, v_ErrorText OUT VARCHAR2
                        );
                        
  PROCEDURE FRAUD_0003(v_PROC_ID IN OUT NUMBER 
                          ,v_Request_ID IN NUMBER, v_Person_ID IN NUMBER, v_OutCode IN VARCHAR2, v_InArray IN VARCHAR2
                          ,v_OutNumber OUT NUMBER, v_OutChar OUT VARCHAR2
                          ,v_OutArray OUT VARCHAR2
                          ,v_ErrorCode OUT NUMBER, v_ErrorText OUT VARCHAR2
                        );
                        
  PROCEDURE FRAUD_0004(v_PROC_ID IN OUT NUMBER 
                          ,v_Request_ID IN NUMBER, v_Person_ID IN NUMBER, v_OutCode IN VARCHAR2, v_InArray IN VARCHAR2
                          ,v_OutNumber OUT NUMBER, v_OutChar OUT VARCHAR2
                          ,v_OutArray OUT VARCHAR2
                          ,v_ErrorCode OUT NUMBER, v_ErrorText OUT VARCHAR2
                        );
                             
END AFS_RULES_PASP;

/

  GRANT EXECUTE ON "AFS"."AFS_RULES_PASP" TO "MVKARELIN";
--------------------------------------------------------
--  DDL for Package AFS_RULES_PHONE
--------------------------------------------------------

  CREATE OR REPLACE PACKAGE "AFS"."AFS_RULES_PHONE" AS 
-- ========================================================================
-- ==    ПАКЕТ со списком фрод правил. Базируется на номерах телефонов"
-- ========================================================================
-- ==	СОЗДАНИЕ:		  06.2016 (ТРАХАЧЕВ В.В.)
-- ==	МОДИФИКАЦИЯ:	06.2016 (ТРАХАЧЕВ В.В.)
-- ========================================================================
  /*Описание параметров для фрод правила.
        v_Request_ID IN NUMBER    - !входной параметр REQUEST_ID заявки из ПКК
        v_Person_ID IN NUMBER     - входной параметр PERSON_ID заявки из ПКК. 
        v_OutCode IN VARCHAR2     - Код внешней системы (для синхронизации между системами)
        v_InArray IN VARCHAR2     - входной параметр для расширения (массив данных)
 
        v_OutNumber OUT NUMBER    - !Выходные данные (результат по фрод-правилу)
                                      0   - фрода нет
                                      1   - фрод есть
                                      -1  - ошибка
                                      9  - нет информации. Т.к. будут использоваться данные ПКК, то считаю код неактуальным
        v_OutChar IN OUT VARCHAR2 - строковый эквивалент возвращаемого значения
        v_OutArray OUT VARCHAR2   - выходной параметр для расширения (массив данных)
        v_ErrorCode OUT NUMBER    - Код ошибки (внутренний код ошибки объекта, если более Нуля то значит возникла проблема при работе)
        v_ErrorText OUT VARCHAR2  - Описание ошибки (детальное описание ошибки)
  */

  PROCEDURE FRAUD_0001(v_PROC_ID IN OUT NUMBER 
                          ,v_Request_ID IN NUMBER, v_Person_ID IN NUMBER, v_OutCode IN VARCHAR2, v_InArray IN VARCHAR2
                          ,v_OutNumber OUT NUMBER, v_OutChar OUT VARCHAR2 
                          ,v_OutArray OUT VARCHAR2
                          ,v_ErrorCode OUT NUMBER, v_ErrorText OUT VARCHAR2
                        );
                        
  PROCEDURE FRAUD_0002(v_PROC_ID IN OUT NUMBER 
                          ,v_Request_ID IN NUMBER, v_Person_ID IN NUMBER, v_OutCode IN VARCHAR2, v_InArray IN VARCHAR2
                          ,v_OutNumber OUT NUMBER, v_OutChar OUT VARCHAR2
                          ,v_OutArray OUT VARCHAR2
                          ,v_ErrorCode OUT NUMBER, v_ErrorText OUT VARCHAR2
                        );
                        
  PROCEDURE FRAUD_0003(v_PROC_ID IN OUT NUMBER 
                          ,v_Request_ID IN NUMBER, v_Person_ID IN NUMBER, v_OutCode IN VARCHAR2, v_InArray IN VARCHAR2
                          ,v_OutNumber OUT NUMBER, v_OutChar OUT VARCHAR2
                          ,v_OutArray OUT VARCHAR2
                          ,v_ErrorCode OUT NUMBER, v_ErrorText OUT VARCHAR2
                        );
                        
  PROCEDURE FRAUD_0004(v_PROC_ID IN OUT NUMBER 
                          ,v_Request_ID IN NUMBER, v_Person_ID IN NUMBER, v_OutCode IN VARCHAR2, v_InArray IN VARCHAR2
                          ,v_OutNumber OUT NUMBER, v_OutChar OUT VARCHAR2
                          ,v_OutArray OUT VARCHAR2
                          ,v_ErrorCode OUT NUMBER, v_ErrorText OUT VARCHAR2
                        );
                             
END AFS_RULES_PHONE;

/

  GRANT EXECUTE ON "AFS"."AFS_RULES_PHONE" TO "MVKARELIN";
--------------------------------------------------------
--  DDL for Package AFS_RULES_REQUEST
--------------------------------------------------------

  CREATE OR REPLACE PACKAGE "AFS"."AFS_RULES_REQUEST" AS 
-- ========================================================================
-- ==    ПАКЕТ со списком фрод правил. Сравнение по заявкам"
-- ========================================================================
-- ==	СОЗДАНИЕ:		  06.2016 (ТРАХАЧЕВ В.В.)
-- ==	МОДИФИКАЦИЯ:	06.2016 (ТРАХАЧЕВ В.В.)
-- ========================================================================
  /*Описание параметров для фрод правила.
        v_Request_ID IN NUMBER    - !входной параметр REQUEST_ID заявки из ПКК
        v_Person_ID IN NUMBER     - входной параметр PERSON_ID заявки из ПКК. 
        v_OutCode IN VARCHAR2     - Код внешней системы (для синхронизации между системами)
        v_InArray IN VARCHAR2     - входной параметр для расширения (массив данных)
 
        v_OutNumber OUT NUMBER    - !Выходные данные (результат по фрод-правилу)
                                      0   - фрода нет
                                      1   - фрод есть
                                      -1  - ошибка
                                      9  - нет информации. Т.к. будут использоваться данные ПКК, то считаю код неактуальным
        v_OutChar IN OUT VARCHAR2 - строковый эквивалент возвращаемого значения
        v_OutArray OUT VARCHAR2   - выходной параметр для расширения (массив данных)
        v_ErrorCode OUT NUMBER    - Код ошибки (внутренний код ошибки объекта, если более Нуля то значит возникла проблема при работе)
        v_ErrorText OUT VARCHAR2  - Описание ошибки (детальное описание ошибки)
  */

  PROCEDURE FRAUD_0001(v_PROC_ID IN OUT NUMBER
                          ,v_Request_ID IN NUMBER, v_Person_ID IN NUMBER, v_OutCode IN VARCHAR2, v_InArray IN VARCHAR2
                          ,v_OutNumber OUT NUMBER, v_OutChar OUT VARCHAR2
                          ,v_OutArray OUT VARCHAR2
                          ,v_ErrorCode OUT NUMBER, v_ErrorText OUT VARCHAR2
                        );
                        
  PROCEDURE FRAUD_0002(v_PROC_ID IN OUT NUMBER
                          ,v_Request_ID IN NUMBER, v_Person_ID IN NUMBER, v_OutCode IN VARCHAR2, v_InArray IN VARCHAR2
                          ,v_OutNumber OUT NUMBER, v_OutChar OUT VARCHAR2
                          ,v_OutArray OUT VARCHAR2
                          ,v_ErrorCode OUT NUMBER, v_ErrorText OUT VARCHAR2
                        );
                        
  PROCEDURE FRAUD_0003(v_PROC_ID IN OUT NUMBER
                          ,v_Request_ID IN NUMBER, v_Person_ID IN NUMBER, v_OutCode IN VARCHAR2, v_InArray IN VARCHAR2
                          ,v_OutNumber OUT NUMBER, v_OutChar OUT VARCHAR2
                          ,v_OutArray OUT VARCHAR2
                          ,v_ErrorCode OUT NUMBER, v_ErrorText OUT VARCHAR2
                        );
                        
  PROCEDURE FRAUD_0004(v_PROC_ID IN OUT NUMBER
                          ,v_Request_ID IN NUMBER, v_Person_ID IN NUMBER, v_OutCode IN VARCHAR2, v_InArray IN VARCHAR2
                          ,v_OutNumber OUT NUMBER, v_OutChar OUT VARCHAR2
                          ,v_OutArray OUT VARCHAR2
                          ,v_ErrorCode OUT NUMBER, v_ErrorText OUT VARCHAR2
                        );
                             
END AFS_RULES_REQUEST;

/
--------------------------------------------------------
--  DDL for Package AFS_RULES_TEST
--------------------------------------------------------

  CREATE OR REPLACE PACKAGE "AFS"."AFS_RULES_TEST" AS 
-- ========================================================================
-- ==    ПАКЕТ со списком ТЕСТОВЫХ фрод правил. "
-- ========================================================================
-- ==	СОЗДАНИЕ:		  06.2016 (ТРАХАЧЕВ В.В.)
-- ==	МОДИФИКАЦИЯ:	06.2016 (ТРАХАЧЕВ В.В.)
-- ========================================================================
  /*Описание параметров для фрод правила.
        v_Request_ID IN NUMBER    - !входной параметр REQUEST_ID заявки из ПКК
        v_Person_ID IN NUMBER     - входной параметр PERSON_ID заявки из ПКК. 
        v_OutCode IN VARCHAR2     - Код внешней системы (для синхронизации между системами)
        v_InArray IN VARCHAR2     - входной параметр для расширения (массив данных)
 
        v_OutNumber OUT NUMBER    - !Выходные данные (результат по фрод-правилу)
                                      0   - фрода нет
                                      1   - фрод есть
                                      -1  - ошибка
                                      9  - нет информации. Т.к. будут использоваться данные ПКК, то считаю код неактуальным
        v_OutChar IN OUT VARCHAR2 - строковый эквивалент возвращаемого значения
        v_OutArray OUT VARCHAR2   - выходной параметр для расширения (массив данных)
        v_ErrorCode OUT NUMBER    - Код ошибки (внутренний код ошибки объекта, если более Нуля то значит возникла проблема при работе)
        v_ErrorText OUT VARCHAR2  - Описание ошибки (детальное описание ошибки)
  */

  PROCEDURE FRAUD_0001(v_PROC_ID IN OUT NUMBER 
                          ,v_Request_ID IN NUMBER, v_Person_ID IN NUMBER, v_OutCode IN VARCHAR2, v_InArray IN VARCHAR2
                          ,v_OutNumber OUT NUMBER, v_OutChar OUT VARCHAR2
                          ,v_OutArray OUT VARCHAR2
                          ,v_ErrorCode OUT NUMBER, v_ErrorText OUT VARCHAR2
                        );
                        
  PROCEDURE FRAUD_0002(v_PROC_ID IN OUT NUMBER 
                          ,v_Request_ID IN NUMBER, v_Person_ID IN NUMBER, v_OutCode IN VARCHAR2, v_InArray IN VARCHAR2
                          ,v_OutNumber OUT NUMBER, v_OutChar OUT VARCHAR2
                          ,v_OutArray OUT VARCHAR2
                          ,v_ErrorCode OUT NUMBER, v_ErrorText OUT VARCHAR2
                        );
                        
  PROCEDURE FRAUD_0003(v_PROC_ID IN OUT NUMBER 
                          ,v_Request_ID IN NUMBER, v_Person_ID IN NUMBER, v_OutCode IN VARCHAR2, v_InArray IN VARCHAR2
                          ,v_OutNumber OUT NUMBER, v_OutChar OUT VARCHAR2
                          ,v_OutArray OUT VARCHAR2
                          ,v_ErrorCode OUT NUMBER, v_ErrorText OUT VARCHAR2
                        );

  PROCEDURE FRAUD_0004(v_PROC_ID IN OUT NUMBER 
                          ,v_Request_ID IN NUMBER, v_Person_ID IN NUMBER, v_OutCode IN VARCHAR2, v_InArray IN VARCHAR2
                          ,v_OutNumber OUT NUMBER, v_OutChar OUT VARCHAR2
                          ,v_OutArray OUT VARCHAR2
                          ,v_ErrorCode OUT NUMBER, v_ErrorText OUT VARCHAR2
                        );                      
END AFS_RULES_TEST;

/
--------------------------------------------------------
--  DDL for Package AFS_SYS
--------------------------------------------------------

  CREATE OR REPLACE PACKAGE "AFS"."AFS_SYS" AS 
--Процедура занесения информации в лог ошибок.
  PROCEDURE WRITE_LOG_ERROR (v_PROC_ID IN NUMBER, v_E_CODE IN NUMBER
                          , v_E_TEXT IN VARCHAR2, v_E_TYPE IN VARCHAR2, v_E_INFO IN VARCHAR2);

--Процедура занесения информации в лог вызова процедур фрод-правил.
  PROCEDURE WRITE_LOG_CALL (
          v_PROC_ID IN NUMBER     --уникальный ID процедуры 
          , v_FRAUD_ID IN NUMBER  --внутренний ID правила по справочнику
          ,v_FRAUD_NAME IN VARCHAR2 --имя правила по справочнику
          ,v_REQUEST_ID IN NUMBER --ID заявки ПКК
          , v_PERSON_ID IN NUMBER --ID физика ПКК
          ,v_OUTCODE IN VARCHAR2  --Код внешней системы (для синхронизации между системами)
          ,v_INARRAY IN VARCHAR2  --Параметр для расширения (массив данных, структуру проработаем позже)
          ,v_OUTNUMBER IN NUMBER  --–Выходные данные, число
          ,v_OUTCHAR IN VARCHAR2  --Выходные данные, строка (если null, то дублирует первый параметр - number)
          ,v_OUTARRAY IN VARCHAR2 --Выходной данные для расширения (массив данных, структуру проработаем позже)
          ,v_E_CODE IN OUT NUMBER  --Код ошибки (внутренний код ошибки объекта, если >0 то значит возникла проблема при работе) 
          ,v_E_TEXT IN OUT VARCHAR2 --Описание ошибки (детальное описание ошибки, если ErrorCode>0)
        ); 

--Процедура занесения информации в лог ошибок.
  PROCEDURE WRITE_AFS_HISTORY(
          v_PROC_ID IN NUMBER     --уникальный ID процедуры 
          ,v_REQUEST_ID IN NUMBER --ID заявки ПКК
          ,v_PERSON_ID IN NUMBER --ID физика ПКК
          ,v_REQUEST_DATE IN DATE --дата заявки в ПКК
          ,v_FRAUD_ID IN NUMBER  --внутренний ID правила по справочнику
          ,v_FRAUD_OUTNUMBER IN NUMBER --Выходные данные по проверке фрод правила, число
          ,v_FIO IN VARCHAR2           --ФИО на тек. момент по заявке. Если пустое, то можно подтянуть после
          ,v_DR IN VARCHAR2            --ДР  на тек. момент по заявке. Если пустое, то можно подтянуть после
          ,v_DAY_BETWEEN IN NUMBER     --Дней между параметрами. Расчетный параметр
          ,v_REQUEST_ID_REL IN NUMBER  --ID обнаруженной по правилу заявки ПКК
          ,v_PERSON_ID_REL IN NUMBER   --ID обнаруженного по правилу физика ПКК
          ,v_REQUEST_DATE_REL IN DATE  --дата обнаруженной по правилу заявки в ПКК
          ,v_FIO_REL IN VARCHAR2       --ФИО на тек. момент по обнаруженной заявке. Если пустое, то можно подтянуть потом
          ,v_DR_REL IN VARCHAR2        --ДР  на тек. момент по обнаруженной заявке. Если пустое, то можно подтянуть потом
          ,v_INFO_EQUAL IN	VARCHAR2         --Информация которая провери
          ,v_INFO_NOT_EQUAL IN	VARCHAR2
          ,v_INFO_NOT_EQUAL_REL IN VARCHAR2
          ,v_E_CODE IN OUT NUMBER  --Код ошибки (внутренний код ошибки объекта, если >0 то значит возникла проблема при работе) 
          ,v_E_TEXT IN OUT VARCHAR2 --Описание ошибки (детальное описание ошибки, если ErrorCode>0)
                        );


END AFS_SYS;

/
--------------------------------------------------------
--  DDL for Package Body AFS_FILTER
--------------------------------------------------------

  CREATE OR REPLACE PACKAGE BODY "AFS"."AFS_FILTER" AS

  FUNCTION CHECK_REQUEST_ID_MAIN (v_RID IN NUMBER) RETURN NUMBER AS
    p_FlagAct NUMBER := 0; --возвращаемое значение 1 - фильрация сработала. 0 - фильтр пропустил заявку
  BEGIN
    SELECT COUNT(*) INTO p_FlagAct FROM dual
		WHERE EXISTS(SELECT REQUEST_ID FROM KREDIT.C_REQUEST_REACT WHERE REQUEST_ID = v_RID
						AND (request_old_status_id = 200 and request_new_status_id = 7) )      --убираем отказы на прескоринге
			OR EXISTS(SELECT REQUEST_ID FROM KREDIT.C_REQUEST WHERE REQUEST_ID = v_RID 
          AND
          (NOT TYPE_REQUEST_ID=1                                                     --убираем не кредитные заявки
            OR CREATED_GROUP_ID IN(4862,4863,4865,4864,5736,5899,7674,11455,10165)   --убираем интернет-заявки и пакетную генерацию
            --OR NOT (status_id IN(7,8,9,11,14,16,17,21))                              --убираем незавершенные заявки
            OR score_tree_route_id IN(
                1	--Мгновенные кросс-продажи
                ,4	--Для тестирования интерфейса
                ,6	--предпринятое решение
                ,7	--Дистанционное Увеличение лимита КК
                ,9	--Изменение существенных условий по ТП «КК новогодний кредит»
                ,10	--Увеличение лимита по ТП «КК новогодний кредит»
                ,14	--Повышение ставки по договору сотрудника
                ,16	--Изменение существенных условий по ТП «КК Аппетитный кредит»
                ,17	--Увеличение лимита по ТП «КК Аппетитный кредит»
                ,18	--Выдача кредитов МФО
                ,20	--Изменение лимита по ВИП КК
                ,35	--Мгновенные КК вкладчикам (Instant)
                )
            )
          )
      OR EXISTS(SELECT 1 --, TO_NUMBER(vaa.ATRIBUTS_VALUE) AS SALE_ID
                  FROM KREDIT.C_REQUEST_REACT crr
                  INNER JOIN objects_atributs oa
                     ON oa.objects_id=crr.REACT_GROUP_ID AND oa.ATRIBUTS_ID=51 AND oa.OBJECTS_TYPE=11
                  INNER join atributs_values vaa 
                    on oa.atributs_values = vaa.atributs_values AND vaa.ATRIBUTS_VALUE IN('7', '10', '20', '26') 
                  WHERE REQUEST_ID = v_RID
                  )
        ;
    RETURN p_FlagAct;
    
    EXCEPTION WHEN OTHERS THEN RETURN -1;
  END CHECK_REQUEST_ID_MAIN;

  FUNCTION CHECK_REQUEST_ID_REL (v_RID IN NUMBER) RETURN NUMBER AS
    p_FlagAct NUMBER := 0; --возвращаемое значение 1 - фильрация сработала. 0 - фильтр пропустил заявку
  BEGIN
    SELECT COUNT(*) INTO p_FlagAct FROM dual
		WHERE EXISTS(SELECT REQUEST_ID FROM KREDIT.C_REQUEST WHERE REQUEST_ID = v_RID 
          AND 
          (NOT TYPE_REQUEST_ID=1                                                     --убираем не кредитные заявки
            OR CREATED_GROUP_ID IN(4862,4863,4865,4864,5736,5899,7674,11455,10165)   --убираем интернет-заявки и пакетную генерацию
            OR NOT status_id IN(7,8,9,11,14,16,17,21)                                --убираем не завершенные заявки
            OR score_tree_route_id IN(
                1	--Мгновенные кросс-продажи
                ,4	--Для тестирования интерфейса
                ,6	--предпринятое решение
                ,7	--Дистанционное Увеличение лимита КК
                ,9	--Изменение существенных условий по ТП «КК новогодний кредит»
                ,10	--Увеличение лимита по ТП «КК новогодний кредит»
                ,14	--Повышение ставки по договору сотрудника
                ,16	--Изменение существенных условий по ТП «КК Аппетитный кредит»
                ,17	--Увеличение лимита по ТП «КК Аппетитный кредит»
                ,18	--Выдача кредитов МФО
                ,20	--Изменение лимита по ВИП КК
                ,35	--Мгновенные КК вкладчикам (Instant)
                )
            )
          )
        ;
    RETURN p_FlagAct;
  
    EXCEPTION WHEN OTHERS THEN RETURN -1;
  END CHECK_REQUEST_ID_REL;

END AFS_FILTER;

/
--------------------------------------------------------
--  DDL for Package Body AFS_RULES
--------------------------------------------------------

  CREATE OR REPLACE PACKAGE BODY "AFS"."AFS_RULES" AS
  --тестовая процедура
  PROCEDURE FRAUD_RULE_0000(v_Request_ID IN NUMBER, v_Person_ID IN NUMBER, v_OutCode IN VARCHAR2, v_InArray IN VARCHAR2
                          ,v_OutNumber OUT NUMBER, v_OutChar IN OUT VARCHAR2
                          ,v_OutArray OUT VARCHAR2 
                          ,v_ErrorCode OUT NUMBER, v_ErrorText OUT VARCHAR2
                        ) AS
      p_PROC_ID_NEW   NUMBER;        --уникальный ID процесса. Генерируется при запуске фрод правила
      p_FRAUD_ID			NUMBER := 0;   --ID правила. Определяется по справочнику D_FRAUD_RULE
      p_FRAUD_NM			VARCHAR2(255) := 'AFS.AFS_RULES.FRAUD_RULE_0000';   --ID правила. Определяется по справочнику D_FRAUD_RULE
      p_FlagEx 				NUMBER;        --Флаг наличия заявки в ПКК
      
      p_ErrorCode_Sys NUMBER;          --Системный код ошибки. В выходной параметр может не передаваться если не существенен
      p_ErrorText_Sys VARCHAR2(4000);  --Системный текст ошибки. В выходной параметр может не передаваться если не существенен
	  
      pragma autonomous_transaction;  
  BEGIN
      --определяем новый уникальный ID процесса
      SELECT AFS.AFS_PROC_SEQ.NEXTVAL INTO p_PROC_ID_NEW FROM DUAL;
  
      v_OutNumber := NULL;
      v_OutChar := '';
      v_OutArray := '';
      p_ErrorCode_Sys := SQLCODE;
      p_ErrorText_Sys := SUBSTR(SQLCODE||' | '||SQLERRM,0,4000);
      
      --пишем в лог вызова фрод-правил
      afs_sys.write_log_call(v_PROC_ID => p_PROC_ID_NEW, v_FRAUD_ID => p_FRAUD_ID, v_FRAUD_NAME => p_FRAUD_NM
            , v_REQUEST_ID => v_Request_ID, v_PERSON_ID => v_Person_ID
            , v_OUTCODE => v_OutCode, v_INARRAY => v_InArray
            , v_OUTNUMBER => v_OutNumber, v_OUTCHAR => v_OutChar, v_OUTARRAY => v_OutArray
            , v_E_CODE => p_ErrorCode_Sys, v_E_TEXT => p_ErrorText_Sys);

      --проверяем существует ли заявка
      SELECT COUNT(*) INTO p_FlagEx FROM DUAL WHERE EXISTS(SELECT 1 FROM KREDIT.C_REQUEST WHERE REQUEST_ID=v_Request_ID);
      
      CASE WHEN p_FlagEx=1 THEN 
      FOR ic IN (SELECT * FROM 
                  (SELECT sysdate as DATE_FRAUD
                    ,v_Request_ID AS REQUEST_ID
                    ,v_Person_ID AS PERSON_ID
                    ,NULL AS REQUEST_DATE
                    ,NULL AS FIO, NULL AS DR
                    ,NULL AS DAY_BETWEEN
                    ,NULL AS REQUEST_ID_REL
                    ,NULL AS PERSON_ID_REL
                    ,NULL AS REQUEST_DATE_REL
                    ,NULL AS FIO_REL, NULL AS DR_REL
                    --INFO: для каждого правила переобозначить инфу в колонках INFO_EQ, INFO_NEQ, INFO_NEQ_REL
                    ,'Паспорт:'||'ssss nnnnnn' as INFO_EQUAL
                    ,'Фио+Др.:'||'Ф И О дд.мм.гггг' as INFO_NOT_EQUAL
                    ,'Фио+Др.:'||'Ф И О дд.мм.гггг' as INFO_NOT_EQUAL_REL
                    ,1 as FRAUD_OUTNUMBER
                    --,NULL as F_POS
                  FROM DUAL
                  ) TAB
                WHERE rownum=1)
        LOOP
          v_OutNumber := ic.FRAUD_OUTNUMBER;
          v_OutChar := TO_CHAR(v_OutNumber);
		  
          afs_sys.write_afs_history(v_PROC_ID => p_PROC_ID_NEW
              , v_REQUEST_ID 			=> v_Request_ID
              , v_PERSON_ID 			=> ic.PERSON_ID
              , v_REQUEST_DATE 		=> ic.REQUEST_DATE
              , v_FRAUD_ID 				=> p_FRAUD_ID
              , v_FRAUD_OUTNUMBER => v_OutNumber
              , v_FIO 					  => ic.FIO 
              , v_DR 					    => ic.DR
              , v_DAY_BETWEEN 		=> ic.DAY_BETWEEN
              , v_REQUEST_ID_REL 		=> ic.REQUEST_ID_REL
              , v_PERSON_ID_REL 		=> ic.PERSON_ID_REL
              , v_REQUEST_DATE_REL 	=> ic.REQUEST_DATE_REL
              , v_FIO_REL 				  => ic.FIO_REL 
              , v_DR_REL 				    => ic.DR_REL
              , v_INFO_EQUAL 			   => ic.INFO_EQUAL
              , v_INFO_NOT_EQUAL 		 => ic.INFO_NOT_EQUAL
              , v_INFO_NOT_EQUAL_REL => ic.INFO_NOT_EQUAL_REL
              , v_E_CODE => v_ErrorCode, v_E_TEXT => v_ErrorText);
        END LOOP;
      WHEN p_FlagEx=0 THEN --если заявки нет присваиваем статус
        v_OutNumber := 9;
        v_OutChar := TO_CHAR(v_OutNumber);
      END CASE;
      
      --если фрод не нашелся возвращаем 0
      IF (v_OutNumber IS NULL) THEN 
        v_OutNumber := 0;
        v_OutChar := TO_CHAR(v_OutNumber);
      END IF;
    
      p_ErrorCode_Sys := SQLCODE;
      p_ErrorText_Sys := SUBSTR(SQLCODE||' | '||SQLERRM,0,4000);
    --пишем результат в лог вызова фрод-правила полученные данные
    afs_sys.write_log_call(v_PROC_ID => p_PROC_ID_NEW, v_FRAUD_ID => p_FRAUD_ID, v_FRAUD_NAME => p_FRAUD_NM
          , v_REQUEST_ID => v_Request_ID, v_PERSON_ID => v_Person_ID
          , v_OUTCODE => v_OutCode, v_INARRAY => v_InArray
          , v_OUTNUMBER => v_OutNumber, v_OUTCHAR => v_OutChar, v_OUTARRAY => v_OutArray
          , v_E_CODE => p_ErrorCode_Sys, v_E_TEXT => p_ErrorText_Sys);
    v_ErrorCode := p_ErrorCode_Sys;
    v_ErrorText := p_ErrorText_Sys;
	COMMIT;

    EXCEPTION WHEN OTHERS
      THEN 
        v_OutNumber := -1;
        v_OutChar := TO_CHAR(v_OutNumber);
        --присваиваем ошибки для системного мониторинга и для возврата
        p_ErrorCode_Sys := SQLCODE;
        p_ErrorText_Sys := v_ErrorText;
        v_ErrorCode := p_ErrorCode_Sys;
        v_ErrorText := p_ErrorText_Sys;
        --пишем в лог вызова
        afs_sys.write_log_call(v_PROC_ID => p_PROC_ID_NEW, v_FRAUD_ID => p_FRAUD_ID, v_FRAUD_NAME => p_FRAUD_NM
              , v_REQUEST_ID => v_Request_ID, v_PERSON_ID => v_Person_ID
              , v_OUTCODE => v_OutCode, v_INARRAY => v_InArray
              , v_OUTNUMBER => v_OutNumber, v_OUTCHAR => v_OutChar, v_OUTARRAY => v_OutArray
              , v_E_CODE => p_ErrorCode_Sys, v_E_TEXT => p_ErrorText_Sys);
        --если нужно вернуть ошибку при записи в лог в выходных параметрах, то присваиваем параметрам
        /*v_ErrorCode := p_ErrorCode_Sys;
        v_ErrorText := p_ErrorText_Sys;*/
  END FRAUD_RULE_0000;
 

 PROCEDURE FRAUD_RULE_0001(v_Request_ID IN NUMBER, v_Person_ID IN NUMBER, v_OutCode IN VARCHAR2, v_InArray IN VARCHAR2
                          ,v_OutNumber OUT NUMBER, v_OutChar IN OUT VARCHAR2
                          ,v_OutArray OUT VARCHAR2
                          ,v_ErrorCode OUT NUMBER, v_ErrorText OUT VARCHAR2
                        ) AS
      p_PROC_ID_NEW   NUMBER;        --уникальный ID процесса. Генерируется при запуске фрод правила
      p_FRAUD_ID			NUMBER := 1;   --ID правила. Определяется по справочнику D_FRAUD_RULE
      p_FRAUD_NM			VARCHAR2(255) := 'AFS.AFS_RULES.FRAUD_RULE_0001';   --ID правила. Определяется по справочнику D_FRAUD_RULE
      p_FlagEx 				NUMBER;        --Флаг наличия заявки в ПКК
      
      p_ErrorCode_Sys NUMBER;          --Системный код ошибки. В выходной параметр может не передаваться если не существенен
      p_ErrorText_Sys VARCHAR2(4000);  --Системный текст ошибки. В выходной параметр может не передаваться если не существенен
	  
      pragma autonomous_transaction;
  BEGIN
      --определяем новый уникальный ID процесса
      SELECT AFS.AFS_PROC_SEQ.NEXTVAL INTO p_PROC_ID_NEW FROM DUAL;
  
      v_OutNumber := NULL;
      v_OutChar := '';
      v_OutArray := '';
      p_ErrorCode_Sys := SQLCODE;
      p_ErrorText_Sys := SUBSTR(SQLCODE||' | '||SQLERRM,0,4000);
      
      --пишем в лог вызова фрод-правил
      afs_sys.write_log_call(v_PROC_ID => p_PROC_ID_NEW, v_FRAUD_ID => p_FRAUD_ID, v_FRAUD_NAME => p_FRAUD_NM
            , v_REQUEST_ID => v_Request_ID, v_PERSON_ID => v_Person_ID
            , v_OUTCODE => v_OutCode, v_INARRAY => v_InArray
            , v_OUTNUMBER => v_OutNumber, v_OUTCHAR => v_OutChar, v_OUTARRAY => v_OutArray
            , v_E_CODE => p_ErrorCode_Sys, v_E_TEXT => p_ErrorText_Sys);
      
      --проверяем существует ли заявка
      SELECT COUNT(*) INTO p_FlagEx FROM DUAL WHERE EXISTS(SELECT 1 FROM KREDIT.C_REQUEST WHERE REQUEST_ID=v_Request_ID
        AND AFS.afs_filter.check_request_id_main(v_Request_ID)=0);
      
      CASE WHEN p_FlagEx=1 THEN 
        FOR ic IN (SELECT * FROM (      
          SELECT DISTINCT v_src.REQUEST_ID, v_src.OBJECTS_ID AS PERSON_ID, v_src.CREATED_DATE AS REQUEST_DATE
              , v_rel.REQUEST_ID AS REQUEST_ID_REL, v_rel.OBJECTS_ID AS PERSON_ID_REL
              , v_rel.CREATED_DATE AS REQUEST_DATE_REL
              , ABS(ROUND(v_src.CREATED_DATE-v_rel.CREATED_DATE, 2)) as DAY_BETWEEN
              , 'Паспорт:'||v_src.DOCUMENTS_SERIAL||v_src.DOCUMENTS_NUMBER as INFO_EQUAL
              ,'Фио+Др.:'||v_src.FIO||' '||v_src.BIRTH as INFO_NOT_EQUAL
              ,DENSE_RANK() OVER (PARTITION BY v_src.OBJECTS_ID 
                          ORDER BY v_src.fio_akt DESC,  v_src.FIO_CREATED DESC, v_src.FIO_MOD DESC
                            ,v_src.BIRTH_AKT DESC, v_src.BIRTH_CREATED DESC, v_src.BIRTH_MOD DESC) as RANK
              ,'Фио+Др.:'||v_rel.FIO||' '||v_rel.BIRTH as INFO_NOT_EQUAL_REL
              ,DENSE_RANK() OVER (PARTITION BY v_rel.OBJECTS_ID 
                          ORDER BY v_rel.fio_akt DESC,  v_rel.FIO_CREATED DESC, v_rel.FIO_MOD DESC
                            ,v_rel.BIRTH_AKT DESC, v_rel.BIRTH_CREATED DESC, v_rel.BIRTH_MOD DESC) as RANK_REL
              ,NULL AS FIO, NULL AS DR
              ,NULL AS FIO_REL, NULL AS DR_REL
              ,1 AS FRAUD_OUTNUMBER
            FROM AFS.VR_DOC_FB v_src
            INNER JOIN AFS.VR_DOC_FB v_rel
              ON v_src.DOCUMENTS_SERIAL = v_rel.DOCUMENTS_SERIAL
                AND v_src.DOCUMENTS_NUMBER = v_rel.DOCUMENTS_NUMBER
                AND v_src.DOCUMENTS_TYPE = v_rel.DOCUMENTS_TYPE
                AND v_src.OBJECTS_ID ^= v_rel.OBJECTS_ID
              AND v_src.REQUEST_ID > v_rel.REQUEST_ID
            WHERE v_src.REQUEST_ID = v_Request_ID AND v_src.DOCUMENTS_TYPE=21
              AND (v_src.FIO^=v_rel.FIO OR v_src.BIRTH^=v_rel.BIRTH)
              AND AFS.FN_DIST_LEV(v_src.FIO||v_src.BIRTH, v_rel.FIO||v_rel.BIRTH)>1
              --AND (AFS.FN_DIST_LEV(v_src.FIO, v_rel.FIO)>1 OR AFS.FN_DIST_LEV(v_src.BIRTH, v_rel.BIRTH)>1)
           ) WHERE RANK=1 AND RANK_REL=1 
              --оставляем только заявки не попавшие под общий фильтр
              AND AFS.afs_filter.check_request_id_rel(REQUEST_ID_REL)=0)
        LOOP
          v_OutNumber := ic.FRAUD_OUTNUMBER;
          v_OutChar := TO_CHAR(v_OutNumber);
		  
          afs_sys.write_afs_history(v_PROC_ID => p_PROC_ID_NEW
              , v_REQUEST_ID 			=> v_Request_ID
              , v_PERSON_ID 			=> ic.PERSON_ID
              , v_REQUEST_DATE 		=> ic.REQUEST_DATE
              , v_FRAUD_ID 				=> p_FRAUD_ID
              , v_FRAUD_OUTNUMBER => v_OutNumber
              , v_FIO 					  => ic.FIO 
              , v_DR 					    => ic.DR
              , v_DAY_BETWEEN 		=> ic.DAY_BETWEEN
              , v_REQUEST_ID_REL 		=> ic.REQUEST_ID_REL
              , v_PERSON_ID_REL 		=> ic.PERSON_ID_REL
              , v_REQUEST_DATE_REL 	=> ic.REQUEST_DATE_REL
              , v_FIO_REL 				  => ic.FIO_REL 
              , v_DR_REL 				    => ic.DR_REL
              , v_INFO_EQUAL 			   => ic.INFO_EQUAL
              , v_INFO_NOT_EQUAL 		 => ic.INFO_NOT_EQUAL
              , v_INFO_NOT_EQUAL_REL => ic.INFO_NOT_EQUAL_REL
              , v_E_CODE => v_ErrorCode, v_E_TEXT => v_ErrorText);
        END LOOP;
      WHEN p_FlagEx=0 THEN --если заявки нет присваиваем статус
        v_OutNumber := 9;
        v_OutChar := TO_CHAR(v_OutNumber);
      END CASE;
      
      --если фрод не нашелся возвращаем 0
      IF (v_OutNumber IS NULL) THEN 
        v_OutNumber := 0;
        v_OutChar := TO_CHAR(v_OutNumber);
      END IF;
    
      p_ErrorCode_Sys := SQLCODE;
      p_ErrorText_Sys := SUBSTR(SQLCODE||' | '||SQLERRM,0,4000);
    --пишем результат в лог вызова фрод-правила полученные данные
    afs_sys.write_log_call(v_PROC_ID => p_PROC_ID_NEW, v_FRAUD_ID => p_FRAUD_ID, v_FRAUD_NAME => p_FRAUD_NM
          , v_REQUEST_ID => v_Request_ID, v_PERSON_ID => v_Person_ID
          , v_OUTCODE => v_OutCode, v_INARRAY => v_InArray
          , v_OUTNUMBER => v_OutNumber, v_OUTCHAR => v_OutChar, v_OUTARRAY => v_OutArray
          , v_E_CODE => p_ErrorCode_Sys, v_E_TEXT => p_ErrorText_Sys);
    v_ErrorCode := p_ErrorCode_Sys;
    v_ErrorText := p_ErrorText_Sys;
	COMMIT;

    EXCEPTION WHEN OTHERS
      THEN 
        v_OutNumber := -1;
        v_OutChar := TO_CHAR(v_OutNumber);
        --присваиваем ошибки для системного мониторинга и для возврата
        p_ErrorCode_Sys := SQLCODE;
        p_ErrorText_Sys := v_ErrorText;
        v_ErrorCode := p_ErrorCode_Sys;
        v_ErrorText := p_ErrorText_Sys;
        --пишем в лог вызова
        afs_sys.write_log_call(v_PROC_ID => p_PROC_ID_NEW, v_FRAUD_ID => p_FRAUD_ID, v_FRAUD_NAME => p_FRAUD_NM
              , v_REQUEST_ID => v_Request_ID, v_PERSON_ID => v_Person_ID
              , v_OUTCODE => v_OutCode, v_INARRAY => v_InArray
              , v_OUTNUMBER => v_OutNumber, v_OUTCHAR => v_OutChar, v_OUTARRAY => v_OutArray
              , v_E_CODE => p_ErrorCode_Sys, v_E_TEXT => p_ErrorText_Sys);
        --если нужно вернуть ошибку при записи в лог в выходных параметрах, то присваиваем параметрам
        /*v_ErrorCode := p_ErrorCode_Sys;
        v_ErrorText := p_ErrorText_Sys;*/
  END FRAUD_RULE_0001;

  PROCEDURE FRAUD_RULE_0002(v_Request_ID IN NUMBER, v_Person_ID IN NUMBER, v_OutCode IN VARCHAR2, v_InArray IN VARCHAR2
                          ,v_OutNumber OUT NUMBER, v_OutChar IN OUT VARCHAR2
                          ,v_OutArray OUT VARCHAR2
                          ,v_ErrorCode OUT NUMBER, v_ErrorText OUT VARCHAR2
                        ) AS
      p_PROC_ID_NEW   NUMBER;        --уникальный ID процесса. Генерируется при запуске фрод правила
      p_FRAUD_ID			NUMBER := 2;   --ID правила. Определяется по справочнику D_FRAUD_RULE
      p_FRAUD_NM			VARCHAR2(255) := 'AFS.AFS_RULES.FRAUD_RULE_0002';   --ID правила. Определяется по справочнику D_FRAUD_RULE
      p_FlagEx 				NUMBER;        --Флаг наличия заявки в ПКК
      
      p_ErrorCode_Sys NUMBER;          --Системный код ошибки. В выходной параметр может не передаваться если не существенен
      p_ErrorText_Sys VARCHAR2(4000);  --Системный текст ошибки. В выходной параметр может не передаваться если не существенен
	  
      pragma autonomous_transaction;
  BEGIN
      --определяем новый уникальный ID процесса
      SELECT AFS.AFS_PROC_SEQ.NEXTVAL INTO p_PROC_ID_NEW FROM DUAL;
  
      v_OutNumber := NULL;
      v_OutChar := '';
      v_OutArray := '';
      p_ErrorCode_Sys := SQLCODE;
      p_ErrorText_Sys := SUBSTR(SQLCODE||' | '||SQLERRM,0,4000);
      
      --пишем в лог вызова фрод-правил
      afs_sys.write_log_call(v_PROC_ID => p_PROC_ID_NEW, v_FRAUD_ID => p_FRAUD_ID, v_FRAUD_NAME => p_FRAUD_NM
            , v_REQUEST_ID => v_Request_ID, v_PERSON_ID => v_Person_ID
            , v_OUTCODE => v_OutCode, v_INARRAY => v_InArray
            , v_OUTNUMBER => v_OutNumber, v_OUTCHAR => v_OutChar, v_OUTARRAY => v_OutArray
            , v_E_CODE => p_ErrorCode_Sys, v_E_TEXT => p_ErrorText_Sys);
      
      --проверяем существует ли заявка
      SELECT COUNT(*) INTO p_FlagEx FROM DUAL WHERE EXISTS(SELECT 1 FROM KREDIT.C_REQUEST WHERE REQUEST_ID=v_Request_ID
        AND AFS.afs_filter.check_request_id_main(v_Request_ID)=0);
      
      CASE WHEN p_FlagEx=1 THEN 
        FOR ic IN ( SELECT * FROM (     
                    SELECT DISTINCT cr.REQUEST_ID, cr.OBJECTS_ID AS PERSON_ID, cr.CREATED_DATE AS REQUEST_DATE
                        , cr_rel.REQUEST_ID AS REQUEST_ID_REL, cr_rel.OBJECTS_ID AS PERSON_ID_REL
                        , cr_rel.CREATED_DATE AS REQUEST_DATE_REL
                        , ABS(ROUND(cr.CREATED_DATE-cr_rel.CREATED_DATE, 2)) as DAY_BETWEEN
                        , 'Паспорт:'||vdh_src.DOCUMENTS_SERIAL||vdh_src.DOCUMENTS_NUMBER as INFO_EQUAL
                        ,'Фио+Др.:'||vfio.FIO4SEARCH||' '||vb.BIRTH as INFO_NOT_EQUAL
                        ,DENSE_RANK() OVER (PARTITION BY vfio.FIO_OBJECTS_ID 
                                    ORDER BY vfio.fio_akt DESC,  vfio.FIO_CREATED DESC, vfio.MODIFICATION_DATE DESC
                                      ,vb.BIRTH_AKT DESC, vb.BIRTH_CREATED DESC, vb.MODIFICATION_DATE DESC) as RANK
                        ,'Фио+Др.:'||vfio_rel.FIO4SEARCH||' '||vb_rel.BIRTH as INFO_NOT_EQUAL_REL
                        ,DENSE_RANK() OVER (PARTITION BY vfio_rel.FIO_OBJECTS_ID 
                                    ORDER BY vfio_rel.fio_akt DESC,  vfio_rel.FIO_CREATED DESC, vfio_rel.MODIFICATION_DATE DESC
                                      ,vb_rel.BIRTH_AKT DESC, vb_rel.BIRTH_CREATED DESC, vb_rel.MODIFICATION_DATE DESC) as RANK_REL
                        ,NULL AS FIO, NULL AS DR
                        ,NULL AS FIO_REL, NULL AS DR_REL
                        ,1 AS FRAUD_OUTNUMBER
                      FROM KREDIT.C_REQUEST cr
                      INNER JOIN KREDIT.VIEW_DOCUMENTS_HISTORY vdh_src
                        ON cr.OBJECTS_ID = vdh_src.OBJECTS_ID AND vdh_src.DOCUMENTS_TYPE=21
                      INNER JOIN KREDIT.VIEW_DOCUMENTS_HISTORY vdh_rel
                        ON vdh_src.DOCUMENTS_SERIAL = vdh_rel.DOCUMENTS_SERIAL
                          AND vdh_src.DOCUMENTS_NUMBER = vdh_rel.DOCUMENTS_NUMBER
                          AND vdh_rel.DOCUMENTS_TYPE=vdh_src.DOCUMENTS_TYPE
                          AND vdh_src.OBJECTS_ID ^= vdh_rel.OBJECTS_ID
                      INNER JOIN KREDIT.C_REQUEST cr_rel
                        ON vdh_rel.OBJECTS_ID = cr_rel.OBJECTS_ID
                          AND cr.REQUEST_ID > cr_rel.REQUEST_ID
                      INNER JOIN AFS.VIEW_FIO_HISTORY vfio
                        ON vfio.FIO_OBJECTS_ID = vdh_src.OBJECTS_ID AND vfio.FIO_CREATED < cr.CREATED_DATE+1
                      INNER JOIN AFS.VIEW_BIRTH vb
                        ON vb.OBJECTS_ID = vdh_src.OBJECTS_ID AND vb.BIRTH_CREATED < cr.CREATED_DATE+1
                      INNER JOIN AFS.VIEW_FIO_HISTORY vfio_rel
                        ON vfio_rel.FIO_OBJECTS_ID=vdh_rel.OBJECTS_ID AND vfio_rel.FIO_CREATED<cr_rel.CREATED_DATE+1
                      INNER JOIN AFS.VIEW_BIRTH vb_rel
                        ON vb_rel.OBJECTS_ID = vdh_rel.OBJECTS_ID AND vb_rel.BIRTH_CREATED<cr_rel.CREATED_DATE+1
                      WHERE cr.REQUEST_ID = v_Request_ID
                        AND (vfio.FIO4SEARCH^=vfio_rel.FIO4SEARCH OR vb.BIRTH^=vb_rel.BIRTH)
                        AND AFS.FN_DIST_LEV(vfio.FIO4SEARCH||vb.BIRTH, vfio_rel.FIO4SEARCH||vb_rel.BIRTH)=1
                        --AND (AFS.FN_DIST_LEV(vfio.FIO4SEARCH, vfio_rel.FIO4SEARCH)=1 OR AFS.FN_DIST_LEV(vb.BIRTH, vb_rel.BIRTH)=1)
                     ) WHERE RANK=1 AND RANK_REL=1
                    --оставляем только заявки не попавшие под общий фильтр
                    AND AFS.afs_filter.check_request_id_rel(REQUEST_ID_REL)=0) 
        LOOP
          v_OutNumber := ic.FRAUD_OUTNUMBER;
          v_OutChar := TO_CHAR(v_OutNumber);
		  
          afs_sys.write_afs_history(v_PROC_ID => p_PROC_ID_NEW
              , v_REQUEST_ID 			=> v_Request_ID
              , v_PERSON_ID 			=> ic.PERSON_ID
              , v_REQUEST_DATE 		=> ic.REQUEST_DATE
              , v_FRAUD_ID 				=> p_FRAUD_ID
              , v_FRAUD_OUTNUMBER => v_OutNumber
              , v_FIO 					  => ic.FIO 
              , v_DR 					    => ic.DR
              , v_DAY_BETWEEN 		=> ic.DAY_BETWEEN
              , v_REQUEST_ID_REL 		=> ic.REQUEST_ID_REL
              , v_PERSON_ID_REL 		=> ic.PERSON_ID_REL
              , v_REQUEST_DATE_REL 	=> ic.REQUEST_DATE_REL
              , v_FIO_REL 				  => ic.FIO_REL 
              , v_DR_REL 				    => ic.DR_REL
              , v_INFO_EQUAL 			   => ic.INFO_EQUAL
              , v_INFO_NOT_EQUAL 		 => ic.INFO_NOT_EQUAL
              , v_INFO_NOT_EQUAL_REL => ic.INFO_NOT_EQUAL_REL
              , v_E_CODE => v_ErrorCode, v_E_TEXT => v_ErrorText);
        END LOOP;
      WHEN p_FlagEx=0 THEN --если заявки нет присваиваем статус
        v_OutNumber := 9;
        v_OutChar := TO_CHAR(v_OutNumber);
      END CASE;
      
      --если фрод не нашелся возвращаем 0
      IF (v_OutNumber IS NULL) THEN 
        v_OutNumber := 0;
        v_OutChar := TO_CHAR(v_OutNumber);
      END IF;
    
      p_ErrorCode_Sys := SQLCODE;
      p_ErrorText_Sys := SUBSTR(SQLCODE||' | '||SQLERRM,0,4000);
    --пишем результат в лог вызова фрод-правила полученные данные
    afs_sys.write_log_call(v_PROC_ID => p_PROC_ID_NEW, v_FRAUD_ID => p_FRAUD_ID, v_FRAUD_NAME => p_FRAUD_NM
          , v_REQUEST_ID => v_Request_ID, v_PERSON_ID => v_Person_ID
          , v_OUTCODE => v_OutCode, v_INARRAY => v_InArray
          , v_OUTNUMBER => v_OutNumber, v_OUTCHAR => v_OutChar, v_OUTARRAY => v_OutArray
          , v_E_CODE => p_ErrorCode_Sys, v_E_TEXT => p_ErrorText_Sys);
    v_ErrorCode := p_ErrorCode_Sys;
    v_ErrorText := p_ErrorText_Sys;
	COMMIT;

    EXCEPTION WHEN OTHERS
      THEN 
        v_OutNumber := -1;
        v_OutChar := TO_CHAR(v_OutNumber);
        --присваиваем ошибки для системного мониторинга и для возврата
        p_ErrorCode_Sys := SQLCODE;
        p_ErrorText_Sys := v_ErrorText;
        v_ErrorCode := p_ErrorCode_Sys;
        v_ErrorText := p_ErrorText_Sys;
        --пишем в лог вызова
        afs_sys.write_log_call(v_PROC_ID => p_PROC_ID_NEW, v_FRAUD_ID => p_FRAUD_ID, v_FRAUD_NAME => p_FRAUD_NM
              , v_REQUEST_ID => v_Request_ID, v_PERSON_ID => v_Person_ID
              , v_OUTCODE => v_OutCode, v_INARRAY => v_InArray
              , v_OUTNUMBER => v_OutNumber, v_OUTCHAR => v_OutChar, v_OUTARRAY => v_OutArray
              , v_E_CODE => p_ErrorCode_Sys, v_E_TEXT => p_ErrorText_Sys);
        --если нужно вернуть ошибку при записи в лог в выходных параметрах, то присваиваем параметрам
        /*v_ErrorCode := p_ErrorCode_Sys;
        v_ErrorText := p_ErrorText_Sys;*/
  END FRAUD_RULE_0002;
  
  PROCEDURE FRAUD_RULE_0003(v_Request_ID IN NUMBER, v_Person_ID IN NUMBER, v_OutCode IN VARCHAR2, v_InArray IN VARCHAR2
                          ,v_OutNumber OUT NUMBER, v_OutChar IN OUT VARCHAR2
                          ,v_OutArray OUT VARCHAR2
                          ,v_ErrorCode OUT NUMBER, v_ErrorText OUT VARCHAR2
                        ) AS
      p_PROC_ID_NEW   NUMBER;        --уникальный ID процесса. Генерируется при запуске фрод правила
      p_FRAUD_ID			NUMBER := 3;   --ID правила. Определяется по справочнику D_FRAUD_RULE
      p_FRAUD_NM			VARCHAR2(255) := 'AFS.AFS_RULES.FRAUD_RULE_0003';   --ID правила. Определяется по справочнику D_FRAUD_RULE
      p_FlagEx 				NUMBER;        --Флаг наличия заявки в ПКК
      
      p_ErrorCode_Sys NUMBER;          --Системный код ошибки. В выходной параметр может не передаваться если не существенен
      p_ErrorText_Sys VARCHAR2(4000);  --Системный текст ошибки. В выходной параметр может не передаваться если не существенен
	  
      pragma autonomous_transaction;
  BEGIN
      --определяем новый уникальный ID процесса
      SELECT AFS.AFS_PROC_SEQ.NEXTVAL INTO p_PROC_ID_NEW FROM DUAL;
  
      v_OutNumber := NULL;
      v_OutChar := '';
      v_OutArray := '';
      p_ErrorCode_Sys := SQLCODE;
      p_ErrorText_Sys := SUBSTR(SQLCODE||' | '||SQLERRM,0,4000);
      
      --пишем в лог вызова фрод-правил
      afs_sys.write_log_call(v_PROC_ID => p_PROC_ID_NEW, v_FRAUD_ID => p_FRAUD_ID, v_FRAUD_NAME => p_FRAUD_NM
            , v_REQUEST_ID => v_Request_ID, v_PERSON_ID => v_Person_ID
            , v_OUTCODE => v_OutCode, v_INARRAY => v_InArray
            , v_OUTNUMBER => v_OutNumber, v_OUTCHAR => v_OutChar, v_OUTARRAY => v_OutArray
            , v_E_CODE => p_ErrorCode_Sys, v_E_TEXT => p_ErrorText_Sys);
      
      --проверяем существует ли заявка
      SELECT COUNT(*) INTO p_FlagEx FROM DUAL WHERE EXISTS(SELECT 1 FROM KREDIT.C_REQUEST WHERE REQUEST_ID=v_Request_ID
        AND AFS.afs_filter.check_request_id_main(v_Request_ID)=0);
      
      CASE WHEN p_FlagEx=1 THEN 
        FOR ic IN (SELECT * FROM (     
              SELECT DISTINCT v_src.REQUEST_ID, v_src.OBJECTS_ID AS PERSON_ID, v_src.CREATED_DATE AS REQUEST_DATE
                  , v_rel.REQUEST_ID AS REQUEST_ID_REL, v_rel.OBJECTS_ID AS PERSON_ID_REL
                  , v_rel.CREATED_DATE AS REQUEST_DATE_REL
                  , ABS(ROUND(v_src.CREATED_DATE-v_rel.CREATED_DATE, 2)) as DAY_BETWEEN
                  , 'Тел.моб:'||v_src.PHONE_MOB as INFO_EQUAL
                  ,'Фио+Др.:'||v_src.FIO||' '||v_src.BIRTH as INFO_NOT_EQUAL
                  ,DENSE_RANK() OVER (PARTITION BY v_src.OBJECTS_ID 
                              ORDER BY v_src.fio_akt DESC,  v_src.FIO_CREATED DESC, v_src.FIO_MOD DESC
                                ,v_src.BIRTH_AKT DESC, v_src.BIRTH_CREATED DESC, v_src.BIRTH_MOD DESC) as RANK
                  ,'Фио+Др.:'||v_rel.FIO||' '||v_rel.BIRTH as INFO_NOT_EQUAL_REL
                  ,DENSE_RANK() OVER (PARTITION BY v_rel.OBJECTS_ID 
                              ORDER BY v_rel.fio_akt DESC,  v_rel.FIO_CREATED DESC, v_rel.FIO_MOD DESC
                                ,v_rel.BIRTH_AKT DESC, v_rel.BIRTH_CREATED DESC, v_rel.BIRTH_MOD DESC) as RANK_REL
                  ,v_src.FIO AS FIO, v_src.BIRTH AS DR
                  ,v_rel.FIO AS FIO_REL, v_rel.BIRTH AS DR_REL
                  ,1 AS FRAUD_OUTNUMBER
                FROM AFS.VR_MOB_FB v_src
                INNER JOIN AFS.VR_MOB_FB v_rel
                  ON v_src.PHONE_MOB = v_rel.PHONE_MOB
                    AND v_src.OBJECTS_TYPE = v_rel.OBJECTS_TYPE
                    AND v_src.OBJECTS_ID ^= v_rel.OBJECTS_ID
                  AND v_src.REQUEST_ID > v_rel.REQUEST_ID
                WHERE v_src.REQUEST_ID = v_Request_ID AND v_src.OBJECTS_TYPE=2
                  AND (v_src.FIO^=v_rel.FIO OR v_src.BIRTH^=v_rel.BIRTH)
                  AND v_rel.CREATED_DATE BETWEEN v_src.CREATED_DATE-91*1 AND v_src.CREATED_DATE
                  AND NOT v_src.PHONE_MOB LIKE '%000000%' AND NOT v_src.PHONE_MOB LIKE '%999999%'
               ) WHERE RANK=1 AND RANK_REL=1 
              --оставляем только заявки не попавшие под общий фильтр
              AND AFS.afs_filter.check_request_id_rel(REQUEST_ID_REL)=0
              --исключаем девичьи фамилии
              AND NOT (SUBSTR(FIO, INSTR(FIO,' '), 254 )=SUBSTR(FIO_REL, INSTR(FIO_REL,' '), 254 ) AND DR=DR_REL )
              -- исключение однофамильцев 
              AND NOT UTL_MATCH.EDIT_DISTANCE(SUBSTR(FIO, 1, INSTR(FIO,' ')-1 ), SUBSTR(FIO_REL, 1, INSTR(FIO_REL,' ')-1 ))<3
               --PostCheck: Модифицированная проверка на родственников
              AND NOT AFS.FN_IS_FAMILY_REL(PERSON_ID, PERSON_ID_REL)=1
              AND NOT AFS.FN_IS_FAMILY_REL(PERSON_ID_REL, PERSON_ID)=1
              AND NOT AFS.FN_IS_FAMILY_CONT(PERSON_ID, PERSON_ID_REL)=1 
              AND NOT AFS.FN_IS_FAMILY_CONT(PERSON_ID_REL, PERSON_ID)=1
              AND NOT EXISTS(SELECT 1 FROM CPD.PHONES 
                                WHERE (OBJECTS_ID=PERSON_ID OR OBJECTS_ID=PERSON_ID_REL ) AND OBJECTS_TYPE=2 
                                    AND PHONE=SUBSTR(INFO_EQUAL, 9, 10)
                                    AND PHONES_COMM = 'Телефон из РБО: Мобильный') )
        LOOP
          v_OutNumber := ic.FRAUD_OUTNUMBER;
          v_OutChar := TO_CHAR(v_OutNumber);
		  
          afs_sys.write_afs_history(v_PROC_ID => p_PROC_ID_NEW
              , v_REQUEST_ID 			=> v_Request_ID
              , v_PERSON_ID 			=> ic.PERSON_ID
              , v_REQUEST_DATE 		=> ic.REQUEST_DATE
              , v_FRAUD_ID 				=> p_FRAUD_ID
              , v_FRAUD_OUTNUMBER => v_OutNumber
              , v_FIO 					  => ic.FIO 
              , v_DR 					    => ic.DR
              , v_DAY_BETWEEN 		=> ic.DAY_BETWEEN
              , v_REQUEST_ID_REL 		=> ic.REQUEST_ID_REL
              , v_PERSON_ID_REL 		=> ic.PERSON_ID_REL
              , v_REQUEST_DATE_REL 	=> ic.REQUEST_DATE_REL
              , v_FIO_REL 				  => ic.FIO_REL 
              , v_DR_REL 				    => ic.DR_REL
              , v_INFO_EQUAL 			   => ic.INFO_EQUAL
              , v_INFO_NOT_EQUAL 		 => ic.INFO_NOT_EQUAL
              , v_INFO_NOT_EQUAL_REL => ic.INFO_NOT_EQUAL_REL
              , v_E_CODE => v_ErrorCode, v_E_TEXT => v_ErrorText);
        END LOOP;
      WHEN p_FlagEx=0 THEN --если заявки нет присваиваем статус
        v_OutNumber := 9;
        v_OutChar := TO_CHAR(v_OutNumber);
      END CASE;
      
      --если фрод не нашелся возвращаем 0
      IF (v_OutNumber IS NULL) THEN 
        v_OutNumber := 0;
        v_OutChar := TO_CHAR(v_OutNumber);
      END IF;
    
      p_ErrorCode_Sys := SQLCODE;
      p_ErrorText_Sys := SUBSTR(SQLCODE||' | '||SQLERRM,0,4000);
    --пишем результат в лог вызова фрод-правила полученные данные
    afs_sys.write_log_call(v_PROC_ID => p_PROC_ID_NEW, v_FRAUD_ID => p_FRAUD_ID, v_FRAUD_NAME => p_FRAUD_NM
          , v_REQUEST_ID => v_Request_ID, v_PERSON_ID => v_Person_ID
          , v_OUTCODE => v_OutCode, v_INARRAY => v_InArray
          , v_OUTNUMBER => v_OutNumber, v_OUTCHAR => v_OutChar, v_OUTARRAY => v_OutArray
          , v_E_CODE => p_ErrorCode_Sys, v_E_TEXT => p_ErrorText_Sys);
    v_ErrorCode := p_ErrorCode_Sys;
    v_ErrorText := p_ErrorText_Sys;
	COMMIT;

    EXCEPTION WHEN OTHERS
      THEN 
        v_OutNumber := -1;
        v_OutChar := TO_CHAR(v_OutNumber);
        --присваиваем ошибки для системного мониторинга и для возврата
        p_ErrorCode_Sys := SQLCODE;
        p_ErrorText_Sys := v_ErrorText;
        v_ErrorCode := p_ErrorCode_Sys;
        v_ErrorText := p_ErrorText_Sys;
        --пишем в лог вызова
        afs_sys.write_log_call(v_PROC_ID => p_PROC_ID_NEW, v_FRAUD_ID => p_FRAUD_ID, v_FRAUD_NAME => p_FRAUD_NM
              , v_REQUEST_ID => v_Request_ID, v_PERSON_ID => v_Person_ID
              , v_OUTCODE => v_OutCode, v_INARRAY => v_InArray
              , v_OUTNUMBER => v_OutNumber, v_OUTCHAR => v_OutChar, v_OUTARRAY => v_OutArray
              , v_E_CODE => p_ErrorCode_Sys, v_E_TEXT => p_ErrorText_Sys);
        --если нужно вернуть ошибку при записи в лог в выходных параметрах, то присваиваем параметрам
        /*v_ErrorCode := p_ErrorCode_Sys;
        v_ErrorText := p_ErrorText_Sys;*/
  END FRAUD_RULE_0003;

  PROCEDURE FRAUD_RULE_0004(v_Request_ID IN NUMBER, v_Person_ID IN NUMBER, v_OutCode IN VARCHAR2, v_InArray IN VARCHAR2
                          ,v_OutNumber OUT NUMBER, v_OutChar IN OUT VARCHAR2
                          ,v_OutArray OUT VARCHAR2
                          ,v_ErrorCode OUT NUMBER, v_ErrorText OUT VARCHAR2
                        ) AS
  BEGIN
    /* TODO implementation required */
    NULL;
  END FRAUD_RULE_0004;

  PROCEDURE FRAUD_RULE_0005(v_Request_ID IN NUMBER, v_Person_ID IN NUMBER, v_OutCode IN VARCHAR2, v_InArray IN VARCHAR2
                          ,v_OutNumber OUT NUMBER, v_OutChar IN OUT VARCHAR2
                          ,v_OutArray OUT VARCHAR2
                          ,v_ErrorCode OUT NUMBER, v_ErrorText OUT VARCHAR2
                        ) AS
  BEGIN
    /* TODO implementation required */
    NULL;
  END FRAUD_RULE_0005;

END AFS_RULES;

/

  GRANT EXECUTE ON "AFS"."AFS_RULES" TO "L_KREDIT_SCORING";
--------------------------------------------------------
--  DDL for Package Body AFS_RULES_PASP
--------------------------------------------------------

  CREATE OR REPLACE PACKAGE BODY "AFS"."AFS_RULES_PASP" AS

 PROCEDURE FRAUD_0001(v_PROC_ID IN OUT NUMBER 
                          ,v_Request_ID IN NUMBER, v_Person_ID IN NUMBER, v_OutCode IN VARCHAR2, v_InArray IN VARCHAR2
                          ,v_OutNumber OUT NUMBER, v_OutChar OUT VARCHAR2
                          ,v_OutArray OUT VARCHAR2
                          ,v_ErrorCode OUT NUMBER, v_ErrorText OUT VARCHAR2
                        ) AS
      --Правило 1 -	Разные ФиоДр - Паспорт равен
      p_FRAUD_ID			NUMBER := 1;   --ID правила. Определяется по справочнику D_FRAUD_RULE
      p_FRAUD_NM			VARCHAR2(255) := 'AFS.AFS_RULES_PASP.FRAUD_0001';   --ID правила. Определяется по справочнику D_FRAUD_RULE
      p_FlagEx 				NUMBER;        --Флаг наличия заявки в ПКК

      pragma autonomous_transaction; 
  BEGIN
      --определяем новый уникальный ID процесса
      IF v_PROC_ID IS NULL OR v_PROC_ID<0 THEN 
        SELECT AFS.AFS_PROC_SEQ.NEXTVAL INTO v_PROC_ID FROM DUAL;
      END IF;   
  
      v_OutNumber := NULL;
      v_OutChar := '';
      v_OutArray := '';
      v_ErrorCode := 0;
      v_ErrorText := ''; 
      
      --проверяем существует ли заявка
      SELECT COUNT(*) INTO p_FlagEx FROM DUAL WHERE EXISTS(SELECT 1 FROM KREDIT.C_REQUEST WHERE REQUEST_ID=v_Request_ID
        AND AFS.afs_filter.check_request_id_main(v_Request_ID)=0);
       
      CASE WHEN p_FlagEx=1 THEN 
        --логика определения текущего фрод правила
        FOR ic IN (SELECT * FROM (      
          SELECT DISTINCT v_src.REQUEST_ID, v_src.OBJECTS_ID AS PERSON_ID, v_src.CREATED_DATE AS REQUEST_DATE
              , v_rel.REQUEST_ID AS REQUEST_ID_REL, v_rel.OBJECTS_ID AS PERSON_ID_REL
              , v_rel.CREATED_DATE AS REQUEST_DATE_REL
              , ABS(ROUND(v_src.CREATED_DATE-v_rel.CREATED_DATE, 2)) as DAY_BETWEEN
              , 'Паспорт:'||v_src.DOCUMENTS_SERIAL||v_src.DOCUMENTS_NUMBER as INFO_EQUAL
              ,'Фио+Др.:'||v_src.FIO||' '||v_src.BIRTH as INFO_NOT_EQUAL
              ,DENSE_RANK() OVER (PARTITION BY v_src.OBJECTS_ID 
                          ORDER BY v_src.fio_akt DESC,  v_src.FIO_CREATED DESC, v_src.FIO_MOD DESC
                            ,v_src.BIRTH_AKT DESC, v_src.BIRTH_CREATED DESC, v_src.BIRTH_MOD DESC) as RANK
              ,'Фио+Др.:'||v_rel.FIO||' '||v_rel.BIRTH as INFO_NOT_EQUAL_REL
              ,DENSE_RANK() OVER (PARTITION BY v_rel.OBJECTS_ID 
                          ORDER BY v_rel.fio_akt DESC,  v_rel.FIO_CREATED DESC, v_rel.FIO_MOD DESC
                            ,v_rel.BIRTH_AKT DESC, v_rel.BIRTH_CREATED DESC, v_rel.BIRTH_MOD DESC) as RANK_REL
              ,v_src.FIO AS FIO, v_src.BIRTH AS DR
              ,v_rel.FIO AS FIO_REL, v_rel.BIRTH AS DR_REL
              ,1 AS FRAUD_OUTNUMBER
            FROM AFS.VR_DOC_FB v_src
            INNER JOIN AFS.VR_DOC_FB v_rel
              ON v_src.DOCUMENTS_SERIAL = v_rel.DOCUMENTS_SERIAL
                AND v_src.DOCUMENTS_NUMBER = v_rel.DOCUMENTS_NUMBER
                AND v_src.DOCUMENTS_TYPE = v_rel.DOCUMENTS_TYPE
                AND v_src.OBJECTS_ID ^= v_rel.OBJECTS_ID
              AND v_src.REQUEST_ID > v_rel.REQUEST_ID
            WHERE v_src.REQUEST_ID = v_Request_ID AND v_src.DOCUMENTS_TYPE=21
              AND (v_src.FIO^=v_rel.FIO OR v_src.BIRTH^=v_rel.BIRTH)
              AND AFS.FN_DIST_LEV(v_src.FIO||v_src.BIRTH, v_rel.FIO||v_rel.BIRTH)>1
              --AND (AFS.FN_DIST_LEV(v_src.FIO, v_rel.FIO)>1 OR AFS.FN_DIST_LEV(v_src.BIRTH, v_rel.BIRTH)>1)
              --AND UTL_MATCH.EDIT_DISTANCE(v_src.FIO||v_src.BIRTH, v_rel.FIO||v_rel.BIRTH)=1
           ) WHERE RANK=1 AND RANK_REL=1 
              --оставляем только заявки не попавшие под общий фильтр
              AND AFS.afs_filter.check_request_id_rel(REQUEST_ID_REL)=0) 
        LOOP
          v_OutNumber := ic.FRAUD_OUTNUMBER;
          v_OutChar := TO_CHAR(v_OutNumber);
		  
          afs_sys.write_afs_history(v_PROC_ID => v_PROC_ID
              , v_REQUEST_ID 			=> v_Request_ID
              , v_PERSON_ID 			=> ic.PERSON_ID
              , v_REQUEST_DATE 		=> ic.REQUEST_DATE
              , v_FRAUD_ID 				=> p_FRAUD_ID
              , v_FRAUD_OUTNUMBER => v_OutNumber
              , v_FIO 					  => ic.FIO 
              , v_DR 					    => ic.DR
              , v_DAY_BETWEEN 		=> ic.DAY_BETWEEN
              , v_REQUEST_ID_REL 		=> ic.REQUEST_ID_REL
              , v_PERSON_ID_REL 		=> ic.PERSON_ID_REL
              , v_REQUEST_DATE_REL 	=> ic.REQUEST_DATE_REL
              , v_FIO_REL 				  => ic.FIO_REL 
              , v_DR_REL 				    => ic.DR_REL
              , v_INFO_EQUAL 			   => ic.INFO_EQUAL
              , v_INFO_NOT_EQUAL 		 => ic.INFO_NOT_EQUAL
              , v_INFO_NOT_EQUAL_REL => ic.INFO_NOT_EQUAL_REL
              , v_E_CODE => v_ErrorCode, v_E_TEXT => v_ErrorText);
        END LOOP;
      WHEN p_FlagEx=0 THEN --если заявки нет присваиваем статус
        v_OutNumber := 9;
        v_OutChar := TO_CHAR(v_OutNumber);
      END CASE;
      
      --если фрод не нашелся возвращаем 0
      IF (v_OutNumber IS NULL) THEN 
        v_OutNumber := 0;
        v_OutChar := TO_CHAR(v_OutNumber);
      END IF;

      COMMIT;

    EXCEPTION WHEN OTHERS
      THEN 
        v_OutNumber := -1;
        v_OutChar := TO_CHAR(v_OutNumber);
        --присваиваем ошибки для системного мониторинга
        v_ErrorCode := 10011;
        v_ErrorText := SUBSTR(TO_CHAR(SQLCODE)||' | '||SQLERRM,0,4000);
  END FRAUD_0001;

  PROCEDURE FRAUD_0002(v_PROC_ID IN OUT NUMBER 
                          ,v_Request_ID IN NUMBER, v_Person_ID IN NUMBER, v_OutCode IN VARCHAR2, v_InArray IN VARCHAR2
                          ,v_OutNumber OUT NUMBER, v_OutChar OUT VARCHAR2
                          ,v_OutArray OUT VARCHAR2
                          ,v_ErrorCode OUT NUMBER, v_ErrorText OUT VARCHAR2
                        ) AS
      --Правило 2 -	Ошибка ФиоДр - Паспорт равен
      p_FRAUD_ID			NUMBER := 2;   --ID правила. Определяется по справочнику D_FRAUD_RULE
      p_FRAUD_NM			VARCHAR2(255) := 'AFS.AFS_RULES_PASP.FRAUD_0002';   --ID правила. Определяется по справочнику D_FRAUD_RULE
      p_FlagEx 				NUMBER;        --Флаг наличия заявки в ПКК

      pragma autonomous_transaction; 
  BEGIN
      --определяем новый уникальный ID процесса
      IF v_PROC_ID IS NULL OR v_PROC_ID<0 THEN 
        SELECT AFS.AFS_PROC_SEQ.NEXTVAL INTO v_PROC_ID FROM DUAL;
      END IF; 
  
      v_OutNumber := NULL;
      v_OutChar := '';
      v_OutArray := '';
      v_ErrorCode := 0;
      v_ErrorText := '';
      
      --проверяем существует ли заявка
      SELECT COUNT(*) INTO p_FlagEx FROM DUAL WHERE EXISTS(SELECT 1 FROM KREDIT.C_REQUEST WHERE REQUEST_ID=v_Request_ID
        AND AFS.afs_filter.check_request_id_main(v_Request_ID)=0);
      
      CASE WHEN p_FlagEx=1 THEN
        --логика определения текущего фрод правила
        FOR ic IN ( SELECT * FROM (     
                SELECT DISTINCT cr.REQUEST_ID, cr.OBJECTS_ID AS PERSON_ID, cr.CREATED_DATE AS REQUEST_DATE
                    , cr_rel.REQUEST_ID AS REQUEST_ID_REL, cr_rel.OBJECTS_ID AS PERSON_ID_REL
                    , cr_rel.CREATED_DATE AS REQUEST_DATE_REL
                    , ABS(ROUND(cr.CREATED_DATE-cr_rel.CREATED_DATE, 2)) as DAY_BETWEEN
                    , 'Паспорт:'||vdh_src.DOCUMENTS_SERIAL||vdh_src.DOCUMENTS_NUMBER as INFO_EQUAL
                    ,'Фио+Др.:'||vfio.FIO4SEARCH||' '||vb.BIRTH as INFO_NOT_EQUAL
                    ,DENSE_RANK() OVER (PARTITION BY vfio.FIO_OBJECTS_ID 
                                ORDER BY vfio.fio_akt DESC,  vfio.FIO_CREATED DESC, vfio.MODIFICATION_DATE DESC
                                  ,vb.BIRTH_AKT DESC, vb.BIRTH_CREATED DESC, vb.MODIFICATION_DATE DESC) as RANK
                    ,'Фио+Др.:'||vfio_rel.FIO4SEARCH||' '||vb_rel.BIRTH as INFO_NOT_EQUAL_REL
                    ,DENSE_RANK() OVER (PARTITION BY vfio_rel.FIO_OBJECTS_ID 
                                ORDER BY vfio_rel.fio_akt DESC,  vfio_rel.FIO_CREATED DESC, vfio_rel.MODIFICATION_DATE DESC
                                  ,vb_rel.BIRTH_AKT DESC, vb_rel.BIRTH_CREATED DESC, vb_rel.MODIFICATION_DATE DESC) as RANK_REL
                    ,vfio.FIO4SEARCH AS FIO, vb.BIRTH AS DR
                    ,vfio_rel.FIO4SEARCH AS FIO_REL, vb_rel.BIRTH AS DR_REL
                    ,1 AS FRAUD_OUTNUMBER
                  FROM KREDIT.C_REQUEST cr
                  INNER JOIN KREDIT.VIEW_DOCUMENTS_HISTORY vdh_src
                    ON cr.OBJECTS_ID = vdh_src.OBJECTS_ID AND vdh_src.DOCUMENTS_TYPE=21
                  INNER JOIN KREDIT.VIEW_DOCUMENTS_HISTORY vdh_rel
                    ON vdh_src.DOCUMENTS_SERIAL = vdh_rel.DOCUMENTS_SERIAL
                      AND vdh_src.DOCUMENTS_NUMBER = vdh_rel.DOCUMENTS_NUMBER
                      AND vdh_rel.DOCUMENTS_TYPE=vdh_src.DOCUMENTS_TYPE
                      AND vdh_src.OBJECTS_ID ^= vdh_rel.OBJECTS_ID
                  INNER JOIN KREDIT.C_REQUEST cr_rel
                    ON vdh_rel.OBJECTS_ID = cr_rel.OBJECTS_ID
                      AND cr.REQUEST_ID > cr_rel.REQUEST_ID
                  INNER JOIN AFS.VIEW_FIO_HISTORY vfio
                    ON vfio.FIO_OBJECTS_ID = vdh_src.OBJECTS_ID AND vfio.FIO_CREATED < cr.CREATED_DATE+1
                  INNER JOIN AFS.VIEW_BIRTH vb
                    ON vb.OBJECTS_ID = vdh_src.OBJECTS_ID AND vb.BIRTH_CREATED < cr.CREATED_DATE+1
                  INNER JOIN AFS.VIEW_FIO_HISTORY vfio_rel
                    ON vfio_rel.FIO_OBJECTS_ID=vdh_rel.OBJECTS_ID AND vfio_rel.FIO_CREATED<cr_rel.CREATED_DATE+1
                  INNER JOIN AFS.VIEW_BIRTH vb_rel
                    ON vb_rel.OBJECTS_ID = vdh_rel.OBJECTS_ID AND vb_rel.BIRTH_CREATED<cr_rel.CREATED_DATE+1
                  WHERE cr.REQUEST_ID = v_Request_ID
                    AND (vfio.FIO4SEARCH^=vfio_rel.FIO4SEARCH OR vb.BIRTH^=vb_rel.BIRTH)
                    AND AFS.FN_DIST_LEV(vfio.FIO4SEARCH||vb.BIRTH, vfio_rel.FIO4SEARCH||vb_rel.BIRTH)=1
                    --AND (AFS.FN_DIST_LEV(vfio.FIO4SEARCH, vfio_rel.FIO4SEARCH)=1 OR AFS.FN_DIST_LEV(vb.BIRTH, vb_rel.BIRTH)=1)
                 ) WHERE RANK=1 AND RANK_REL=1
              --оставляем только заявки не попавшие под общий фильтр
              AND AFS.afs_filter.check_request_id_rel(REQUEST_ID_REL)=0
            )
        LOOP
          v_OutNumber := ic.FRAUD_OUTNUMBER;
          v_OutChar := TO_CHAR(v_OutNumber);
		  
          afs_sys.write_afs_history(v_PROC_ID => v_PROC_ID
              , v_REQUEST_ID 			=> v_Request_ID
              , v_PERSON_ID 			=> ic.PERSON_ID
              , v_REQUEST_DATE 		=> ic.REQUEST_DATE
              , v_FRAUD_ID 				=> p_FRAUD_ID
              , v_FRAUD_OUTNUMBER => v_OutNumber
              , v_FIO 					  => ic.FIO 
              , v_DR 					    => ic.DR
              , v_DAY_BETWEEN 		=> ic.DAY_BETWEEN
              , v_REQUEST_ID_REL 		=> ic.REQUEST_ID_REL
              , v_PERSON_ID_REL 		=> ic.PERSON_ID_REL
              , v_REQUEST_DATE_REL 	=> ic.REQUEST_DATE_REL
              , v_FIO_REL 				  => ic.FIO_REL 
              , v_DR_REL 				    => ic.DR_REL
              , v_INFO_EQUAL 			   => ic.INFO_EQUAL
              , v_INFO_NOT_EQUAL 		 => ic.INFO_NOT_EQUAL
              , v_INFO_NOT_EQUAL_REL => ic.INFO_NOT_EQUAL_REL
              , v_E_CODE => v_ErrorCode, v_E_TEXT => v_ErrorText);
        END LOOP;
      WHEN p_FlagEx=0 THEN --если заявки нет присваиваем статус
        v_OutNumber := 9;
        v_OutChar := TO_CHAR(v_OutNumber);
      END CASE;
      
      --если фрод не нашелся возвращаем 0
      IF (v_OutNumber IS NULL) THEN 
        v_OutNumber := 0;
        v_OutChar := TO_CHAR(v_OutNumber);
      END IF;
      
    COMMIT;

    EXCEPTION WHEN OTHERS
      THEN 
        v_OutNumber := -1;
        v_OutChar := TO_CHAR(v_OutNumber);
        --присваиваем ошибки для системного мониторинга
        v_ErrorCode := 10021;
        v_ErrorText := SUBSTR(TO_CHAR(SQLCODE)||' | '||SQLERRM,0,4000);
  END FRAUD_0002;

  PROCEDURE FRAUD_0003(v_PROC_ID IN OUT NUMBER 
                          ,v_Request_ID IN NUMBER, v_Person_ID IN NUMBER, v_OutCode IN VARCHAR2, v_InArray IN VARCHAR2
                          ,v_OutNumber OUT NUMBER, v_OutChar OUT VARCHAR2
                          ,v_OutArray OUT VARCHAR2
                          ,v_ErrorCode OUT NUMBER, v_ErrorText OUT VARCHAR2
                        ) AS
  BEGIN
    /* TODO implementation required */
    NULL;
  END FRAUD_0003;

  PROCEDURE FRAUD_0004(v_PROC_ID IN OUT NUMBER 
                          ,v_Request_ID IN NUMBER, v_Person_ID IN NUMBER, v_OutCode IN VARCHAR2, v_InArray IN VARCHAR2
                          ,v_OutNumber OUT NUMBER, v_OutChar OUT VARCHAR2
                          ,v_OutArray OUT VARCHAR2
                          ,v_ErrorCode OUT NUMBER, v_ErrorText OUT VARCHAR2
                        ) AS
  BEGIN
    /* TODO implementation required */
    NULL;
  END FRAUD_0004;
  
  PROCEDURE FRAUD_0005(v_PROC_ID IN OUT NUMBER 
                          ,v_Request_ID IN NUMBER, v_Person_ID IN NUMBER, v_OutCode IN VARCHAR2, v_InArray IN VARCHAR2
                          ,v_OutNumber OUT NUMBER, v_OutChar OUT VARCHAR2
                          ,v_OutArray OUT VARCHAR2
                          ,v_ErrorCode OUT NUMBER, v_ErrorText OUT VARCHAR2
                        ) AS
  BEGIN
    /* TODO implementation required */
    NULL;
  END FRAUD_0005;


END AFS_RULES_PASP;

/

  GRANT EXECUTE ON "AFS"."AFS_RULES_PASP" TO "MVKARELIN";
--------------------------------------------------------
--  DDL for Package Body AFS_RULES_PHONE
--------------------------------------------------------

  CREATE OR REPLACE PACKAGE BODY "AFS"."AFS_RULES_PHONE" AS

  PROCEDURE FRAUD_0001(v_PROC_ID IN OUT NUMBER 
                          ,v_Request_ID IN NUMBER, v_Person_ID IN NUMBER, v_OutCode IN VARCHAR2, v_InArray IN VARCHAR2
                          ,v_OutNumber OUT NUMBER, v_OutChar OUT VARCHAR2 
                          ,v_OutArray OUT VARCHAR2
                          ,v_ErrorCode OUT NUMBER, v_ErrorText OUT VARCHAR2
                        ) AS
      --Правило 3 -	Телефон1-Псп0
      p_FRAUD_ID			NUMBER := 3;   --ID правила. Определяется по справочнику D_FRAUD_RULE
      p_FRAUD_NM			VARCHAR2(255) := 'AFS.AFS_RULES_PHONE.FRAUD_0001';   --ID правила. Определяется по справочнику D_FRAUD_RULE
      p_FlagEx 				NUMBER;        --Флаг наличия заявки в ПКК

      pragma autonomous_transaction; 
  BEGIN
      --определяем новый уникальный ID процесса
      IF v_PROC_ID IS NULL OR v_PROC_ID<0 THEN 
        SELECT AFS.AFS_PROC_SEQ.NEXTVAL INTO v_PROC_ID FROM DUAL;
      END IF; 
  
      v_OutNumber := NULL;
      v_OutChar := '';
      v_OutArray := '';
      v_ErrorCode := 0;
      v_ErrorText := ''; 
      
      --проверяем существует ли заявка
      SELECT COUNT(*) INTO p_FlagEx FROM DUAL WHERE EXISTS(SELECT 1 FROM KREDIT.C_REQUEST WHERE REQUEST_ID=v_Request_ID
        AND AFS.afs_filter.check_request_id_main(v_Request_ID)=0);
      
      CASE WHEN p_FlagEx=1 THEN 
        --логика определения текущего фрод правила
        FOR ic IN (SELECT * FROM (     
              SELECT DISTINCT v_src.REQUEST_ID, v_src.OBJECTS_ID AS PERSON_ID, v_src.CREATED_DATE AS REQUEST_DATE
                  , v_rel.REQUEST_ID AS REQUEST_ID_REL, v_rel.OBJECTS_ID AS PERSON_ID_REL
                  , v_rel.CREATED_DATE AS REQUEST_DATE_REL
                  , ABS(ROUND(v_src.CREATED_DATE-v_rel.CREATED_DATE, 2)) as DAY_BETWEEN
                  , 'Тел.моб:'||v_src.PHONE_MOB as INFO_EQUAL
                  ,'Фио+Др.:'||v_src.FIO||' '||v_src.BIRTH as INFO_NOT_EQUAL
                  ,DENSE_RANK() OVER (PARTITION BY v_src.OBJECTS_ID 
                              ORDER BY v_src.fio_akt DESC,  v_src.FIO_CREATED DESC, v_src.FIO_MOD DESC
                                ,v_src.BIRTH_AKT DESC, v_src.BIRTH_CREATED DESC, v_src.BIRTH_MOD DESC) as RANK
                  ,'Фио+Др.:'||v_rel.FIO||' '||v_rel.BIRTH as INFO_NOT_EQUAL_REL
                  ,DENSE_RANK() OVER (PARTITION BY v_rel.OBJECTS_ID 
                              ORDER BY v_rel.fio_akt DESC,  v_rel.FIO_CREATED DESC, v_rel.FIO_MOD DESC
                                ,v_rel.BIRTH_AKT DESC, v_rel.BIRTH_CREATED DESC, v_rel.BIRTH_MOD DESC) as RANK_REL
                  ,v_src.FIO AS FIO, v_src.BIRTH AS DR
                  ,v_rel.FIO AS FIO_REL, v_rel.BIRTH AS DR_REL
                  ,1 AS FRAUD_OUTNUMBER
                FROM AFS.VR_MOB_FB v_src
                INNER JOIN AFS.VR_MOB_FB v_rel
                  ON v_src.PHONE_MOB = v_rel.PHONE_MOB
                    AND v_src.OBJECTS_TYPE = v_rel.OBJECTS_TYPE
                    AND v_src.OBJECTS_ID ^= v_rel.OBJECTS_ID
                  AND v_src.REQUEST_ID > v_rel.REQUEST_ID
                WHERE v_src.REQUEST_ID = v_Request_ID AND v_src.OBJECTS_TYPE=2
                  AND (v_src.FIO^=v_rel.FIO OR v_src.BIRTH^=v_rel.BIRTH)
                  AND v_rel.CREATED_DATE BETWEEN v_src.CREATED_DATE-91*1 AND v_src.CREATED_DATE
                  AND NOT v_src.PHONE_MOB LIKE '%000000%' AND NOT v_src.PHONE_MOB LIKE '%999999%'
               ) WHERE RANK=1 AND RANK_REL=1 
              --оставляем только заявки не попавшие под общий фильтр
              AND AFS.afs_filter.check_request_id_rel(REQUEST_ID_REL)=0
              --исключаем девичьи фамилии
              AND NOT (SUBSTR(FIO, INSTR(FIO,' '), 254 )=SUBSTR(FIO_REL, INSTR(FIO_REL,' '), 254 ) AND DR=DR_REL )
              -- исключение однофамильцев 
              AND NOT UTL_MATCH.EDIT_DISTANCE(SUBSTR(FIO, 1, INSTR(FIO,' ')-1 ), SUBSTR(FIO_REL, 1, INSTR(FIO_REL,' ')-1 ))<3
               --PostCheck: Модифицированная проверка на родственников
              AND NOT AFS.FN_IS_FAMILY_REL(PERSON_ID, PERSON_ID_REL)=1
              AND NOT AFS.FN_IS_FAMILY_REL(PERSON_ID_REL, PERSON_ID)=1
              AND NOT AFS.FN_IS_FAMILY_CONT(PERSON_ID, PERSON_ID_REL)=1 
              AND NOT AFS.FN_IS_FAMILY_CONT(PERSON_ID_REL, PERSON_ID)=1
              AND NOT EXISTS(SELECT 1 FROM CPD.PHONES 
                                WHERE (OBJECTS_ID=PERSON_ID OR OBJECTS_ID=PERSON_ID_REL ) AND OBJECTS_TYPE=2 
                                    AND PHONE=SUBSTR(INFO_EQUAL, 9, 10)
                                    AND PHONES_COMM = 'Телефон из РБО: Мобильный') )
        LOOP
          v_OutNumber := ic.FRAUD_OUTNUMBER;
          v_OutChar := TO_CHAR(v_OutNumber);
		  
          afs_sys.write_afs_history(v_PROC_ID => v_PROC_ID
              , v_REQUEST_ID 			=> v_Request_ID
              , v_PERSON_ID 			=> ic.PERSON_ID
              , v_REQUEST_DATE 		=> ic.REQUEST_DATE
              , v_FRAUD_ID 				=> p_FRAUD_ID
              , v_FRAUD_OUTNUMBER => v_OutNumber
              , v_FIO 					  => ic.FIO 
              , v_DR 					    => ic.DR
              , v_DAY_BETWEEN 		=> ic.DAY_BETWEEN
              , v_REQUEST_ID_REL 		=> ic.REQUEST_ID_REL
              , v_PERSON_ID_REL 		=> ic.PERSON_ID_REL
              , v_REQUEST_DATE_REL 	=> ic.REQUEST_DATE_REL
              , v_FIO_REL 				  => ic.FIO_REL 
              , v_DR_REL 				    => ic.DR_REL
              , v_INFO_EQUAL 			   => ic.INFO_EQUAL
              , v_INFO_NOT_EQUAL 		 => ic.INFO_NOT_EQUAL
              , v_INFO_NOT_EQUAL_REL => ic.INFO_NOT_EQUAL_REL
              , v_E_CODE => v_ErrorCode, v_E_TEXT => v_ErrorText);
        END LOOP;
      WHEN p_FlagEx=0 THEN --если заявки нет присваиваем статус
        v_OutNumber := 9;
        v_OutChar := TO_CHAR(v_OutNumber);
      END CASE;
      
      --если фрод не нашелся возвращаем 0
      IF (v_OutNumber IS NULL) THEN 
        v_OutNumber := 0;
        v_OutChar := TO_CHAR(v_OutNumber);
      END IF;
    
      COMMIT;

    EXCEPTION WHEN OTHERS
      THEN 
        v_OutNumber := -1;
        v_OutChar := TO_CHAR(v_OutNumber);
        --присваиваем ошибки для системного мониторинга
        v_ErrorCode := 10031;
        v_ErrorText := SUBSTR(TO_CHAR(SQLCODE)||' | '||SQLERRM,0,4000);
  END FRAUD_0001;

  PROCEDURE FRAUD_0002(v_PROC_ID IN OUT NUMBER 
                          ,v_Request_ID IN NUMBER, v_Person_ID IN NUMBER, v_OutCode IN VARCHAR2, v_InArray IN VARCHAR2
                          ,v_OutNumber OUT NUMBER, v_OutChar OUT VARCHAR2
                          ,v_OutArray OUT VARCHAR2
                          ,v_ErrorCode OUT NUMBER, v_ErrorText OUT VARCHAR2
                        ) AS
  BEGIN
    /* TODO implementation required */
    NULL;
  END FRAUD_0002;

  PROCEDURE FRAUD_0003(v_PROC_ID IN OUT NUMBER 
                          ,v_Request_ID IN NUMBER, v_Person_ID IN NUMBER, v_OutCode IN VARCHAR2, v_InArray IN VARCHAR2
                          ,v_OutNumber OUT NUMBER, v_OutChar OUT VARCHAR2
                          ,v_OutArray OUT VARCHAR2
                          ,v_ErrorCode OUT NUMBER, v_ErrorText OUT VARCHAR2
                        ) AS
  BEGIN
    /* TODO implementation required */
    NULL;
  END FRAUD_0003;

  PROCEDURE FRAUD_0004(v_PROC_ID IN OUT NUMBER 
                          ,v_Request_ID IN NUMBER, v_Person_ID IN NUMBER, v_OutCode IN VARCHAR2, v_InArray IN VARCHAR2
                          ,v_OutNumber OUT NUMBER, v_OutChar OUT VARCHAR2
                          ,v_OutArray OUT VARCHAR2
                          ,v_ErrorCode OUT NUMBER, v_ErrorText OUT VARCHAR2
                        ) AS
  BEGIN
    /* TODO implementation required */
    NULL;
  END FRAUD_0004;

END AFS_RULES_PHONE;

/

  GRANT EXECUTE ON "AFS"."AFS_RULES_PHONE" TO "MVKARELIN";
--------------------------------------------------------
--  DDL for Package Body AFS_RULES_REQUEST
--------------------------------------------------------

  CREATE OR REPLACE PACKAGE BODY "AFS"."AFS_RULES_REQUEST" AS

  PROCEDURE FRAUD_0001(v_PROC_ID IN OUT NUMBER
                          ,v_Request_ID IN NUMBER, v_Person_ID IN NUMBER, v_OutCode IN VARCHAR2, v_InArray IN VARCHAR2
                          ,v_OutNumber OUT NUMBER, v_OutChar OUT VARCHAR2
                          ,v_OutArray OUT VARCHAR2
                          ,v_ErrorCode OUT NUMBER, v_ErrorText OUT VARCHAR2
                        ) AS
      --Правило 94 -	Client_diff_loan_amount_goal
      p_FRAUD_ID			NUMBER := 94;   --ID правила. Определяется по справочнику D_FRAUD_RULE
      p_FlagEx 				NUMBER;        --Флаг наличия заявки в ПКК

      pragma autonomous_transaction; 
  BEGIN
      --определяем новый уникальный ID процесса
      IF v_PROC_ID IS NULL OR v_PROC_ID<0 THEN 
        SELECT AFS.AFS_PROC_SEQ.NEXTVAL INTO v_PROC_ID FROM DUAL;
      END IF;   
      v_OutNumber := NULL;
      v_OutChar := '';
      v_OutArray := '';
      v_ErrorCode := 0;
      v_ErrorText := ''; 
      
      --проверяем существует ли заявка
      SELECT COUNT(*) INTO p_FlagEx FROM DUAL WHERE EXISTS(SELECT 1 FROM KREDIT.C_REQUEST WHERE REQUEST_ID=v_Request_ID
        AND AFS.afs_filter.check_request_id_main(v_Request_ID)=0);
       
      CASE WHEN p_FlagEx=1 THEN 
        --логика определения текущего фрод правила
        FOR ic IN (with main_request as (select /*+ materialize*/* from(
                select v_src.request_id, v_src.objects_id as person_id, v_src.created_date as request_date
                  , v_rel.request_id as request_id_rel, v_rel.objects_id as person_id_rel
                  , v_rel.created_date as request_date_rel
                  , abs(round(v_src.created_date-v_rel.created_date, 2)) as day_between
                  ,'Client_diff_loan_amount_goal' as  info_equal
                  ,'Заявленная сумма: '||v_crc.summa as info_not_equal
                  ,dense_rank() over (partition by v_src.request_id order by v_crr.request_react_id) as rank
                  ,'Заявленная сумма: '||r_crc.summa as info_not_equal_rel
                  ,dense_rank() over (partition by v_rel.request_id order by r_crr.request_react_id) as rank_rel
                  ,null as fio, null as dr
                  ,null as fio_rel,  null as dr_rel 
                  ,1 as fraud_outnumber
                  ,v_crc.summa as src_summa
                  ,r_crc.summa as rel_summa
                from kredit.c_request v_src
                inner join kredit.c_request_react v_crr on v_crr.request_id = v_src.request_id and v_crr.react_user_id <> 1512142 and v_src.score_tree_route_id <> 1
                inner join kredit.c_request_credit v_crc on v_crc.request_credit_id = v_crr.request_credit_id
                inner join kredit.c_request v_rel on v_rel.objects_id = v_src.objects_id
                    and v_src.request_id = v_request_id --59941879 --60183886 --61808545 -- v_request_id <--- входящая заявка
                    and v_src.request_id > v_rel.request_id
                    and v_rel.created_date >= v_src.created_date - 14
                    and v_src.type_request_id = 1 and v_rel.type_request_id = 1
                    and v_src.objects_type = 2 and v_rel.objects_type = 2    
                    and v_rel.score_tree_route_id <> 1
                inner join kredit.c_request_react r_crr on r_crr.request_id = v_rel.request_id and r_crr.react_user_id <> 1512142
                inner join kredit.c_request_credit r_crc on r_crc.request_credit_id = r_crr.request_credit_id   
              ) where rank = 1 and rank_rel = 1
              and abs(src_summa -  rel_summa)/src_summa >= 0.2)
            ,target_source_request as
                     (select crs.request_id,
                      sum(distinct crs.spr_value_id) over (partition by  crs.request_id) as sum_target_id,
                      --to_char(wm_concat(distinct nsn.shotname)over (partition by crs.request_id )) as target_request,
                      regexp_replace( LISTAGG(nsn.shotname, ';') WITHIN GROUP (ORDER BY nsn.shotname) OVER (PARTITION BY crs.request_id) 
                        , '([^;]+)(;\1)+', '\1') as target_request,
                      row_number() over(partition by crs.request_id order by crs.spr_value_id desc) as r_num
                    from kredit.c_request_spr crs
                    inner join main_request req 
                      on req.request_id = crs.request_id and crs.spr_names_id in (838390,43542106)
                    inner join kredit.spr_names nsn on nsn.id = crs.spr_value_id )
            ,target_rel_request as
                    (select crs.request_id,
                      sum(distinct crs.spr_value_id) over (partition by  crs.request_id) as sum_target_id,
                      --to_char(wm_concat(distinct nsn.shotname)over (partition by crs.request_id )) as target_request,
                      regexp_replace( LISTAGG(nsn.shotname, ';') WITHIN GROUP (ORDER BY nsn.shotname) OVER (PARTITION BY crs.request_id) 
                        , '([^;]+)(;\1)+', '\1') as target_request,
                      row_number() over(partition by crs.request_id order by crs.spr_value_id desc) as r_num
                    from kredit.c_request_spr crs
                    inner join main_request req 
                      on req.request_id_rel = crs.request_id and crs.spr_names_id in (838390,43542106)
                    inner join kredit.spr_names nsn on nsn.id = crs.spr_value_id
                )
              select DISTINCT s_req.request_id, s_req.person_id, s_req.request_date
                , s_req.request_id_rel, s_req.person_id_rel
                , s_req.request_date_rel
                , s_req.day_between
                ,s_req.info_equal
                ,s_req.info_not_equal||' Цель: '||tsr.target_request as info_not_equal
                ,s_req.rank
                ,s_req.info_not_equal_rel||' Цель: '||trt.target_request as info_not_equal_rel
                ,s_req.rank_rel
                ,s_req.fio, s_req.dr
                ,s_req.fio_rel,  s_req.dr_rel 
                ,s_req.fraud_outnumber
              from main_request s_req
              left join target_source_request tsr on s_req.request_id = tsr.request_id and tsr.r_num = 1
              left join target_rel_request trt on s_req.request_id_rel = trt.request_id
              and trt.r_num = 1
            where tsr.sum_target_id <> trt.sum_target_id
              --оставляем только заявки не попавшие под общий фильтр
              AND AFS.afs_filter.check_request_id_rel(REQUEST_ID_REL)=0
            )
        LOOP
          v_OutNumber := ic.FRAUD_OUTNUMBER;
          v_OutChar := TO_CHAR(v_OutNumber);
		  
          afs_sys.write_afs_history(v_PROC_ID => v_PROC_ID
              , v_REQUEST_ID 			=> v_Request_ID
              , v_PERSON_ID 			=> ic.PERSON_ID
              , v_REQUEST_DATE 		=> ic.REQUEST_DATE
              , v_FRAUD_ID 				=> p_FRAUD_ID
              , v_FRAUD_OUTNUMBER => v_OutNumber
              , v_FIO 					  => ic.FIO 
              , v_DR 					    => ic.DR
              , v_DAY_BETWEEN 		=> ic.DAY_BETWEEN
              , v_REQUEST_ID_REL 		=> ic.REQUEST_ID_REL
              , v_PERSON_ID_REL 		=> ic.PERSON_ID_REL
              , v_REQUEST_DATE_REL 	=> ic.REQUEST_DATE_REL
              , v_FIO_REL 				  => ic.FIO_REL 
              , v_DR_REL 				    => ic.DR_REL
              , v_INFO_EQUAL 			   => ic.INFO_EQUAL
              , v_INFO_NOT_EQUAL 		 => ic.INFO_NOT_EQUAL
              , v_INFO_NOT_EQUAL_REL => ic.INFO_NOT_EQUAL_REL
              , v_E_CODE => v_ErrorCode, v_E_TEXT => v_ErrorText);
        END LOOP;
      WHEN p_FlagEx=0 THEN --если заявки нет присваиваем статус
        v_OutNumber := 9;
        v_OutChar := TO_CHAR(v_OutNumber);
      END CASE;
      
      --если фрод не нашелся возвращаем 0
      IF (v_OutNumber IS NULL) THEN 
        v_OutNumber := 0;
        v_OutChar := TO_CHAR(v_OutNumber);
      END IF;

      COMMIT;

    EXCEPTION WHEN OTHERS
      THEN 
        v_OutNumber := -1;
        v_OutChar := TO_CHAR(v_OutNumber);
        --присваиваем ошибки для системного мониторинга
        v_ErrorCode := 10941;
        v_ErrorText := SUBSTR(TO_CHAR(SQLCODE)||' | '||SQLERRM,0,4000);
  END FRAUD_0001;

  PROCEDURE FRAUD_0002(v_PROC_ID IN OUT NUMBER
                          ,v_Request_ID IN NUMBER, v_Person_ID IN NUMBER, v_OutCode IN VARCHAR2, v_InArray IN VARCHAR2
                          ,v_OutNumber OUT NUMBER, v_OutChar OUT VARCHAR2
                          ,v_OutArray OUT VARCHAR2
                          ,v_ErrorCode OUT NUMBER, v_ErrorText OUT VARCHAR2
                        ) AS
      --Правило 94 -	Client_diff_loan_amount_goal
      p_FRAUD_ID			NUMBER := 94;   --ID правила. Определяется по справочнику D_FRAUD_RULE
      p_FlagEx 				NUMBER;        --Флаг наличия заявки в ПКК

      pragma autonomous_transaction; 
  BEGIN
      --определяем новый уникальный ID процесса
      IF v_PROC_ID IS NULL OR v_PROC_ID<0 THEN 
        SELECT AFS.AFS_PROC_SEQ.NEXTVAL INTO v_PROC_ID FROM DUAL;
      END IF;   
      v_OutNumber := NULL;
      v_OutChar := '';
      v_OutArray := '';
      v_ErrorCode := 0;
      v_ErrorText := ''; 
      
      --проверяем существует ли заявка
      SELECT COUNT(*) INTO p_FlagEx FROM DUAL WHERE EXISTS(SELECT 1 FROM KREDIT.C_REQUEST WHERE REQUEST_ID=v_Request_ID);
       
      CASE WHEN p_FlagEx=1 THEN 
        --логика определения текущего фрод правила
        FOR ic IN (select DISTINCT REQUEST_ID, PERSON_ID, REQUEST_DATE
              , FRAUD_OUTNUMBER
              , FIO, DR, DAY_BETWEEN
              , REQUEST_ID_REL, PERSON_ID_REL, REQUEST_DATE_REL
              , FIO_REL, DR_REL, INFO_EQUAL
              , INFO_NOT_EQUAL||'; Цель: '||target_request as info_not_equal
              , INFO_NOT_EQUAL_REL||'; Цель: '||target_request_rel as info_not_equal_rel
            from (
            select v_src.request_id, v_src.objects_id as person_id, v_src.created_date as request_date
                  , v_rel.request_id as request_id_rel, v_rel.objects_id as person_id_rel
                  , v_rel.created_date as request_date_rel
                  , abs(round(v_src.created_date-v_rel.created_date, 2)) as day_between
                  ,'Client_diff_loan_amount_goal' as  info_equal
                  ,'Заявленная сумма: '||v_src.summa as info_not_equal
                  ,dense_rank() over (partition by v_src.request_id order by v_src.request_react_id) as rank
                  ,'Заявленная сумма: '||v_rel.summa as info_not_equal_rel
                  ,dense_rank() over (partition by v_rel.request_id order by v_rel.request_react_id) as rank_rel
                  ,null as fio, null as dr
                  ,null as fio_rel,  null as dr_rel 
                  ,1 as fraud_outnumber
                  /*,v_src.summa as src_summa
                  ,v_rel.summa as rel_summa*/
                  ,sum(distinct tar_src.spr_value_id) over (partition by  tar_src.request_id) as sum_target_id
                  ,regexp_replace( LISTAGG(tar_src.shotname, ';') WITHIN GROUP (ORDER BY tar_src.shotname) OVER (PARTITION BY tar_src.request_id) 
                          , '([^;]+)(;\1)+', '\1') as target_request
                  ,DENSE_RANK() over(partition by tar_src.request_id order by tar_src.spr_value_id desc) as r_num
                  --цели найденного request_id
                  ,sum(distinct tar_rel.spr_value_id) over (partition by  tar_rel.request_id) as sum_target_id_rel
                  ,regexp_replace( LISTAGG(tar_rel.shotname, ';') WITHIN GROUP (ORDER BY tar_rel.shotname) OVER (PARTITION BY tar_rel.request_id) 
                          , '([^;]+)(;\1)+', '\1') as target_request_rel
                  ,DENSE_RANK() over(partition by tar_rel.request_id order by tar_rel.spr_value_id desc) as r_num_rel
                from afs.V_C_REQUEST_REACT_CREDIT v_src
                inner join afs.V_C_REQUEST_REACT_CREDIT v_rel 
                  on v_rel.objects_id = v_src.objects_id
                    and v_rel.type_request_id = 1 and v_rel.objects_type = 2 and v_rel.react_user_id <> 1512142 and v_rel.score_tree_route_id <> 1 
                    and v_rel.created_date >= v_src.created_date - 14
                     and v_rel.request_id < v_src.request_id
                     and abs(v_src.summa - v_rel.summa)/v_src.summa >= 0.2 --фильтруем по сумме
                left join AFS.V_C_REQUEST_SPR_SHOTNAME tar_src on v_src.request_id = tar_src.request_id
                left join AFS.V_C_REQUEST_SPR_SHOTNAME tar_rel on v_rel.request_id = tar_rel.request_id
                where v_src.request_id = v_request_id --59941879 --60183886 --61808545 -- v_request_id <--- входящая заявка
                  and v_src.type_request_id = 1 and v_src.objects_type = 2 and v_src.react_user_id <> 1512142 and v_src.score_tree_route_id <> 1
              ) where rank = 1 and rank_rel = 1
                  and sum_target_id <> sum_target_id_rel and r_num = 1 and r_num_rel = 1
                  --оставляем только заявки не попавшие под общий фильтр
                  AND AFS.afs_filter.check_request_id_rel(REQUEST_ID_REL)=0)
        LOOP
          v_OutNumber := ic.FRAUD_OUTNUMBER;
          v_OutChar := TO_CHAR(v_OutNumber);
		  
          afs_sys.write_afs_history(v_PROC_ID => v_PROC_ID
              , v_REQUEST_ID 			=> v_Request_ID
              , v_PERSON_ID 			=> ic.PERSON_ID
              , v_REQUEST_DATE 		=> ic.REQUEST_DATE
              , v_FRAUD_ID 				=> p_FRAUD_ID
              , v_FRAUD_OUTNUMBER => v_OutNumber
              , v_FIO 					  => ic.FIO 
              , v_DR 					    => ic.DR
              , v_DAY_BETWEEN 		=> ic.DAY_BETWEEN
              , v_REQUEST_ID_REL 		=> ic.REQUEST_ID_REL
              , v_PERSON_ID_REL 		=> ic.PERSON_ID_REL
              , v_REQUEST_DATE_REL 	=> ic.REQUEST_DATE_REL
              , v_FIO_REL 				  => ic.FIO_REL 
              , v_DR_REL 				    => ic.DR_REL
              , v_INFO_EQUAL 			   => ic.INFO_EQUAL
              , v_INFO_NOT_EQUAL 		 => ic.INFO_NOT_EQUAL
              , v_INFO_NOT_EQUAL_REL => ic.INFO_NOT_EQUAL_REL
              , v_E_CODE => v_ErrorCode, v_E_TEXT => v_ErrorText);
        END LOOP;
      WHEN p_FlagEx=0 THEN --если заявки нет присваиваем статус
        v_OutNumber := 9;
        v_OutChar := TO_CHAR(v_OutNumber);
      END CASE;
      
      --если фрод не нашелся возвращаем 0
      IF (v_OutNumber IS NULL) THEN 
        v_OutNumber := 0;
        v_OutChar := TO_CHAR(v_OutNumber);
      END IF;

      COMMIT;

    EXCEPTION WHEN OTHERS
      THEN 
        v_OutNumber := -1;
        v_OutChar := TO_CHAR(v_OutNumber);
        --присваиваем ошибки для системного мониторинга
        v_ErrorCode := 10941;
        v_ErrorText := SUBSTR(TO_CHAR(SQLCODE)||' | '||SQLERRM,0,4000);
  END FRAUD_0002;

  PROCEDURE FRAUD_0003(v_PROC_ID IN OUT NUMBER
                          ,v_Request_ID IN NUMBER, v_Person_ID IN NUMBER, v_OutCode IN VARCHAR2, v_InArray IN VARCHAR2
                          ,v_OutNumber OUT NUMBER, v_OutChar OUT VARCHAR2
                          ,v_OutArray OUT VARCHAR2
                          ,v_ErrorCode OUT NUMBER, v_ErrorText OUT VARCHAR2
                        ) AS
  BEGIN
    /* TODO implementation required */
    NULL;
  END FRAUD_0003;

  PROCEDURE FRAUD_0004(v_PROC_ID IN OUT NUMBER
                          ,v_Request_ID IN NUMBER, v_Person_ID IN NUMBER, v_OutCode IN VARCHAR2, v_InArray IN VARCHAR2
                          ,v_OutNumber OUT NUMBER, v_OutChar OUT VARCHAR2
                          ,v_OutArray OUT VARCHAR2
                          ,v_ErrorCode OUT NUMBER, v_ErrorText OUT VARCHAR2
                        ) AS
  BEGIN
    /* TODO implementation required */
    NULL;
  END FRAUD_0004;

END AFS_RULES_REQUEST;

/
--------------------------------------------------------
--  DDL for Package Body AFS_RULES_TEST
--------------------------------------------------------

  CREATE OR REPLACE PACKAGE BODY "AFS"."AFS_RULES_TEST" AS

  --тестовая процедура-пустышка // для корректировки метода обработки результатов и различных тестов
  PROCEDURE FRAUD_0001(v_PROC_ID IN OUT NUMBER 
                          ,v_Request_ID IN NUMBER, v_Person_ID IN NUMBER, v_OutCode IN VARCHAR2, v_InArray IN VARCHAR2
                          ,v_OutNumber OUT NUMBER, v_OutChar OUT VARCHAR2
                          ,v_OutArray OUT VARCHAR2 
                          ,v_ErrorCode OUT NUMBER, v_ErrorText OUT VARCHAR2
                        ) AS
      p_FRAUD_ID			NUMBER := 0;   --ID правила. Определяется по справочнику D_FRAUD_RULE
      p_FRAUD_NM			VARCHAR2(255) := 'AFS.AFS_RULES_TEST.FRAUD_0001';   --ID правила. Определяется по справочнику D_FRAUD_RULE
      p_FlagEx 				NUMBER;        --Флаг наличия заявки в ПКК

      pragma autonomous_transaction; 
  BEGIN
      --определяем новый уникальный ID процесса
      IF v_PROC_ID IS NULL OR v_PROC_ID<0 THEN 
        SELECT AFS.AFS_PROC_SEQ.NEXTVAL INTO v_PROC_ID FROM DUAL;
      END IF; 
  
      v_OutNumber := NULL; 
      v_OutChar := '';
      v_OutArray := '';
      v_ErrorCode := 0;
      v_ErrorText := '';
      
      --проверяем существует ли заявка
      SELECT COUNT(*) INTO p_FlagEx FROM DUAL WHERE EXISTS(SELECT 1 FROM KREDIT.C_REQUEST WHERE REQUEST_ID=v_Request_ID
        AND AFS.afs_filter.check_request_id_main(v_Request_ID)=0);
      
      CASE WHEN p_FlagEx=1 THEN 
      FOR ic IN (SELECT * FROM 
                  (SELECT sysdate as DATE_FRAUD
                    ,v_Request_ID AS REQUEST_ID
                    ,v_Person_ID AS PERSON_ID
                    ,NULL AS REQUEST_DATE
                    ,NULL AS FIO, NULL AS DR
                    ,NULL AS DAY_BETWEEN
                    ,NULL AS REQUEST_ID_REL
                    ,NULL AS PERSON_ID_REL
                    ,NULL AS REQUEST_DATE_REL
                    ,NULL AS FIO_REL, NULL AS DR_REL
                    --INFO: для каждого правила переобозначить инфу в колонках INFO_EQ, INFO_NEQ, INFO_NEQ_REL
                    ,'Паспорт:'||'ssss nnnnnn' as INFO_EQUAL
                    ,'Фио+Др.:'||'Ф И О дд.мм.гггг' as INFO_NOT_EQUAL
                    ,'Фио+Др.:'||'Ф И О дд.мм.гггг' as INFO_NOT_EQUAL_REL
                    ,1 as FRAUD_OUTNUMBER
                    --,NULL as F_POS
                  FROM DUAL
                  ) TAB
                WHERE rownum=1
                )
        LOOP
          v_OutNumber := ic.FRAUD_OUTNUMBER;
          v_OutChar := TO_CHAR(v_OutNumber);
		  
          afs_sys.write_afs_history(v_PROC_ID => v_PROC_ID
              , v_REQUEST_ID 			=> v_Request_ID
              , v_PERSON_ID 			=> ic.PERSON_ID
              , v_REQUEST_DATE 		=> ic.REQUEST_DATE
              , v_FRAUD_ID 				=> p_FRAUD_ID
              , v_FRAUD_OUTNUMBER => v_OutNumber
              , v_FIO 					  => ic.FIO 
              , v_DR 					    => ic.DR
              , v_DAY_BETWEEN 		=> ic.DAY_BETWEEN
              , v_REQUEST_ID_REL 		=> ic.REQUEST_ID_REL
              , v_PERSON_ID_REL 		=> ic.PERSON_ID_REL
              , v_REQUEST_DATE_REL 	=> ic.REQUEST_DATE_REL
              , v_FIO_REL 				  => ic.FIO_REL 
              , v_DR_REL 				    => ic.DR_REL
              , v_INFO_EQUAL 			   => ic.INFO_EQUAL
              , v_INFO_NOT_EQUAL 		 => ic.INFO_NOT_EQUAL
              , v_INFO_NOT_EQUAL_REL => ic.INFO_NOT_EQUAL_REL
              , v_E_CODE => v_ErrorCode, v_E_TEXT => v_ErrorText);
        END LOOP;
      WHEN p_FlagEx=0 THEN --если заявки нет присваиваем статус
        v_OutNumber := 9;
        v_OutChar := TO_CHAR(v_OutNumber);
      END CASE;
      
      --если фрод не нашелся возвращаем 0
      IF (v_OutNumber IS NULL) THEN 
        v_OutNumber := 0;
        v_OutChar := TO_CHAR(v_OutNumber);
      END IF;
    
      COMMIT;

    EXCEPTION WHEN OTHERS
      THEN 
        v_OutNumber := -1;
        v_OutChar := TO_CHAR(v_OutNumber);
        --присваиваем ошибки для системного мониторинга
        v_ErrorCode := 10001;
        v_ErrorText := SUBSTR(TO_CHAR(SQLCODE)||' | '||SQLERRM,0,4000);
  END FRAUD_0001;

  PROCEDURE FRAUD_0002(v_PROC_ID IN OUT NUMBER 
                          ,v_Request_ID IN NUMBER, v_Person_ID IN NUMBER, v_OutCode IN VARCHAR2, v_InArray IN VARCHAR2
                          ,v_OutNumber OUT NUMBER, v_OutChar OUT VARCHAR2
                          ,v_OutArray OUT VARCHAR2
                          ,v_ErrorCode OUT NUMBER, v_ErrorText OUT VARCHAR2
                        ) AS
      p_FRAUD_ID			NUMBER := 12;   --ID правила. Определяется по справочнику D_FRAUD_RULE
      p_FRAUD_NM			VARCHAR2(255) := 'AFS.AFS_RULES_TEST.FRAUD_0002';   --ID правила. Определяется по справочнику D_FRAUD_RULE
      p_FlagEx 				NUMBER;        --Флаг наличия заявки в ПКК

      pragma autonomous_transaction; 
  BEGIN
      --определяем новый уникальный ID процесса
      IF v_PROC_ID IS NULL OR v_PROC_ID<0 THEN 
        SELECT AFS.AFS_PROC_SEQ.NEXTVAL INTO v_PROC_ID FROM DUAL;
      END IF; 
  
      v_OutNumber := NULL;
      v_OutChar := '';
      v_OutArray := '';
      v_ErrorCode := 0;
      v_ErrorText := '';
      
      --проверяем существует ли заявка
      SELECT COUNT(*) INTO p_FlagEx FROM DUAL WHERE EXISTS(SELECT 1 FROM KREDIT.C_REQUEST WHERE REQUEST_ID=v_Request_ID
        AND AFS.afs_filter.check_request_id_main(v_Request_ID)=0);
      
      CASE WHEN p_FlagEx=1 THEN 
      FOR ic IN (/*select 
              v_src.request_id, v_src.objects_id as person_id, v_src.created_date as request_date
            , v_rel.request_id as request_id_rel, v_rel.objects_id as person_id_rel
            , v_rel.created_date as request_date_rel
            , abs(round(v_src.created_date-v_rel.created_date, 2)) as day_between
            ,trim('Клиент-агент') as  info_equal
            ,'Тарифный план: '||c_src.tc_name as info_not_equal
             ,cast(null as number) as rank
            ,'Тарифный план: '||c_rel.tc_name as info_not_equal_rel
            ,cast(null as number) as rank_rel
            ,f_src.fio4search as fio, cri.bdate as dr
            ,f_src.fio4search as fio_rel,  cri.bdate as dr_rel -- так как одинаковый objects_id 
            ,12 as fraud_outnumber
   from
             kredit.c_request v_src
                       inner join kredit.c_request v_rel on v_src.objects_id = v_rel.objects_id
                       and v_src.request_id = v_Request_ID -- v_request_id <--- входящая заявка
                                                                               and v_src.request_id > v_rel.request_id
                                                                               and v_src.type_request_id = 1 and v_rel.type_request_id = 1
                                                                               and v_src.objects_type = 2 and v_rel.objects_type = 2
                                                                               and v_rel.created_group_id in (8393,6955) -- группы аккредитации агентов
                       inner join kredit.c_request_info cri on cri.request_info_id = v_src.request_info_id_last 
                       inner join kredit.view_fio_history f_src on f_src.fio_objects_id = v_src.objects_id and f_src.fio_akt = 1
                       inner join kredit.view_request_credit c_src on c_src.request_credit_id = v_src.request_credit_id_last
                       inner join kredit.view_request_credit c_rel on c_rel.request_credit_id = v_rel.request_credit_id_last */

          select v_src.request_id, v_src.objects_id as person_id, v_src.created_date as request_date
                  , v_rel.request_id as request_id_rel, v_rel.objects_id as person_id_rel
                  , v_rel.created_date as request_date_rel
                  , abs(round(v_src.created_date-v_rel.created_date, 2)) as day_between
                  ,'OBJECTS_ID: '||to_char(v_src.OBJECTS_ID) as  info_equal
                  --,'Тарифный план: '||tc.tc_name as info_not_equal
                  ,NULL as info_not_equal
                  ,cast(null as number) as rank
                  --,'Тарифный план: '||tc_rel.tc_name as info_not_equal_rel
                  ,NULL  as info_not_equal_rel
                  ,cast(null as number) as rank_rel
                  ,NULL as fio, NULL as dr
                  ,NULL as fio_rel,  NULL as dr_rel -- так как одинаковый objects_id 
                  --,f_src.fio4search as fio, cri.bdate as dr
                  --,f_src.fio4search as fio_rel,  cri.bdate as dr_rel -- так как одинаковый objects_id 
                  ,1 as fraud_outnumber
                from kredit.c_request v_src
                inner join kredit.c_request v_rel 
                  on v_src.request_id = v_Request_ID -- v_request_id <--- входящая заявка
                    and v_src.type_request_id = 1 and v_src.objects_type = 2 
                    AND v_src.objects_id = v_rel.objects_id
                    and v_src.request_id > v_rel.request_id
                    and v_rel.type_request_id = 1 and v_rel.objects_type = 2
                    and v_rel.created_group_id in (8393,6955) -- группы аккредитации агентов
                    --Смотрим всё на один и тот же objects_id
                /*inner join kredit.c_request_info cri 
                  on cri.request_info_id = v_src.request_info_id_last*/
                /*inner join kredit.view_fio_history f_src 
                  on f_src.fio_objects_id = v_src.objects_id and f_src.fio_akt = 1*/
                /*вывод тарифного плана для подтверждения работы правила*/  
                INNER JOIN kredit.c_request_credit c_src  ON c_src.request_credit_id = v_src.request_credit_id_last
                INNER JOIN kredit.c_type_credit tc         ON tc.type_credit_id = c_src.type_credit_id
                INNER JOIN kredit.c_request_credit c_rel  ON c_rel.request_credit_id = v_rel.request_credit_id_last
                INNER JOIN kredit.c_type_credit tc_rel     ON tc_rel.type_credit_id = c_rel.type_credit_id
              WHERE 
              --оставляем только заявки не попавшие под общий фильтр
              AFS.afs_filter.check_request_id_rel(v_rel.request_id)=0
                )
              
        LOOP
          v_OutNumber := ic.FRAUD_OUTNUMBER;
          v_OutChar := TO_CHAR(v_OutNumber);
		  
          afs_sys.write_afs_history(v_PROC_ID => v_PROC_ID
              , v_REQUEST_ID 			=> v_Request_ID
              , v_PERSON_ID 			=> ic.PERSON_ID
              , v_REQUEST_DATE 		=> ic.REQUEST_DATE
              , v_FRAUD_ID 				=> p_FRAUD_ID
              , v_FRAUD_OUTNUMBER => v_OutNumber
              , v_FIO 					  => ic.FIO 
              , v_DR 					    => ic.DR
              , v_DAY_BETWEEN 		=> ic.DAY_BETWEEN
              , v_REQUEST_ID_REL 		=> ic.REQUEST_ID_REL
              , v_PERSON_ID_REL 		=> ic.PERSON_ID_REL
              , v_REQUEST_DATE_REL 	=> ic.REQUEST_DATE_REL
              , v_FIO_REL 				  => ic.FIO_REL 
              , v_DR_REL 				    => ic.DR_REL
              , v_INFO_EQUAL 			   => ic.INFO_EQUAL
              , v_INFO_NOT_EQUAL 		 => ic.INFO_NOT_EQUAL
              , v_INFO_NOT_EQUAL_REL => ic.INFO_NOT_EQUAL_REL
              , v_E_CODE => v_ErrorCode, v_E_TEXT => v_ErrorText);
        END LOOP;
      WHEN p_FlagEx=0 THEN --если заявки нет присваиваем статус
        v_OutNumber := 9;
        v_OutChar := TO_CHAR(v_OutNumber);
      END CASE;
      
      --если фрод не нашелся возвращаем 0
      IF (v_OutNumber IS NULL) THEN 
        v_OutNumber := 0;
        v_OutChar := TO_CHAR(v_OutNumber);
      END IF;
    
      v_ErrorCode := SQLCODE;
      v_ErrorText := SUBSTR(SQLCODE||' | '||SQLERRM,0,4000);
	COMMIT;

    EXCEPTION WHEN OTHERS
      THEN 
        v_OutNumber := -1;
        v_OutChar := TO_CHAR(v_OutNumber);
        --присваиваем ошибки для системного мониторинга
        v_ErrorCode := 10001;
        v_ErrorText := SUBSTR(TO_CHAR(SQLCODE)||' | '||SQLERRM,0,4000);
  END FRAUD_0002;

  PROCEDURE FRAUD_0003(v_PROC_ID IN OUT NUMBER 
                          ,v_Request_ID IN NUMBER, v_Person_ID IN NUMBER, v_OutCode IN VARCHAR2, v_InArray IN VARCHAR2
                          ,v_OutNumber OUT NUMBER, v_OutChar OUT VARCHAR2
                          ,v_OutArray OUT VARCHAR2
                          ,v_ErrorCode OUT NUMBER, v_ErrorText OUT VARCHAR2
                        ) AS
      p_FRAUD_ID			NUMBER := 12;   --ID правила. Определяется по справочнику D_FRAUD_RULE
      p_FRAUD_NM			VARCHAR2(255) := 'AFS.AFS_RULES_TEST.FRAUD_0002';   --ID правила. Определяется по справочнику D_FRAUD_RULE
      p_FlagEx 				NUMBER;        --Флаг наличия заявки в ПКК

      pragma autonomous_transaction; 
  BEGIN
      --определяем новый уникальный ID процесса
      IF v_PROC_ID IS NULL OR v_PROC_ID<0 THEN 
        SELECT AFS.AFS_PROC_SEQ.NEXTVAL INTO v_PROC_ID FROM DUAL;
      END IF; 
  
      v_OutNumber := NULL;
      v_OutChar := '';
      v_OutArray := '';
      v_ErrorCode := 0;
      v_ErrorText := '';
      
      --проверяем существует ли заявка
      SELECT COUNT(*) INTO p_FlagEx FROM DUAL WHERE EXISTS(SELECT 1 FROM KREDIT.C_REQUEST WHERE REQUEST_ID=v_Request_ID
        AND AFS.afs_filter.check_request_id_main(v_Request_ID)=0);
      
      CASE WHEN p_FlagEx=1 THEN 
      FOR ic IN (select v_src.request_id, v_src.objects_id as person_id, v_src.created_date as request_date
                  , v_rel.request_id as request_id_rel, v_rel.objects_id as person_id_rel
                  , v_rel.created_date as request_date_rel
                  , abs(round(v_src.created_date-v_rel.created_date, 2)) as day_between
                  ,'OBJECTS_ID: '||to_char(v_src.OBJECTS_ID) as  info_equal
                  ,NULL  /*'Тарифный план: '||tc.tc_name*/ as info_not_equal
                  ,cast(null as number) as rank
                  ,NULL /*'Тарифный план: '||tc_rel.tc_name*/ as info_not_equal_rel
                  ,cast(null as number) as rank_rel
                  ,NULL as fio, NULL as dr
                  ,NULL as fio_rel,  NULL as dr_rel -- так как одинаковый objects_id 
                  /*,f_src.fio4search as fio, cri.bdate as dr
                  ,f_src.fio4search as fio_rel,  cri.bdate as dr_rel -- так как одинаковый objects_id */
                  ,1 as fraud_outnumber
                from kredit.c_request v_src
                inner join kredit.c_request v_rel 
                  on v_src.request_id = v_Request_ID -- v_request_id <--- входящая заявка
                    and v_src.type_request_id = 1 and v_src.objects_type = 2 
                    AND v_src.objects_id = v_rel.objects_id
                    and v_src.request_id > v_rel.request_id
                    and v_rel.type_request_id = 1 and v_rel.objects_type = 2
                    and v_rel.created_group_id in (8393,6955) -- группы аккредитации агентов
                    /*Смотрим всё на один и тот же objects_id*/
                inner join kredit.c_request_info cri 
                  on cri.request_info_id = v_src.request_info_id_last
                /*inner join kredit.view_fio_history f_src 
                  on f_src.fio_objects_id = v_src.objects_id and f_src.fio_akt = 1*/
                /*вывод тарифного плана для подтверждения работы правила*/  
                /*INNER JOIN kredit.c_request_credit c_src  ON c_src.request_credit_id = v_src.request_credit_id_last
                INNER JOIN kredit.c_type_credit tc         ON tc.type_credit_id = c_src.type_credit_id
                INNER JOIN kredit.c_request_credit c_rel  ON c_rel.request_credit_id = v_rel.request_credit_id_last
                INNER JOIN kredit.c_type_credit tc_rel     ON tc_rel.type_credit_id = c_rel.type_credit_id*/
                )
        LOOP
          v_OutNumber := ic.FRAUD_OUTNUMBER;
          v_OutChar := TO_CHAR(v_OutNumber);
          
          afs_sys.write_afs_history(v_PROC_ID => v_PROC_ID
              , v_REQUEST_ID 			=> v_Request_ID
              , v_PERSON_ID 			=> ic.PERSON_ID
              , v_REQUEST_DATE 		=> ic.REQUEST_DATE
              , v_FRAUD_ID 				=> p_FRAUD_ID
              , v_FRAUD_OUTNUMBER => v_OutNumber
              , v_FIO 					  => ic.FIO 
              , v_DR 					    => ic.DR
              , v_DAY_BETWEEN 		=> ic.DAY_BETWEEN
              , v_REQUEST_ID_REL 		=> ic.REQUEST_ID_REL
              , v_PERSON_ID_REL 		=> ic.PERSON_ID_REL
              , v_REQUEST_DATE_REL 	=> ic.REQUEST_DATE_REL
              , v_FIO_REL 				  => ic.FIO_REL 
              , v_DR_REL 				    => ic.DR_REL
              , v_INFO_EQUAL 			   => ic.INFO_EQUAL
              , v_INFO_NOT_EQUAL 		 => ic.INFO_NOT_EQUAL
              , v_INFO_NOT_EQUAL_REL => ic.INFO_NOT_EQUAL_REL
              , v_E_CODE => v_ErrorCode, v_E_TEXT => v_ErrorText);
        END LOOP;
      WHEN p_FlagEx=0 THEN --если заявки нет присваиваем статус
        v_OutNumber := 9;
        v_OutChar := TO_CHAR(v_OutNumber);
      END CASE;
      
      --если фрод не нашелся возвращаем 0
      IF (v_OutNumber IS NULL) THEN 
        v_OutNumber := 0;
        v_OutChar := TO_CHAR(v_OutNumber);
      END IF;

      COMMIT;

    EXCEPTION WHEN OTHERS
      THEN 
        v_OutNumber := -1;
        v_OutChar := TO_CHAR(v_OutNumber);
        --присваиваем ошибки для системного мониторинга
        v_ErrorCode := 10121;
        v_ErrorText := SUBSTR(TO_CHAR(SQLCODE)||' | '||SQLERRM,0,4000);
  END FRAUD_0003;
  

  PROCEDURE FRAUD_0004(v_PROC_ID IN OUT NUMBER 
                          ,v_Request_ID IN NUMBER, v_Person_ID IN NUMBER, v_OutCode IN VARCHAR2, v_InArray IN VARCHAR2
                          ,v_OutNumber OUT NUMBER, v_OutChar OUT VARCHAR2 
                          ,v_OutArray OUT VARCHAR2
                          ,v_ErrorCode OUT NUMBER, v_ErrorText OUT VARCHAR2
                        ) AS
      --Правило 3 -	Телефон1-Псп0
      p_FRAUD_ID			NUMBER := 3;   --ID правила. Определяется по справочнику D_FRAUD_RULE
      p_FRAUD_NM			VARCHAR2(255) := 'AFS.AFS_....FRAUD_0001';   --ID правила. Определяется по справочнику D_FRAUD_RULE
      p_FlagEx 				NUMBER;        --Флаг наличия заявки в ПКК

      pragma autonomous_transaction; 
  BEGIN
      --определяем новый уникальный ID процесса
      IF v_PROC_ID IS NULL OR v_PROC_ID<0 THEN 
        SELECT AFS.AFS_PROC_SEQ.NEXTVAL INTO v_PROC_ID FROM DUAL;
      END IF; 
  
      v_OutNumber := NULL;
      v_OutChar := '';
      v_OutArray := '';
      v_ErrorCode := 0;
      v_ErrorText := ''; 
      
      --проверяем существует ли заявка
      SELECT COUNT(*) INTO p_FlagEx FROM DUAL WHERE EXISTS(SELECT 1 FROM KREDIT.C_REQUEST WHERE REQUEST_ID=v_Request_ID);
      
      CASE WHEN p_FlagEx=1 THEN 
        --логика определения текущего фрод правила
        FOR ic IN (SELECT * FROM (     
              SELECT DISTINCT v_src.REQUEST_ID, v_src.OBJECTS_ID AS PERSON_ID, v_src.CREATED_DATE AS REQUEST_DATE
                  , v_rel.REQUEST_ID AS REQUEST_ID_REL, v_rel.OBJECTS_ID AS PERSON_ID_REL
                  , v_rel.CREATED_DATE AS REQUEST_DATE_REL
                  , ABS(ROUND(v_src.CREATED_DATE-v_rel.CREATED_DATE, 2)) as DAY_BETWEEN
                  , 'Тел.моб:'||v_src.PHONE_MOB as INFO_EQUAL
                  ,'Фио+Др.:'||v_src.FIO||' '||v_src.BIRTH as INFO_NOT_EQUAL
                  ,DENSE_RANK() OVER (PARTITION BY v_src.OBJECTS_ID 
                              ORDER BY v_src.fio_akt DESC,  v_src.FIO_CREATED DESC, v_src.FIO_MOD DESC
                                ,v_src.BIRTH_AKT DESC, v_src.BIRTH_CREATED DESC, v_src.BIRTH_MOD DESC) as RANK
                  ,'Фио+Др.:'||v_rel.FIO||' '||v_rel.BIRTH as INFO_NOT_EQUAL_REL
                  ,DENSE_RANK() OVER (PARTITION BY v_rel.OBJECTS_ID 
                              ORDER BY v_rel.fio_akt DESC,  v_rel.FIO_CREATED DESC, v_rel.FIO_MOD DESC
                                ,v_rel.BIRTH_AKT DESC, v_rel.BIRTH_CREATED DESC, v_rel.BIRTH_MOD DESC) as RANK_REL
                  ,v_src.FIO AS FIO, v_src.BIRTH AS DR
                  ,v_rel.FIO AS FIO_REL, v_rel.BIRTH AS DR_REL
                  ,1 AS FRAUD_OUTNUMBER
                FROM AFS.VR_MOB_FB v_src
                INNER JOIN AFS.VR_MOB_FB v_rel
                  ON v_src.PHONE_MOB = v_rel.PHONE_MOB
                    AND v_src.OBJECTS_TYPE = v_rel.OBJECTS_TYPE
                    AND v_src.OBJECTS_ID ^= v_rel.OBJECTS_ID
                  AND v_src.REQUEST_ID > v_rel.REQUEST_ID
                WHERE v_src.REQUEST_ID = v_Request_ID AND v_src.OBJECTS_TYPE=2
                  AND (v_src.FIO^=v_rel.FIO OR v_src.BIRTH^=v_rel.BIRTH)
                  AND v_rel.CREATED_DATE BETWEEN v_src.CREATED_DATE-91*1 AND v_src.CREATED_DATE
                  AND NOT v_src.PHONE_MOB LIKE '%000000%' AND NOT v_src.PHONE_MOB LIKE '%999999%'
               ) WHERE RANK=1 AND RANK_REL=1 
              AND AFS.FN$IS_REQUEST_FOR_FRAUD(REQUEST_ID_REL, 1)=1
              --исключаем девичьи фамилии
              AND NOT (SUBSTR(FIO, INSTR(FIO,' '), 254 )=SUBSTR(FIO_REL, INSTR(FIO_REL,' '), 254 ) AND DR=DR_REL )
              -- исключение однофамильцев 
              AND NOT UTL_MATCH.EDIT_DISTANCE(SUBSTR(FIO, 1, INSTR(FIO,' ')-1 ), SUBSTR(FIO_REL, 1, INSTR(FIO_REL,' ')-1 ))<3
               --PostCheck: Модифицированная проверка на родственников
              AND NOT AFS.FN_IS_FAMILY_REL(PERSON_ID, PERSON_ID_REL)=1
              AND NOT AFS.FN_IS_FAMILY_REL(PERSON_ID_REL, PERSON_ID)=1
              AND NOT AFS.FN_IS_FAMILY_CONT(PERSON_ID, PERSON_ID_REL)=1 
              AND NOT AFS.FN_IS_FAMILY_CONT(PERSON_ID_REL, PERSON_ID)=1
              AND NOT EXISTS(SELECT 1 FROM CPD.PHONES 
                                WHERE (OBJECTS_ID=PERSON_ID OR OBJECTS_ID=PERSON_ID_REL ) AND OBJECTS_TYPE=2 
                                    AND PHONE=SUBSTR(INFO_EQUAL, 9, 10)
                                    AND PHONES_COMM = 'Телефон из РБО: Мобильный') )
        LOOP
          v_OutNumber := ic.FRAUD_OUTNUMBER;
          v_OutChar := TO_CHAR(v_OutNumber);
		  
          afs_sys.write_afs_history(v_PROC_ID => v_PROC_ID
              , v_REQUEST_ID 			=> v_Request_ID
              , v_PERSON_ID 			=> ic.PERSON_ID
              , v_REQUEST_DATE 		=> ic.REQUEST_DATE
              , v_FRAUD_ID 				=> p_FRAUD_ID
              , v_FRAUD_OUTNUMBER => v_OutNumber
              , v_FIO 					  => ic.FIO 
              , v_DR 					    => ic.DR
              , v_DAY_BETWEEN 		=> ic.DAY_BETWEEN
              , v_REQUEST_ID_REL 		=> ic.REQUEST_ID_REL
              , v_PERSON_ID_REL 		=> ic.PERSON_ID_REL
              , v_REQUEST_DATE_REL 	=> ic.REQUEST_DATE_REL
              , v_FIO_REL 				  => ic.FIO_REL 
              , v_DR_REL 				    => ic.DR_REL
              , v_INFO_EQUAL 			   => ic.INFO_EQUAL
              , v_INFO_NOT_EQUAL 		 => ic.INFO_NOT_EQUAL
              , v_INFO_NOT_EQUAL_REL => ic.INFO_NOT_EQUAL_REL
              , v_E_CODE => v_ErrorCode, v_E_TEXT => v_ErrorText);
        END LOOP;
      WHEN p_FlagEx=0 THEN --если заявки нет присваиваем статус
        v_OutNumber := 9;
        v_OutChar := TO_CHAR(v_OutNumber);
      END CASE;
      
      --если фрод не нашелся возвращаем 0
      IF (v_OutNumber IS NULL) THEN 
        v_OutNumber := 0;
        v_OutChar := TO_CHAR(v_OutNumber);
      END IF;
    
      COMMIT;

    EXCEPTION WHEN OTHERS
      THEN 
        v_OutNumber := -1;
        v_OutChar := TO_CHAR(v_OutNumber);
        --присваиваем ошибки для системного мониторинга
        v_ErrorCode := 10031;
        v_ErrorText := SUBSTR(TO_CHAR(SQLCODE)||' | '||SQLERRM,0,4000);
  END FRAUD_0004;


END AFS_RULES_TEST;

/
--------------------------------------------------------
--  DDL for Package Body AFS_SYS
--------------------------------------------------------

  CREATE OR REPLACE PACKAGE BODY "AFS"."AFS_SYS" AS
  --Процедура WRITE_LOG_ERROR - НЕАКТУЛАЬНА!
  --ПРОЦЕДУРА: пишем в лог ошибок, если при проверке любого правила что-то произошло
  PROCEDURE WRITE_LOG_ERROR (v_PROC_ID IN NUMBER, v_E_CODE IN NUMBER
                          , v_E_TEXT IN VARCHAR2, v_E_TYPE IN VARCHAR2, v_E_INFO IN VARCHAR2) AS
    pragma autonomous_transaction;
  BEGIN
    --ЛОГИРУЕМ ОШИБКУ В ТАБЛИЦУ ЛОГИРОВАНИЯ
    MERGE INTO AFS.AFS_LOG_ERROR tar
      USING (SELECT 1 FROM DUAL) t_new
    ON (tar.PROC_ID=v_PROC_ID)
    WHEN MATCHED THEN UPDATE SET
        tar.ERRORCODE=v_E_CODE
        ,tar.ERRORTEXT=v_E_TEXT
        ,tar.ERROR_TYPE=v_E_TYPE
        ,tar.ERROR_INFO=v_E_INFO
    WHERE NOT (--ОБНОВЛЯЕТСЯ ТОЛЬКО ЕСЛИ ЧТО ТО ИЗМЕНИЛОСЬ!!
          NVL(tar.ERRORCODE, 0)=NVL(v_E_CODE, 0)
      AND NVL(tar.ERRORTEXT, '-')=NVL(v_E_TEXT, '-') 
      AND NVL(tar.ERROR_TYPE, '-')=NVL(v_E_TYPE, '-')
      AND NVL(tar.ERROR_INFO, '-')=NVL(v_E_INFO, '-') 
        ) 
    WHEN NOT MATCHED THEN  
    --вставляем новое
    INSERT (tar.PROC_ID, tar.ERRORCODE, tar.ERRORTEXT, tar.ERROR_TYPE, tar.ERROR_INFO)
      VALUES (v_PROC_ID, v_E_CODE, v_E_TEXT, v_E_TYPE, v_E_INFO)
    ; 
    COMMIT;
  EXCEPTION WHEN OTHERS 
    THEN 
      INSERT INTO AFS.AFS_LOG_ERROR (PROC_ID, ERRORCODE, ERRORTEXT, ERROR_TYPE, ERROR_INFO)
      VALUES (v_PROC_ID, v_E_CODE, v_E_TEXT, v_E_TYPE, v_E_INFO)
    ;
  END WRITE_LOG_ERROR;  

  PROCEDURE WRITE_LOG_CALL (
          v_PROC_ID IN NUMBER     --уникальный ID процедуры 
          ,v_FRAUD_ID IN NUMBER  --внутренний ID правила по справочнику
          ,v_FRAUD_NAME IN VARCHAR2 --внутреннее имя правила по справочнику
          ,v_REQUEST_ID IN NUMBER --ID заявки ПКК
          ,v_PERSON_ID IN NUMBER --ID физика ПКК
          ,v_OUTCODE IN VARCHAR2  --Код внешней системы (для синхронизации между системами)
          ,v_INARRAY IN VARCHAR2  --Параметр для расширения (массив данных, структуру проработаем позже)
          ,v_OUTNUMBER IN NUMBER  --–Выходные данные, число
          ,v_OUTCHAR IN VARCHAR2  --Выходные данные, строка (если null, то дублирует первый параметр - number)
          ,v_OUTARRAY IN VARCHAR2 --Выходной данные для расширения (массив данных, структуру проработаем позже)
          ,v_E_CODE IN OUT NUMBER  --Код ошибки (внутренний код ошибки объекта, если >0 то значит возникла проблема при работе) 
          ,v_E_TEXT IN OUT VARCHAR2 --Описание ошибки (детальное описание ошибки, если ErrorCode>0)
        ) AS 
    pragma autonomous_transaction;
  BEGIN
    --логируем вызов процедуры проверки фрод правила
    MERGE INTO AFS.AFS_LOG_CALL tar
      USING (SELECT 1 FROM DUAL) t_new
    ON (tar.PROC_ID=v_PROC_ID)
    ----ОБНОВЛЯЕТСЯ ТОЛЬКО ЕСЛИ ЧТО ТО ИЗМЕНИЛОСЬ
    WHEN MATCHED THEN UPDATE SET
        tar.FRAUD_ID=v_FRAUD_ID
        ,tar.FRAUD_RULE_NM=v_FRAUD_NAME
        ,tar.REQUEST_ID=v_REQUEST_ID
        ,tar.PERSON_ID=v_PERSON_ID
        ,tar.OUTCODE=v_OUTCODE
        ,tar.INARRAY=v_INARRAY
        ,tar.OUTNUMBER=v_OUTNUMBER
        ,tar.OUTCHAR=v_OUTCHAR
        ,tar.OUTARRAY=v_OUTARRAY
        ,tar.ERRORCODE=v_E_CODE
        ,tar.ERRORTEXT=v_E_TEXT
    WHERE NOT (
          NVL(tar.FRAUD_ID, -1)=NVL(v_FRAUD_ID, -1)
      AND NVL(tar.FRAUD_RULE_NM, '-')=NVL(v_FRAUD_NAME, '-')
      AND NVL(tar.REQUEST_ID, -1)=NVL(v_REQUEST_ID, -1)
      AND NVL(tar.PERSON_ID, -1)=NVL(v_PERSON_ID, -1)
      AND NVL(tar.INARRAY, '-')=NVL(v_INARRAY, '-')
      AND NVL(tar.OUTNUMBER, -1)=NVL(v_OUTNUMBER, -1)
      AND NVL(tar.OUTCHAR, '-')=NVL(v_OUTCHAR, '-')
      AND NVL(tar.OUTARRAY, '-')=NVL(v_OUTARRAY, '-')
      AND NVL(tar.ERRORCODE, -1)=NVL(v_E_CODE, -1)
      AND NVL(tar.ERRORTEXT, '-')=NVL(v_E_TEXT, '-')
        )
    WHEN NOT MATCHED THEN 
      --вставляем новое
      INSERT (tar.PROC_ID, tar.FRAUD_ID, tar.FRAUD_RULE_NM
              ,tar.REQUEST_ID,tar.PERSON_ID
              ,tar.OUTCODE,tar.INARRAY
              ,tar.OUTNUMBER,tar.OUTCHAR,tar.OUTARRAY
              ,tar.ERRORCODE,tar.ERRORTEXT)
      VALUES (v_PROC_ID, v_FRAUD_ID, v_FRAUD_NAME
              , v_REQUEST_ID, v_PERSON_ID
              , v_OUTCODE, v_INARRAY
              , v_OUTNUMBER, v_OUTCHAR, v_OUTARRAY
              , v_E_CODE, v_E_TEXT)
    ;
    COMMIT;
  EXCEPTION WHEN OTHERS 
    THEN 
      INSERT INTO AFS.AFS_LOG_CALL (PROC_ID, FRAUD_ID, FRAUD_RULE_NM
              ,REQUEST_ID,PERSON_ID
              ,OUTCODE,INARRAY
              ,OUTNUMBER,OUTCHAR,OUTARRAY
              ,ERRORCODE, ERRORTEXT)
      VALUES (v_PROC_ID, v_FRAUD_ID, v_FRAUD_NAME
              , v_REQUEST_ID, v_PERSON_ID
              , v_OUTCODE, v_INARRAY
              , v_OUTNUMBER, v_OUTCHAR, v_OUTARRAY
              , v_E_CODE, v_E_TEXT);
    v_E_CODE := 301;
    v_E_TEXT := substr('Ошибка при записи в AFS.AFS_LOG_CALL. '||TO_CHAR(SQLCODE)||' | '||SQLERRM, 0, 4000);
  END WRITE_LOG_CALL;

  PROCEDURE WRITE_AFS_HISTORY(
          v_PROC_ID IN NUMBER     --уникальный ID процедуры 
          ,v_REQUEST_ID IN NUMBER --ID заявки ПКК
          ,v_PERSON_ID IN NUMBER --ID физика ПКК
          ,v_REQUEST_DATE IN DATE --дата заявки в ПКК
          ,v_FRAUD_ID IN NUMBER  --внутренний ID правила по справочнику
          ,v_FRAUD_OUTNUMBER IN NUMBER --Выходные данные по проверке фрод правила, число
          ,v_FIO IN VARCHAR2           --ФИО на тек. момент по заявке. Если пустое, то можно подтянуть после
          ,v_DR IN VARCHAR2            --ДР  на тек. момент по заявке. Если пустое, то можно подтянуть после
          ,v_DAY_BETWEEN IN NUMBER     --Дней между параметрами. Расчетный параметр
          ,v_REQUEST_ID_REL IN NUMBER  --ID обнаруженной по правилу заявки ПКК
          ,v_PERSON_ID_REL IN NUMBER   --ID обнаруженного по правилу физика ПКК
          ,v_REQUEST_DATE_REL IN DATE  --дата обнаруженной по правилу заявки в ПКК
          ,v_FIO_REL IN VARCHAR2       --ФИО на тек. момент по обнаруженной заявке. Если пустое, то можно подтянуть потом
          ,v_DR_REL IN VARCHAR2        --ДР  на тек. момент по обнаруженной заявке. Если пустое, то можно подтянуть потом
          ,v_INFO_EQUAL IN	VARCHAR2         --Информация которая провери
          ,v_INFO_NOT_EQUAL IN	VARCHAR2
          ,v_INFO_NOT_EQUAL_REL IN VARCHAR2
          ,v_E_CODE IN OUT NUMBER  --Код ошибки (внутренний код ошибки объекта, если >0 то значит возникла проблема при работе) 
          ,v_E_TEXT IN OUT VARCHAR2 --Описание ошибки (детальное описание ошибки, если ErrorCode>0)
          ) AS
    pragma autonomous_transaction;
  BEGIN
    --логируем подробности сработавшего результата правила
    MERGE INTO AFS.AFS_HISTORY tar
      USING (SELECT 1 FROM DUAL) t_new
    ON (tar.PROC_ID=v_PROC_ID AND tar.REQUEST_ID_REL=v_REQUEST_ID_REL)
    WHEN MATCHED THEN UPDATE SET
        tar.REQUEST_ID=v_REQUEST_ID
        ,tar.PERSON_ID=v_PERSON_ID
        ,tar.REQUEST_DATE=v_REQUEST_DATE
        ,tar.FRAUD_ID=v_FRAUD_ID
        ,tar.FRAUD_OUTNUMBER=v_FRAUD_OUTNUMBER
        ,tar.FIO=v_FIO
        ,tar.DR=v_DR
        ,tar.DAY_BETWEEN=v_DAY_BETWEEN
        --,tar.REQUEST_ID_REL=v_REQUEST_ID_REL
        ,tar.PERSON_ID_REL=v_PERSON_ID_REL
        ,tar.REQUEST_DATE_REL=v_REQUEST_DATE_REL
        ,tar.FIO_REL=v_FIO_REL
        ,tar.DR_REL=v_DR_REL
        ,tar.INFO_EQUAL=v_INFO_EQUAL
        ,tar.INFO_NOT_EQUAL=v_INFO_NOT_EQUAL
        ,tar.INFO_NOT_EQUAL_REL=v_INFO_NOT_EQUAL_REL
    /*WHERE NOT (--ОБНОВЛЯЕТСЯ ТОЛЬКО ЕСЛИ ЧТО ТО ИЗМЕНИЛОСЬ!!
          NVL(tar.REQUEST_ID, -1)=NVL(v_REQUEST_ID, -1)
        AND NVL(tar.PERSON_ID, -1)=NVL(v_PERSON_ID, -1)
        AND NVL(tar.FRAUD_ID, -1)=NVL(v_FRAUD_ID, -1)
        AND NVL(tar.FRAUD_OUTNUMBER, -1)=NVL(v_FRAUD_OUTNUMBER , -1)
        AND NVL(tar.FIO, '-')=NVL(v_FIO , '-')
        AND NVL(tar.DR, '-')=NVL(v_DR , '-')
        AND NVL(tar.DAY_BETWEEN, -1)=NVL(v_DAY_BETWEEN , -1)
        AND NVL(tar.REQUEST_ID_REL, -1)=NVL(v_REQUEST_ID_REL , -1)
        AND NVL(tar.PERSON_ID_REL, -1)=NVL(v_PERSON_ID_REL , -1)
        AND NVL(tar.REQUEST_DATE_REL, TO_DATE('01-01-1900', 'dd-mm-yyyy'))=NVL(v_REQUEST_DATE_REL , TO_DATE('01-01-1900', 'dd-mm-yyyy'))
        AND NVL(tar.FIO_REL, '-')=NVL(v_FIO_REL , '-')
        AND NVL(tar.DR_REL, '-')=NVL(v_DR_REL , '-')
        AND NVL(tar.INFO_EQUAL, '-')=NVL(v_INFO_EQUAL , '-')
        AND NVL(tar.INFO_NOT_EQUAL, '-')=NVL(v_INFO_NOT_EQUAL , '-')
        AND NVL(tar.INFO_NOT_EQUAL_REL, '-')=NVL(v_INFO_NOT_EQUAL_REL , '-')
        )*/
    WHEN NOT MATCHED THEN 
    --вставляем новое
    INSERT (tar.PROC_ID, tar.REQUEST_ID, tar.PERSON_ID, tar.REQUEST_DATE
            , tar.FRAUD_ID, tar.FRAUD_OUTNUMBER
            , tar.FIO, tar.DR, tar.DAY_BETWEEN
            , tar.REQUEST_ID_REL, tar.PERSON_ID_REL, tar.REQUEST_DATE_REL, tar.FIO_REL,tar.DR_REL
            ,tar.INFO_EQUAL, tar.INFO_NOT_EQUAL, tar.INFO_NOT_EQUAL_REL)
      VALUES (v_PROC_ID 
            ,v_REQUEST_ID, v_PERSON_ID, v_REQUEST_DATE
            ,v_FRAUD_ID, v_FRAUD_OUTNUMBER
            ,v_FIO, v_DR, v_DAY_BETWEEN
            ,v_REQUEST_ID_REL, v_PERSON_ID_REL, v_REQUEST_DATE_REL, v_FIO_REL,v_DR_REL
            ,v_INFO_EQUAL, v_INFO_NOT_EQUAL, v_INFO_NOT_EQUAL_REL )
    ;
    
    COMMIT;
  
  EXCEPTION WHEN OTHERS 
    THEN 
      INSERT INTO AFS.AFS_HISTORY (PROC_ID, REQUEST_ID, PERSON_ID, REQUEST_DATE
            , FRAUD_ID, FRAUD_OUTNUMBER
            , FIO, DR, DAY_BETWEEN
            , REQUEST_ID_REL, PERSON_ID_REL, REQUEST_DATE_REL, FIO_REL,DR_REL
            ,INFO_EQUAL, INFO_NOT_EQUAL, INFO_NOT_EQUAL_REL)
      VALUES (v_PROC_ID 
            ,v_REQUEST_ID, v_PERSON_ID, v_REQUEST_DATE
            ,v_FRAUD_ID, v_FRAUD_OUTNUMBER
            ,v_FIO, v_DR, v_DAY_BETWEEN
            ,v_REQUEST_ID_REL, v_PERSON_ID_REL, v_REQUEST_DATE_REL, v_FIO_REL,v_DR_REL
            ,v_INFO_EQUAL, v_INFO_NOT_EQUAL, v_INFO_NOT_EQUAL_REL )
    ;
    v_E_CODE := 302;
    v_E_TEXT := substr('Ошибка при записи в AFS.AFS_LOG_CALL. '||TO_CHAR(SQLCODE)||' | '||SQLERRM, 0, 4000);
  END WRITE_AFS_HISTORY;

END AFS_SYS;

/
--------------------------------------------------------
--  DDL for Procedure ROUTE_FRAUD_RULE_BY_ID
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "AFS"."ROUTE_FRAUD_RULE_BY_ID" (v_Rule_Code IN NUMBER
                          ,v_Request_ID IN NUMBER, v_Person_ID IN NUMBER
                          ,v_OutCode IN VARCHAR2, v_InArray IN VARCHAR2
                          ,v_OutNumber OUT NUMBER, v_OutChar OUT VARCHAR2
                          ,v_OutArray OUT VARCHAR2
                          ,v_ErrorCode OUT NUMBER, v_ErrorText OUT VARCHAR2                  
                    ) AS
/*      v_Rule_Code IN NUMBER     - входной параметр с номером правила, по которому нужно проверить заявку
        v_Request_ID IN NUMBER    - !входной параметр REQUEST_ID заявки из ПКК
        v_Person_ID IN NUMBER     - входной параметр PERSON_ID заявки из ПКК. 
        v_OutCode IN VARCHAR2     - Код внешней системы (для синхронизации между системами)
        v_InArray IN VARCHAR2     - входной параметр для расширения (массив данных)
 
        v_OutNumber OUT NUMBER    - !Выходные данные (результат по фрод-правилу)
                                      0   - фрода нет
                                      1   - фрод есть
                                      -1  - ошибка
                                      9  - нет информации. Т.к. будут использоваться данные ПКК, то считаю код неактуальным
        v_OutChar IN OUT VARCHAR2 - строковый эквивалент возвращаемого значения
        v_OutArray OUT VARCHAR2   - выходной параметр для расширения (массив данных)
        v_ErrorCode OUT NUMBER    - Код ошибки (внутренний код ошибки объекта, если более Нуля то значит возникла проблема при работе)
        v_ErrorText OUT VARCHAR2  - Описание ошибки (детальное описание ошибки)
*/
  -- !!!!
  -- Резервируем под данную процедуру код ошибки 300 (3хх)
  -- !!!!

  p_PROC_ID NUMBER            := -1; --уникальный ID процесса
  p_FRAUD_ID NUMBER           := -1;   --ID правила. Определяется по справочнику D_FRAUD_RULE
  p_FRAUD_NAME VARCHAR2(255)  := null;   --ID правила. Определяется по справочнику D_FRAUD_RULE
  
  p_SqlStr VARCHAR2(4000)     :=null; -- Динамическая строка запроса
  
  p_ErrorCode_Sys NUMBER;          --Системный код ошибки. В выходной параметр может не передаваться если не существенен
  p_ErrorText_Sys VARCHAR2(4000);  --Системный текст ошибки. В выходной параметр может не передаваться если не существенен

  ex_no_obj_faund exception;
  pragma exception_init(ex_no_obj_faund, -06550);

BEGIN 
  --задаем первичные выходные параметры
  v_OutNumber := 0;
  v_OutChar := TO_CHAR(v_OutNumber);
  v_OutArray:=null;
  v_ErrorCode:=0;
  v_ErrorText:=null;

  -- определяем RULE
	BEGIN
  SELECT AFS.AFS_PROC_SEQ.NEXTVAL INTO p_PROC_ID FROM DUAL;
  -- Вход
	AFS.AFS_SYS.WRITE_LOG_CALL(v_PROC_ID => p_PROC_ID
                          , v_FRAUD_ID => p_FRAUD_ID, v_FRAUD_NAME => p_FRAUD_NAME
                          , v_REQUEST_ID => v_Request_ID, v_PERSON_ID => v_Person_ID
                          , v_OUTCODE => v_OutCode
                          , v_INARRAY => v_InArray
                          , v_OUTNUMBER => v_OutNumber, v_OUTCHAR => v_OutChar
                          , v_OUTARRAY => v_OutArray
                          , v_E_CODE => v_ErrorCode, v_E_TEXT => v_ErrorText
      );

    SELECT fr.FRAUD_RULE_ID, fr.FRAUD_RULE_NM 
      INTO p_FRAUD_ID, p_FRAUD_NAME 
      FROM AFS.D_FRAUD_RULE fr
      WHERE FRAUD_RULE_ID=v_Rule_Code and rownum<=1 
        AND RULE_AKT=1
      --and rule_status='OK'
      ; 
  EXCEPTION 
    WHEN no_data_found 
                      THEN v_ErrorCode:=301; v_ErrorText:='Правило не найдено'; p_FRAUD_ID:= v_Rule_Code;
        v_OutNumber := -1;
        v_OutChar := TO_CHAR(v_OutNumber);
    WHEN others       THEN v_ErrorCode:=302; v_ErrorText:=substr(TO_CHAR(SQLCODE)||' | '||SQLERRM,0,4000); 
        p_FRAUD_ID:= v_Rule_Code;
        v_OutNumber := -1;
        v_OutChar := TO_CHAR(v_OutNumber);
  END;
    
	IF p_PROC_ID>0 and p_FRAUD_ID>=0 and v_ErrorCode<=0 THEN
    -- выполняем правило
		BEGIN
      p_SqlStr := 'BEGIN '||p_FRAUD_NAME||'(:1,:2,:3,:4,:5,:6,:7,:8,:9,:10); END;';
      EXECUTE IMMEDIATE p_SqlStr USING  
        IN OUT p_PROC_ID
        ,IN v_Request_ID 
        ,IN v_Person_ID 
        ,IN v_OutCode 
        ,IN v_InArray        
        ,OUT v_OutNumber
        ,OUT v_OutChar
        ,OUT v_OutArray
        ,OUT v_ErrorCode
        ,OUT v_ErrorText 
		  ;

    	EXCEPTION WHEN ex_no_obj_faund 
                      THEN v_ErrorCode:=303; v_ErrorText:=substr(TO_CHAR(SQLCODE)||' | '||SQLERRM,0,4000); p_FRAUD_ID:= v_Rule_Code;
        v_OutNumber := -1;
        v_OutChar := TO_CHAR(v_OutNumber);
      WHEN others 
                      THEN v_ErrorCode:=304; v_ErrorText:=substr(TO_CHAR(SQLCODE)||' | '||SQLERRM,0,4000);
        v_OutNumber := -1;
        v_OutChar := TO_CHAR(v_OutNumber);
    END;

	END IF;

  -- Выход
	AFS.AFS_SYS.WRITE_LOG_CALL(v_PROC_ID => p_PROC_ID
                          , v_FRAUD_ID => p_FRAUD_ID, v_FRAUD_NAME => p_FRAUD_NAME
                          , v_REQUEST_ID => v_Request_ID, v_PERSON_ID => v_Person_ID
                          , v_OUTCODE => v_OutCode
                          , v_INARRAY => v_InArray
                          , v_OUTNUMBER => v_OutNumber, v_OUTCHAR => v_OutChar
                          , v_OUTARRAY => v_OutArray
                          , v_E_CODE => v_ErrorCode, v_E_TEXT => v_ErrorText
      );

  -- Обработка общей ошибки
  EXCEPTION WHEN OTHERS  
                      THEN v_ErrorCode:=300; v_ErrorText:=substr(TO_CHAR(SQLCODE)||' | '||SQLERRM,0,4000);
    v_OutNumber := -1;
    v_OutChar := TO_CHAR(v_OutNumber);
    
    AFS.AFS_SYS.WRITE_LOG_CALL(v_PROC_ID => p_PROC_ID
                            , v_FRAUD_ID => p_FRAUD_ID
                            , v_FRAUD_NAME => p_FRAUD_NAME
                            , v_REQUEST_ID => v_Request_ID
                            , v_PERSON_ID => v_Person_ID
                            , v_OUTCODE => v_OutCode
                            , v_INARRAY => v_InArray
                            , v_OUTNUMBER => v_OutNumber
                            , v_OUTCHAR => v_OutChar
                            , v_OUTARRAY => v_OutArray
                            , v_E_CODE => v_ErrorCode
                            , v_E_TEXT => v_ErrorText
      );
END;

/
--------------------------------------------------------
--  DDL for Procedure ROUTE_FRAUD_RULE_BY_ID_TST
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "AFS"."ROUTE_FRAUD_RULE_BY_ID_TST" (v_Rule_Code IN NUMBER
                          ,v_Request_ID IN NUMBER, v_Person_ID IN NUMBER
                          ,v_OutCode IN VARCHAR2, v_InArray IN VARCHAR2
                          ,v_OutNumber OUT NUMBER, v_OutChar OUT VARCHAR2
                          ,v_OutArray OUT VARCHAR2
                          ,v_ErrorCode OUT NUMBER, v_ErrorText OUT VARCHAR2                  
                    ) AS
-- Отличие тестовой от основной - возможность вызывать неактуальные правила и любые FRAUD_ID
/*      v_Rule_Code IN NUMBER     - входной параметр с номером правила, по которому нужно проверить заявку
        v_Request_ID IN NUMBER    - !входной параметр REQUEST_ID заявки из ПКК
        v_Person_ID IN NUMBER     - входной параметр PERSON_ID заявки из ПКК. 
        v_OutCode IN VARCHAR2     - Код внешней системы (для синхронизации между системами)
        v_InArray IN VARCHAR2     - входной параметр для расширения (массив данных)
 
        v_OutNumber OUT NUMBER    - !Выходные данные (результат по фрод-правилу)
                                      0   - фрода нет
                                      1   - фрод есть
                                      -1  - ошибка
                                      9  - нет информации. Т.к. будут использоваться данные ПКК, то считаю код неактуальным
        v_OutChar IN OUT VARCHAR2 - строковый эквивалент возвращаемого значения
        v_OutArray OUT VARCHAR2   - выходной параметр для расширения (массив данных)
        v_ErrorCode OUT NUMBER    - Код ошибки (внутренний код ошибки объекта, если более Нуля то значит возникла проблема при работе)
        v_ErrorText OUT VARCHAR2  - Описание ошибки (детальное описание ошибки)
*/
  -- !!!!
  -- Резервируем под данную процедуру код ошибки 300 (3хх)
  -- !!!!

  p_PROC_ID NUMBER            := -1; --уникальный ID процесса
  p_FRAUD_ID NUMBER           := -1;   --ID правила. Определяется по справочнику D_FRAUD_RULE
  p_FRAUD_NAME VARCHAR2(255)  := null;   --ID правила. Определяется по справочнику D_FRAUD_RULE
  
  p_SqlStr VARCHAR2(4000)     :=null; -- Динамическая строка запроса
  
  p_ErrorCode_Sys NUMBER;          --Системный код ошибки. В выходной параметр может не передаваться если не существенен
  p_ErrorText_Sys VARCHAR2(4000);  --Системный текст ошибки. В выходной параметр может не передаваться если не существенен

  ex_no_obj_faund exception;
  pragma exception_init(ex_no_obj_faund, -06550);

BEGIN 
  --задаем первичные выходные параметры
  v_OutNumber := 0;
  v_OutChar := TO_CHAR(v_OutNumber);
  v_OutArray:=null;
  v_ErrorCode:=0;
  v_ErrorText:=null;

  -- определяем RULE
	BEGIN
  SELECT AFS.AFS_PROC_SEQ.NEXTVAL INTO p_PROC_ID FROM DUAL;
  -- Вход
	AFS.AFS_SYS.WRITE_LOG_CALL(v_PROC_ID => p_PROC_ID
                          , v_FRAUD_ID => p_FRAUD_ID, v_FRAUD_NAME => p_FRAUD_NAME
                          , v_REQUEST_ID => v_Request_ID, v_PERSON_ID => v_Person_ID
                          , v_OUTCODE => v_OutCode
                          , v_INARRAY => v_InArray
                          , v_OUTNUMBER => v_OutNumber, v_OUTCHAR => v_OutChar
                          , v_OUTARRAY => v_OutArray
                          , v_E_CODE => v_ErrorCode, v_E_TEXT => v_ErrorText
      );

    SELECT fr.FRAUD_RULE_ID, fr.FRAUD_RULE_NM 
      INTO p_FRAUD_ID, p_FRAUD_NAME 
      FROM AFS.D_FRAUD_RULE fr
      WHERE FRAUD_RULE_ID=v_Rule_Code and rownum<=1 
        --AND RULE_AKT=1
      --and rule_status='OK'
      ; 
  EXCEPTION 
    WHEN no_data_found 
                      THEN v_ErrorCode:=301; v_ErrorText:='Правило не найдено'; p_FRAUD_ID:= v_Rule_Code;
        v_OutNumber := -1;
        v_OutChar := TO_CHAR(v_OutNumber);
    WHEN others       THEN v_ErrorCode:=302; v_ErrorText:=substr(TO_CHAR(SQLCODE)||' | '||SQLERRM,0,4000); 
        p_FRAUD_ID:= v_Rule_Code;
        v_OutNumber := -1;
        v_OutChar := TO_CHAR(v_OutNumber);
  END;
    
	IF p_PROC_ID>0 /*and p_FRAUD_ID>=0*/ and v_ErrorCode<=0 THEN
    -- выполняем правило
		BEGIN
      p_SqlStr := 'BEGIN '||p_FRAUD_NAME||'(:1,:2,:3,:4,:5,:6,:7,:8,:9,:10); END;';
      EXECUTE IMMEDIATE p_SqlStr USING  
        IN OUT p_PROC_ID
        ,IN v_Request_ID 
        ,IN v_Person_ID 
        ,IN v_OutCode 
        ,IN v_InArray        
        ,OUT v_OutNumber
        ,OUT v_OutChar
        ,OUT v_OutArray
        ,OUT v_ErrorCode
        ,OUT v_ErrorText 
		  ;

    	EXCEPTION WHEN ex_no_obj_faund 
                      THEN v_ErrorCode:=303; v_ErrorText:=substr(TO_CHAR(SQLCODE)||' | '||SQLERRM,0,4000); p_FRAUD_ID:= v_Rule_Code;
        v_OutNumber := -1;
        v_OutChar := TO_CHAR(v_OutNumber);
      WHEN others 
                      THEN v_ErrorCode:=304; v_ErrorText:=substr(TO_CHAR(SQLCODE)||' | '||SQLERRM,0,4000);
        v_OutNumber := -1;
        v_OutChar := TO_CHAR(v_OutNumber);
    END;

	END IF;

  -- Выход
	AFS.AFS_SYS.WRITE_LOG_CALL(v_PROC_ID => p_PROC_ID
                          , v_FRAUD_ID => p_FRAUD_ID, v_FRAUD_NAME => p_FRAUD_NAME
                          , v_REQUEST_ID => v_Request_ID, v_PERSON_ID => v_Person_ID
                          , v_OUTCODE => v_OutCode
                          , v_INARRAY => v_InArray
                          , v_OUTNUMBER => v_OutNumber, v_OUTCHAR => v_OutChar
                          , v_OUTARRAY => v_OutArray
                          , v_E_CODE => v_ErrorCode, v_E_TEXT => v_ErrorText
      );

  -- Обработка общей ошибки
  EXCEPTION WHEN OTHERS  
                      THEN v_ErrorCode:=300; v_ErrorText:=substr(TO_CHAR(SQLCODE)||' | '||SQLERRM,0,4000);
    v_OutNumber := -1;
    v_OutChar := TO_CHAR(v_OutNumber);
    
    AFS.AFS_SYS.WRITE_LOG_CALL(v_PROC_ID => p_PROC_ID
                            , v_FRAUD_ID => p_FRAUD_ID
                            , v_FRAUD_NAME => p_FRAUD_NAME
                            , v_REQUEST_ID => v_Request_ID
                            , v_PERSON_ID => v_Person_ID
                            , v_OUTCODE => v_OutCode
                            , v_INARRAY => v_InArray
                            , v_OUTNUMBER => v_OutNumber
                            , v_OUTCHAR => v_OutChar
                            , v_OUTARRAY => v_OutArray
                            , v_E_CODE => v_ErrorCode
                            , v_E_TEXT => v_ErrorText
      );
END;

/
--------------------------------------------------------
--  DDL for Procedure ROUTE_FRAUD_RULE_OFFLINE
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "AFS"."ROUTE_FRAUD_RULE_OFFLINE" (
      v_Rule_Code IN VARCHAR2
      ,v_Request_ID IN NUMBER 
      ,v_Person_ID IN NUMBER 
      ,v_OutCode IN VARCHAR2 
      ,v_InArray IN VARCHAR2
      ,v_OutNumber OUT NUMBER
      ,v_OutChar OUT VARCHAR2
      ,v_OutArray OUT VARCHAR2
      ,v_ErrorCode OUT NUMBER
      ,v_ErrorText OUT VARCHAR2                  
      ) 
AS
/*      v_Rule_Code IN NUMBER     - входной параметр с номером правила, по которому нужно проверить заявку
        v_Request_ID IN NUMBER    - !входной параметр REQUEST_ID заявки из ПКК
        v_Person_ID IN NUMBER     - входной параметр PERSON_ID заявки из ПКК. 
        v_OutCode IN VARCHAR2     - Код внешней системы (для синхронизации между системами)
        v_InArray IN VARCHAR2     - входной параметр для расширения (массив данных)
 
        v_OutNumber OUT NUMBER    - !Выходные данные (результат по фрод-правилу)
                                      0   - фрода нет
                                      1   - фрод есть
                                      -1  - ошибка
                                      9  - нет информации. Т.к. будут использоваться данные ПКК, то считаю код неактуальным
        v_OutChar IN OUT VARCHAR2 - строковый эквивалент возвращаемого значения
        v_OutArray OUT VARCHAR2   - выходной параметр для расширения (массив данных)
        v_ErrorCode OUT NUMBER    - Код ошибки (внутренний код ошибки объекта, если более Нуля то значит возникла проблема при работе)
        v_ErrorText OUT VARCHAR2  - Описание ошибки (детальное описание ошибки)
*/ 
  -- !!!!
  -- Резервируем под данную процедуру код ошибки 300 (3хх)
  -- !!!!

  p_PROC_ID NUMBER            := -1; --уникальный ID процесса
  p_FRAUD_ID NUMBER           := -1;   --ID правила. Определяется по справочнику D_FRAUD_RULE
  p_FRAUD_NAME VARCHAR2(255)  := null;   --ID правила. Определяется по справочнику D_FRAUD_RULE
  
  p_SqlStr VARCHAR2(4000)     :=null; -- Динамическая строка запроса
  
  p_ErrorCode_Sys NUMBER;          --Системный код ошибки. В выходной параметр может не передаваться если не существенен
  p_ErrorText_Sys VARCHAR2(4000);  --Системный текст ошибки. В выходной параметр может не передаваться если не существенен

  ex_no_obj_faund exception;
  pragma exception_init(ex_no_obj_faund, -06550);

BEGIN 
  --задаем первичные выходные параметры
  v_OutNumber := 0;
  v_OutChar := TO_CHAR(v_OutNumber);
  v_OutArray:=null;
  v_ErrorCode:=0;
  v_ErrorText:=null;

  -- определяем RULE
	BEGIN
  SELECT AFS.AFS_PROC_SEQ.NEXTVAL INTO p_PROC_ID FROM DUAL;
  -- Вход
	AFS.AFS_SYS.WRITE_LOG_CALL(v_PROC_ID => p_PROC_ID
                          , v_FRAUD_ID => p_FRAUD_ID, v_FRAUD_NAME => p_FRAUD_NAME
                          , v_REQUEST_ID => v_Request_ID, v_PERSON_ID => v_Person_ID
                          , v_OUTCODE => v_OutCode
                          , v_INARRAY => v_InArray
                          , v_OUTNUMBER => v_OutNumber, v_OUTCHAR => v_OutChar
                          , v_OUTARRAY => v_OutArray
                          , v_E_CODE => v_ErrorCode, v_E_TEXT => v_ErrorText
      );

    /*SELECT fr.FRAUD_RULE_ID, fr.FRAUD_RULE_NM 
      INTO p_FRAUD_ID, p_FRAUD_NAME 
      FROM AFS.D_FRAUD_RULE fr
      WHERE FRAUD_RULE_ID=v_Rule_Code and rownum<=1 
        AND RULE_AKT=1 --and rule_status='OK'
      ; */
      
    SELECT fr.FRAUD_RULE_ID, fr.FRAUD_RULE_NM 
      INTO p_FRAUD_ID, p_FRAUD_NAME 
      FROM AFS.D_FRAUD_RULE fr
      WHERE RULE_CODE=v_Rule_Code and rownum<=1 
        AND RULE_AKT=1
        ;
  EXCEPTION 
    WHEN no_data_found 
                      THEN v_ErrorCode:=301; v_ErrorText:='Правило не найдено'; 
        --p_FRAUD_ID:= v_Rule_Code;
        v_OutNumber := -1;
        v_OutChar := TO_CHAR(v_OutNumber);
    WHEN others       THEN v_ErrorCode:=302; v_ErrorText:=substr(TO_CHAR(SQLCODE)||' | '||SQLERRM,0,4000); 
        --p_FRAUD_ID:= v_Rule_Code;
        v_OutNumber := -1;
        v_OutChar := TO_CHAR(v_OutNumber);
  END;
    
	IF p_PROC_ID>0 and p_FRAUD_ID>0 and v_ErrorCode<=0 THEN
    -- выполняем правило
		BEGIN
      p_SqlStr := 'BEGIN '||p_FRAUD_NAME||'(:1,:2,:3,:4,:5,:6,:7,:8,:9,:10); END;';
      EXECUTE IMMEDIATE p_SqlStr USING  
        IN OUT p_PROC_ID
        ,IN v_Request_ID 
        ,IN v_Person_ID 
        ,IN v_OutCode 
        ,IN v_InArray        
        ,OUT v_OutNumber
        ,OUT v_OutChar
        ,OUT v_OutArray
        ,OUT v_ErrorCode
        ,OUT v_ErrorText 
		  ;

    	EXCEPTION WHEN ex_no_obj_faund 
                      THEN v_ErrorCode:=303; v_ErrorText:=substr(TO_CHAR(SQLCODE)||' | '||SQLERRM,0,4000); 
        --p_FRAUD_ID:= v_Rule_Code;
        v_OutNumber := -1;
        v_OutChar := TO_CHAR(v_OutNumber);
      WHEN others 
                      THEN v_ErrorCode:=304; v_ErrorText:=substr(TO_CHAR(SQLCODE)||' | '||SQLERRM,0,4000);
        v_OutNumber := -1;
        v_OutChar := TO_CHAR(v_OutNumber);
    END;

	END IF;

  -- Выход
	AFS.AFS_SYS.WRITE_LOG_CALL(v_PROC_ID => p_PROC_ID
                          , v_FRAUD_ID => p_FRAUD_ID, v_FRAUD_NAME => p_FRAUD_NAME
                          , v_REQUEST_ID => v_Request_ID, v_PERSON_ID => v_Person_ID
                          , v_OUTCODE => v_OutCode
                          , v_INARRAY => v_InArray
                          , v_OUTNUMBER => v_OutNumber, v_OUTCHAR => v_OutChar
                          , v_OUTARRAY => v_OutArray
                          , v_E_CODE => v_ErrorCode, v_E_TEXT => v_ErrorText
      );

  -- Обработка общей ошибки
  EXCEPTION WHEN OTHERS  
                      THEN v_ErrorCode:=300; v_ErrorText:=substr(TO_CHAR(SQLCODE)||' | '||SQLERRM,0,4000);
    v_OutNumber := -1;
    v_OutChar := TO_CHAR(v_OutNumber);
    
    AFS.AFS_SYS.WRITE_LOG_CALL(v_PROC_ID => p_PROC_ID
                            , v_FRAUD_ID => p_FRAUD_ID
                            , v_FRAUD_NAME => p_FRAUD_NAME
                            , v_REQUEST_ID => v_Request_ID
                            , v_PERSON_ID => v_Person_ID
                            , v_OUTCODE => v_OutCode
                            , v_INARRAY => v_InArray
                            , v_OUTNUMBER => v_OutNumber
                            , v_OUTCHAR => v_OutChar
                            , v_OUTARRAY => v_OutArray
                            , v_E_CODE => v_ErrorCode
                            , v_E_TEXT => v_ErrorText
      );
END;

/

  GRANT EXECUTE ON "AFS"."ROUTE_FRAUD_RULE_OFFLINE" TO "L_KREDIT_SCORING";
  GRANT EXECUTE ON "AFS"."ROUTE_FRAUD_RULE_OFFLINE" TO "MVKARELIN";
--------------------------------------------------------
--  DDL for Procedure ROUTE_FRAUD_RULE_ONLINE
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "AFS"."ROUTE_FRAUD_RULE_ONLINE" (
      v_Rule_Code IN VARCHAR2
      ,v_Request_ID IN NUMBER 
      ,v_Person_ID IN NUMBER 
      ,v_OutCode IN VARCHAR2 
      ,v_InArray IN VARCHAR2
      ,v_OutNumber OUT NUMBER
      ,v_OutChar OUT VARCHAR2
      ,v_OutArray OUT VARCHAR2
      ,v_ErrorCode OUT NUMBER
      ,v_ErrorText OUT VARCHAR2                  
      ) 
AS
/*      v_Rule_Code IN NUMBER     - входной параметр с номером правила, по которому нужно проверить заявку
        v_Request_ID IN NUMBER    - !входной параметр REQUEST_ID заявки из ПКК
        v_Person_ID IN NUMBER     - входной параметр PERSON_ID заявки из ПКК. 
        v_OutCode IN VARCHAR2     - Код внешней системы (для синхронизации между системами)
        v_InArray IN VARCHAR2     - входной параметр для расширения (массив данных)
 
        v_OutNumber OUT NUMBER    - !Выходные данные (результат по фрод-правилу)
                                      0   - фрода нет
                                      1   - фрод есть
                                      -1  - ошибка
                                      9  - нет информации. Т.к. будут использоваться данные ПКК, то считаю код неактуальным
        v_OutChar IN OUT VARCHAR2 - строковый эквивалент возвращаемого значения
        v_OutArray OUT VARCHAR2   - выходной параметр для расширения (массив данных)
        v_ErrorCode OUT NUMBER    - Код ошибки (внутренний код ошибки объекта, если более Нуля то значит возникла проблема при работе)
        v_ErrorText OUT VARCHAR2  - Описание ошибки (детальное описание ошибки)
*/ 
  -- !!!!
  -- Резервируем под данную процедуру код ошибки 300 (3хх)
  -- !!!!

  p_PROC_ID NUMBER            := -1; --уникальный ID процесса
  p_FRAUD_ID NUMBER           := -1;   --ID правила. Определяется по справочнику D_FRAUD_RULE
  p_FRAUD_NAME VARCHAR2(255)  := null;   --ID правила. Определяется по справочнику D_FRAUD_RULE
  
  p_SqlStr VARCHAR2(4000)     :=null; -- Динамическая строка запроса
  
  p_ErrorCode_Sys NUMBER;          --Системный код ошибки. В выходной параметр может не передаваться если не существенен
  p_ErrorText_Sys VARCHAR2(4000);  --Системный текст ошибки. В выходной параметр может не передаваться если не существенен

  ex_no_obj_faund exception;
  pragma exception_init(ex_no_obj_faund, -06550);

BEGIN 
  --задаем первичные выходные параметры
  v_OutNumber := 0;
  v_OutChar := TO_CHAR(v_OutNumber);
  v_OutArray:=null;
  v_ErrorCode:=0;
  v_ErrorText:=null;

  -- определяем RULE
	BEGIN
  SELECT AFS.AFS_PROC_SEQ.NEXTVAL INTO p_PROC_ID FROM DUAL;
  -- Вход
	AFS.AFS_SYS.WRITE_LOG_CALL(v_PROC_ID => p_PROC_ID
                          , v_FRAUD_ID => p_FRAUD_ID, v_FRAUD_NAME => p_FRAUD_NAME
                          , v_REQUEST_ID => v_Request_ID, v_PERSON_ID => v_Person_ID
                          , v_OUTCODE => v_OutCode
                          , v_INARRAY => v_InArray
                          , v_OUTNUMBER => v_OutNumber, v_OUTCHAR => v_OutChar
                          , v_OUTARRAY => v_OutArray
                          , v_E_CODE => v_ErrorCode, v_E_TEXT => v_ErrorText
      );

    /*SELECT fr.FRAUD_RULE_ID, fr.FRAUD_RULE_NM 
      INTO p_FRAUD_ID, p_FRAUD_NAME 
      FROM AFS.D_FRAUD_RULE fr
      WHERE FRAUD_RULE_ID=v_Rule_Code and rownum<=1 
        AND RULE_AKT=1 --and rule_status='OK'
      ; */
      
    SELECT fr.FRAUD_RULE_ID, fr.FRAUD_RULE_NM 
      INTO p_FRAUD_ID, p_FRAUD_NAME 
      FROM AFS.D_FRAUD_RULE fr
      WHERE RULE_CODE=v_Rule_Code and rownum<=1 
        AND RULE_AKT=1
        ;
  EXCEPTION 
    WHEN no_data_found 
                      THEN v_ErrorCode:=301; v_ErrorText:='Правило не найдено'; 
        --p_FRAUD_ID:= v_Rule_Code;
        v_OutNumber := -1;
        v_OutChar := TO_CHAR(v_OutNumber);
    WHEN others       THEN v_ErrorCode:=302; v_ErrorText:=substr(TO_CHAR(SQLCODE)||' | '||SQLERRM,0,4000); 
        --p_FRAUD_ID:= v_Rule_Code;
        v_OutNumber := -1;
        v_OutChar := TO_CHAR(v_OutNumber);
  END;
    
	IF p_PROC_ID>0 and p_FRAUD_ID>0 and v_ErrorCode<=0 AND AFS.afs_filter.check_request_id_main(v_Request_ID)=0 THEN
    -- выполняем правило
		BEGIN
      p_SqlStr := 'BEGIN '||p_FRAUD_NAME||'(:1,:2,:3,:4,:5,:6,:7,:8,:9,:10); END;';
      EXECUTE IMMEDIATE p_SqlStr USING  
        IN OUT p_PROC_ID
        ,IN v_Request_ID 
        ,IN v_Person_ID 
        ,IN v_OutCode 
        ,IN v_InArray        
        ,OUT v_OutNumber
        ,OUT v_OutChar
        ,OUT v_OutArray
        ,OUT v_ErrorCode
        ,OUT v_ErrorText 
		  ;

    	EXCEPTION WHEN ex_no_obj_faund 
                      THEN v_ErrorCode:=303; v_ErrorText:=substr(TO_CHAR(SQLCODE)||' | '||SQLERRM,0,4000); 
        --p_FRAUD_ID:= v_Rule_Code;
        v_OutNumber := -1;
        v_OutChar := TO_CHAR(v_OutNumber);
      WHEN others 
                      THEN v_ErrorCode:=304; v_ErrorText:=substr(TO_CHAR(SQLCODE)||' | '||SQLERRM,0,4000);
        v_OutNumber := -1;
        v_OutChar := TO_CHAR(v_OutNumber);
    END;

	END IF;

  -- Выход
	AFS.AFS_SYS.WRITE_LOG_CALL(v_PROC_ID => p_PROC_ID
                          , v_FRAUD_ID => p_FRAUD_ID, v_FRAUD_NAME => p_FRAUD_NAME
                          , v_REQUEST_ID => v_Request_ID, v_PERSON_ID => v_Person_ID
                          , v_OUTCODE => v_OutCode
                          , v_INARRAY => v_InArray
                          , v_OUTNUMBER => v_OutNumber, v_OUTCHAR => v_OutChar
                          , v_OUTARRAY => v_OutArray
                          , v_E_CODE => v_ErrorCode, v_E_TEXT => v_ErrorText
      );

  -- Обработка общей ошибки
  EXCEPTION WHEN OTHERS  
                      THEN v_ErrorCode:=300; v_ErrorText:=substr(TO_CHAR(SQLCODE)||' | '||SQLERRM,0,4000);
    v_OutNumber := -1;
    v_OutChar := TO_CHAR(v_OutNumber);
    
    AFS.AFS_SYS.WRITE_LOG_CALL(v_PROC_ID => p_PROC_ID
                            , v_FRAUD_ID => p_FRAUD_ID
                            , v_FRAUD_NAME => p_FRAUD_NAME
                            , v_REQUEST_ID => v_Request_ID
                            , v_PERSON_ID => v_Person_ID
                            , v_OUTCODE => v_OutCode
                            , v_INARRAY => v_InArray
                            , v_OUTNUMBER => v_OutNumber
                            , v_OUTCHAR => v_OutChar
                            , v_OUTARRAY => v_OutArray
                            , v_E_CODE => v_ErrorCode
                            , v_E_TEXT => v_ErrorText
      );
END;

/

  GRANT EXECUTE ON "AFS"."ROUTE_FRAUD_RULE_ONLINE" TO "L_KREDIT_SCORING";
  GRANT EXECUTE ON "AFS"."ROUTE_FRAUD_RULE_ONLINE" TO "MVKARELIN";
--------------------------------------------------------
--  DDL for Procedure ROUTE_FRAUD_RULE_TEST
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "AFS"."ROUTE_FRAUD_RULE_TEST" (
      v_Rule_Code IN VARCHAR2
      ,v_Request_ID IN NUMBER 
      ,v_Person_ID IN NUMBER 
      ,v_OutCode IN VARCHAR2 
      ,v_InArray IN VARCHAR2
      ,v_OutNumber OUT NUMBER
      ,v_OutChar OUT VARCHAR2
      ,v_OutArray OUT VARCHAR2
      ,v_ErrorCode OUT NUMBER
      ,v_ErrorText OUT VARCHAR2                  
      ) 
AS
/*      v_Rule_Code IN NUMBER     - входной параметр с номером правила, по которому нужно проверить заявку
        v_Request_ID IN NUMBER    - !входной параметр REQUEST_ID заявки из ПКК
        v_Person_ID IN NUMBER     - входной параметр PERSON_ID заявки из ПКК. 
        v_OutCode IN VARCHAR2     - Код внешней системы (для синхронизации между системами)
        v_InArray IN VARCHAR2     - входной параметр для расширения (массив данных)
 
        v_OutNumber OUT NUMBER    - !Выходные данные (результат по фрод-правилу)
                                      0   - фрода нет
                                      1   - фрод есть
                                      -1  - ошибка
                                      9  - нет информации. Т.к. будут использоваться данные ПКК, то считаю код неактуальным
        v_OutChar IN OUT VARCHAR2 - строковый эквивалент возвращаемого значения
        v_OutArray OUT VARCHAR2   - выходной параметр для расширения (массив данных)
        v_ErrorCode OUT NUMBER    - Код ошибки (внутренний код ошибки объекта, если более Нуля то значит возникла проблема при работе)
        v_ErrorText OUT VARCHAR2  - Описание ошибки (детальное описание ошибки)
*/ 
  -- !!!!
  -- Резервируем под данную процедуру код ошибки 300 (3хх)
  -- !!!!

  p_PROC_ID NUMBER            := -1; --уникальный ID процесса
  p_FRAUD_ID NUMBER           := -1;   --ID правила. Определяется по справочнику D_FRAUD_RULE
  p_FRAUD_NAME VARCHAR2(255)  := null;   --ID правила. Определяется по справочнику D_FRAUD_RULE
  
  p_SqlStr VARCHAR2(4000)     :=null; -- Динамическая строка запроса
  
  p_ErrorCode_Sys NUMBER;          --Системный код ошибки. В выходной параметр может не передаваться если не существенен
  p_ErrorText_Sys VARCHAR2(4000);  --Системный текст ошибки. В выходной параметр может не передаваться если не существенен

  ex_no_obj_faund exception;
  pragma exception_init(ex_no_obj_faund, -06550);

BEGIN 
  --задаем первичные выходные параметры
  v_OutNumber := 0;
  v_OutChar := TO_CHAR(v_OutNumber);
  v_OutArray:=null;
  v_ErrorCode:=0;
  v_ErrorText:=null;

  -- определяем RULE
	BEGIN
  SELECT AFS.AFS_PROC_SEQ.NEXTVAL INTO p_PROC_ID FROM DUAL;
  -- Вход
	AFS.AFS_SYS.WRITE_LOG_CALL(v_PROC_ID => p_PROC_ID
                          , v_FRAUD_ID => p_FRAUD_ID, v_FRAUD_NAME => p_FRAUD_NAME
                          , v_REQUEST_ID => v_Request_ID, v_PERSON_ID => v_Person_ID
                          , v_OUTCODE => v_OutCode
                          , v_INARRAY => v_InArray
                          , v_OUTNUMBER => v_OutNumber, v_OUTCHAR => v_OutChar
                          , v_OUTARRAY => v_OutArray
                          , v_E_CODE => v_ErrorCode, v_E_TEXT => v_ErrorText
      );

    /*SELECT fr.FRAUD_RULE_ID, fr.FRAUD_RULE_NM 
      INTO p_FRAUD_ID, p_FRAUD_NAME 
      FROM AFS.D_FRAUD_RULE fr
      WHERE FRAUD_RULE_ID=v_Rule_Code and rownum<=1 
        AND RULE_AKT=1 --and rule_status='OK'
      ; */
      
    SELECT fr.FRAUD_RULE_ID, fr.FRAUD_RULE_NM 
      INTO p_FRAUD_ID, p_FRAUD_NAME 
      FROM AFS.D_FRAUD_RULE fr
      WHERE RULE_CODE=v_Rule_Code and rownum<=1 
        AND RULE_AKT=1
        ;
  EXCEPTION 
    WHEN no_data_found 
                      THEN v_ErrorCode:=301; v_ErrorText:='Правило не найдено'; 
        --p_FRAUD_ID:= v_Rule_Code;
        v_OutNumber := -1;
        v_OutChar := TO_CHAR(v_OutNumber);
    WHEN others       THEN v_ErrorCode:=302; v_ErrorText:=substr(TO_CHAR(SQLCODE)||' | '||SQLERRM,0,4000); 
        --p_FRAUD_ID:= v_Rule_Code;
        v_OutNumber := -1;
        v_OutChar := TO_CHAR(v_OutNumber);
  END;
    
	IF p_PROC_ID>0 and p_FRAUD_ID>0 and v_ErrorCode<=0 THEN
    -- выполняем правило
		BEGIN
      p_SqlStr := 'BEGIN '||p_FRAUD_NAME||'(:1,:2,:3,:4,:5,:6,:7,:8,:9,:10); END;';
      EXECUTE IMMEDIATE p_SqlStr USING  
        IN OUT p_PROC_ID
        ,IN v_Request_ID 
        ,IN v_Person_ID 
        ,IN v_OutCode 
        ,IN v_InArray        
        ,OUT v_OutNumber
        ,OUT v_OutChar
        ,OUT v_OutArray
        ,OUT v_ErrorCode
        ,OUT v_ErrorText 
		  ;

    	EXCEPTION WHEN ex_no_obj_faund 
                      THEN v_ErrorCode:=303; v_ErrorText:=substr(TO_CHAR(SQLCODE)||' | '||SQLERRM,0,4000); 
        --p_FRAUD_ID:= v_Rule_Code;
        v_OutNumber := -1;
        v_OutChar := TO_CHAR(v_OutNumber);
      WHEN others 
                      THEN v_ErrorCode:=304; v_ErrorText:=substr(TO_CHAR(SQLCODE)||' | '||SQLERRM,0,4000);
        v_OutNumber := -1;
        v_OutChar := TO_CHAR(v_OutNumber);
    END;

	END IF;

  -- Выход
	AFS.AFS_SYS.WRITE_LOG_CALL(v_PROC_ID => p_PROC_ID
                          , v_FRAUD_ID => p_FRAUD_ID, v_FRAUD_NAME => p_FRAUD_NAME
                          , v_REQUEST_ID => v_Request_ID, v_PERSON_ID => v_Person_ID
                          , v_OUTCODE => v_OutCode
                          , v_INARRAY => v_InArray
                          , v_OUTNUMBER => v_OutNumber, v_OUTCHAR => v_OutChar
                          , v_OUTARRAY => v_OutArray
                          , v_E_CODE => v_ErrorCode, v_E_TEXT => v_ErrorText
      );

  -- Обработка общей ошибки
  EXCEPTION WHEN OTHERS  
                      THEN v_ErrorCode:=300; v_ErrorText:=substr(TO_CHAR(SQLCODE)||' | '||SQLERRM,0,4000);
    v_OutNumber := -1;
    v_OutChar := TO_CHAR(v_OutNumber);
    
    AFS.AFS_SYS.WRITE_LOG_CALL(v_PROC_ID => p_PROC_ID
                            , v_FRAUD_ID => p_FRAUD_ID
                            , v_FRAUD_NAME => p_FRAUD_NAME
                            , v_REQUEST_ID => v_Request_ID
                            , v_PERSON_ID => v_Person_ID
                            , v_OUTCODE => v_OutCode
                            , v_INARRAY => v_InArray
                            , v_OUTNUMBER => v_OutNumber
                            , v_OUTCHAR => v_OutChar
                            , v_OUTARRAY => v_OutArray
                            , v_E_CODE => v_ErrorCode
                            , v_E_TEXT => v_ErrorText
      );
END;

/
--------------------------------------------------------
--  DDL for Procedure TEST0_
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "AFS"."TEST0_" (
inParam IN NUMBER
,outParam OUT NUMBER
,out_ErrorCode OUT NUMBER
,out_ErrorText OUT VARCHAR2                  
) 
AS

pBlock1 NUMBER:=null;
pBlock2 NUMBER:=null;

BEGIN

-- без обработки ошибки 

-- BLOCK 1
SELECT status_id INTO outParam FROM kredit.c_request WHERE request_id=inParam;


-- BLOCK 2
SELECT status_id INTO outParam FROM kredit.c_request WHERE request_id=inParam*1000000000;



EXCEPTION WHEN OTHERS  THEN 
out_ErrorCode:=999; out_ErrorText:='Global Error';
END;

/
--------------------------------------------------------
--  DDL for Procedure TEST1_
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "AFS"."TEST1_" (
inParam IN NUMBER
,outParam OUT NUMBER
,out_ErrorCode OUT NUMBER
,out_ErrorText OUT VARCHAR2                  
) 
AS

pBlock1 NUMBER:=null;
pBlock2 NUMBER:=null;

BEGIN

-- без обработки ошибки 

-- BLOCK 1
BEGIN
SELECT status_id INTO outParam FROM kredit.c_request WHERE request_id=inParam;
EXCEPTION WHEN OTHERS  THEN out_ErrorCode:=1; out_ErrorText:='Error in Block1';
END;

-- BLOCK 2
BEGIN
SELECT status_id INTO outParam FROM kredit.c_request WHERE request_id=inParam*1000000000;
EXCEPTION WHEN OTHERS  THEN out_ErrorCode:=2; out_ErrorText:='Error in Block2';
END;



EXCEPTION WHEN OTHERS  THEN 
out_ErrorCode:=999; out_ErrorText:='Global Error';
END;

/
