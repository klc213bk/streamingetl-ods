CREATE TABLE K_POLICY_PRINT_JOB
   (	
JOB_ID	NUMBER(19,0) NOT NULL,
POLICY_ID	NUMBER(19,0),
PAYCARD_INDI	CHAR(1 BYTE),
LETTER_INDI	CHAR(1 BYTE),
ACKLETTER_INDI	CHAR(1 BYTE),
PIS_INDI	CHAR(1 BYTE),
HEALTH_CARD_INDI	CHAR(1 BYTE),
COVER_INDI	CHAR(1 BYTE),
SCHEDULE_INDI	CHAR(1 BYTE),
CLAUSE_INDI	CHAR(1 BYTE),
ANNEXURE_INDI	CHAR(1 BYTE),
ENDORSE_INDI	CHAR(1 BYTE),
EXCLUSION_INDI	CHAR(1 BYTE),
PRINT_CATEGORY	NUMBER(2,0),
PRINT_TYPE	NUMBER(2,0),
PRINT_COPYSET	CHAR(1 BYTE),
PRINT_REASON	VARCHAR2(400 BYTE),
VALID_STATUS	CHAR(1 BYTE),
PRINT_DATE	DATE,
OPERATOR_ID	NUMBER(19,0),
INSERT_TIME	DATE,
INSERTED_BY	NUMBER(19,0),
INSERT_TIMESTAMP	DATE,
UPDATED_BY	NUMBER(19,0),
UPDATE_TIMESTAMP	DATE,
PREMVOUCHER_INDI	CHAR(1 BYTE),
ARCHIVE_ID	NUMBER(19,0),
UPDATE_TIME	DATE,
JOB_TYPE_DESC	VARCHAR2(100 BYTE),
JOB_READY_DATE	DATE,
CONTENT	CLOB,
COPY	NUMBER(2,0),
ERROR_CODE	NUMBER(22,0),
LANG_ID	VARCHAR2(3 BYTE),
CHANGE_ID	NUMBER(19,0),
PRINT_COMP_INDI	NUMBER(22,0),
DATA_DATE	DATE,
TBL_UPD_TIME	DATE,
TBL_UPD_SCN	NUMBER(19,0)
)
