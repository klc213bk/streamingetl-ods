CREATE TABLE T_IMAGE_UPDATE
(	
IMAGE_ID	NUMBER(19,0) NOT NULL,
POLICY_ID	NUMBER(19,0),
IMAGE_TYPE_ID	NUMBER(19,0),
SEQ_NUMBER	NUMBER(8,0),
IMAGE_FORMAT	VARCHAR2(20 BYTE),
IMAGE_DATA	VARCHAR2(255 BYTE),
SCAN_TIME	DATE,
EMP_ID	NUMBER(19,0),
HEAD_ID	NUMBER(19,0),
FILE_CODE	VARCHAR2(20 BYTE),
PROCESS_STATUS	CHAR(1 BYTE),
GROUP_POLICY_ID	NUMBER(19,0),
CASE_ID	NUMBER(19,0),
CHANGE_ID	NUMBER(19,0),
IMAGE_LOCATION	NUMBER(8,0),
IMAGE_FILE_NAME	VARCHAR2(255 BYTE),
AUTH_CODE	VARCHAR2(20 BYTE),
ORGAN_ID	VARCHAR2(40 BYTE),
SUB_FILE_CODE	VARCHAR2(20 BYTE),
BUSINESS_ORGAN	VARCHAR2(40 BYTE),
IS_PRIORITY	CHAR(1 BYTE),
ZIP_DATE	DATE,
IS_CLEAR	CHAR(1 BYTE),
LIST_ID	NUMBER(19,0),
INSERT_TIME	DATE,
INSERTED_BY	NUMBER(19,0),
UPDATE_TIME	DATE,
UPDATED_BY	NUMBER(19,0),
INSERT_TIMESTAMP	DATE,
UPDATE_TIMESTAMP	DATE,
DEPT_ID	NUMBER(19,0),
COMPANY_CODE	VARCHAR2(10 BYTE),
PERSONAL_CODE	VARCHAR2(6 BYTE),
BATCH_DEPT_TYPE	VARCHAR2(2 BYTE),
BATCH_DATE	VARCHAR2(7 BYTE),
BATCH_AREA	VARCHAR2(6 BYTE),
BATCH_DOC_TYPE	VARCHAR2(2 BYTE),
BOX_NO	VARCHAR2(30 BYTE),
REMARK	VARCHAR2(300 BYTE),
SIGNATURE	CHAR(1 BYTE),
REAL_WIDTH	NUMBER(8,0),
SIG_SEQ_NUMBER	NUMBER(8,0),
SCAN_ORDER	NUMBER(8,0),
SCN NUMBER(19,0),
ROW_ID VARCHAR2(18 BYTE)
)
