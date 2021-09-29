CREATE TABLE T_CONTRACT_EXTEND_LOG_UPDATE
(
CHANGE_ID	NUMBER(19,0),
LOG_TYPE	CHAR(1 BYTE),
POLICY_CHG_ID	NUMBER(19,0),
ITEM_ID	NUMBER(19,0),
DUE_DATE	DATE,
POLICY_YEAR	NUMBER(3,0),
POLICY_PERIOD	NUMBER(4,0),
STRGY_DUE_DATE	DATE,
PREM_STATUS	NUMBER(2,0),
LOG_ID	NUMBER(19,0),
SA_DUE_DATE	DATE,
LAST_CMT_FLG	CHAR(1 BYTE),
EMS_VERSION	NUMBER(10,0),
INSERTED_BY	NUMBER(19,0),
UPDATED_BY	NUMBER(19,0),
INSERT_TIME	DATE,
UPDATE_TIME	DATE,
INSERT_TIMESTAMP	DATE,
UPDATE_TIMESTAMP	DATE,
POLICY_ID	NUMBER(19,0),
BILLING_DATE	DATE,
REMINDER_DATE	DATE,
INDX_DUE_DATE	DATE,
INDX_REJECT	NUMBER(2,0),
INSURABILITY_DUE_DATE	DATE,
INSURABILITY_REJECT_COUNT	NUMBER(4,0),
INSURABILITY_REJECT_REASON	VARCHAR2(1000 BYTE),
BILL_TO_DATE	DATE,
BUCKET_FILLING_DUE_DATE	DATE,
ILP_DUE_DATE	DATE,
WAIVER_SOURCE	NUMBER(2,0),
SCN NUMBER(19,0),
ROW_ID VARCHAR2(18 BYTE)
)
