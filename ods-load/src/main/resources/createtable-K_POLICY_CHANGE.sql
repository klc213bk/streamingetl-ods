CREATE TABLE K_POLICY_CHANGE
(
POLICY_CHG_ID	NUMBER(19,0) NOT NULL,
POLICY_ID	NUMBER(19,0),
SERVICE_ID	NUMBER(4,0),
CHANGE_RECORD	VARCHAR2(4000 BYTE),
INSERT_TIME	DATE,
VALIDATE_TIME	DATE,
NEED_UNDERWRITE	CHAR(1 BYTE),
APPLY_TIME	DATE,
CHANGE_CAUSE	VARCHAR2(10 BYTE),
CANCEL_ID	NUMBER(19,0),
CANCEL_TIME	DATE,
REJECTER_ID	NUMBER(19,0),
REJECT_TIME	DATE,
REJECT_NOTE	VARCHAR2(4000 BYTE),
UPDATE_TIME	DATE,
MASTER_CHG_ID	NUMBER(19,0),
CANCEL_CAUSE	VARCHAR2(2 BYTE),
CANCEL_NOTE	VARCHAR2(4000 BYTE),
REJECT_CAUSE	VARCHAR2(2 BYTE),
LAST_HANDLER_ID	NUMBER(19,0),
LAST_ENTRY_TIME	DATE,
LAST_UW_DISP_ID	NUMBER(19,0),
LAST_UW_DISP_TIME	DATE,
ORDER_ID	NUMBER(4,0),
POLICY_CHG_STATUS	NUMBER(2,0),
DISPATCH_EMP	NUMBER(19,0),
LETTER_EFFECT_TYPE	VARCHAR2(2 BYTE),
DISPATCH_TYPE	VARCHAR2(1 BYTE),
DISPATCH_LETTER	CHAR(1 BYTE),
SUB_SERVICE_ID	NUMBER(4,0),
CHANGE_NOTE	VARCHAR2(4000 BYTE),
INSERTED_BY	NUMBER(19,0),
INSERT_TIMESTAMP	DATE,
UPDATED_BY	NUMBER(19,0),
UPDATE_TIMESTAMP	DATE,
PRE_POLICY_CHG	NUMBER(19,0),
CHANGE_SEQ	NUMBER(15,3),
FINISH_TIME	DATE,
ORG_ID	NUMBER(19,0),
REQUEST_EFFECT_DATE	DATE,
TASK_DUE_DATE	DATE,
ALTERATION_INFO_ID	NUMBER(19,0),
POLICY_CHG_ORDER_SEQ	NUMBER(4,0),
DATA_DATE	DATE,
TBL_UPD_TIME	DATE,
SCN	NUMBER(19,0),
COMMIT_SCN NUMBER(19,0),
ROW_ID VARCHAR2(18 BYTE)
)
