CREATE TABLE T_CONTRACT_EXTEND_CX_UPDATE
(
POLICY_CHG_ID	NUMBER(19,0) NOT NULL,
ITEM_ID	NUMBER(19,0) NOT NULL,
POLICY_ID	NUMBER(19,0),
CHANGE_ID	NUMBER(19,0),
LOG_ID	NUMBER(19,0),
PRE_LOG_ID	NUMBER(19,0),
OPER_TYPE	VARCHAR2(1 BYTE),
LOG_TYPE	CHAR(1 BYTE),
CHANGE_SEQ	NUMBER(15,3),
SCN NUMBER(19,0),
ROW_ID VARCHAR2(18 BYTE)
)
