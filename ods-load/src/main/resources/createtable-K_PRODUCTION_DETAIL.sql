CREATE TABLE K_PRODUCTION_DETAIL
   (	
DETAIL_ID	NUMBER(19,0) NOT NULL,
PRODUCTION_ID	NUMBER(19,0),
POLICY_ID	NUMBER(19,0),
ITEM_ID	NUMBER(19,0),
PRODUCT_ID	NUMBER(19,0),
POLICY_YEAR	NUMBER(3,0),
PRODUCTION_VALUE	NUMBER(20,4),
EFFECTIVE_DATE	DATE,
HIERARCHY_DATE	DATE,
PRODUCER_ID	NUMBER(19,0),
PRODUCER_POSITION	VARCHAR2(2 BYTE),
BENEFIT_TYPE	VARCHAR2(2 BYTE),
FEE_TYPE	NUMBER(3,0),
CHARGE_MODE	CHAR(1 BYTE),
PREM_LIST_ID	NUMBER(14,0),
COMM_ITEM_ID	NUMBER(14,0),
POLICY_CHG_ID	NUMBER(19,0),
EXCHANGE_RATE	NUMBER(18,8),
RELATED_ID	NUMBER(19,0),
INSURED_ID	NUMBER(19,0),
POL_PRODUCTION_VALUE	NUMBER(20,4),
POL_CURRENCY_ID	NUMBER(6,0),
HIERARCHY_EXIST_INDI	CHAR(1 BYTE),
AGGREGATION_ID	NUMBER(19,0),
PRODUCT_VERSION	NUMBER(19,0),
SOURCE_TABLE	CHAR(1 BYTE),
SOURCE_ID	NUMBER(14,0),
RESULT_LIST_ID	NUMBER(19,0),
FINISH_TIME	DATE,
INSERTED_BY	NUMBER(19,0),
UPDATED_BY	NUMBER(19,0),
INSERT_TIME	DATE,
UPDATE_TIME	DATE,
INSERT_TIMESTAMP	DATE,
UPDATE_TIMESTAMP	DATE,
COMMISSION_RATE	NUMBER(5,4),
CHEQUE_INDI	CHAR(1 BYTE),
PREM_ALLOCATE_YEAR	NUMBER(3,0),
RECALCULATED_INDI	CHAR(1 BYTE),
EXCLUDE_POLICY_INDI	CHAR(1 BYTE),
CHANNEL_ORG_ID	NUMBER(19,0),
AGENT_CATE	VARCHAR2(1 BYTE),
YEAR_MONTH	VARCHAR2(6 BYTE),
CONVERSION_CATE	NUMBER(1,0),
ORDER_ID	NUMBER(1,0),
ASSIGN_RATE	NUMBER(4,0),
DATA_DATE	DATE,
TBL_UPD_TIME	DATE
)

