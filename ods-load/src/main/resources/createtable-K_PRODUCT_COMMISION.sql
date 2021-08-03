CREATE TABLE K_PRODUCT_COMMISION
   (	
   "ITEM_ID" NUMBER(14,0) NOT NULL, 
	"LIST_ID" NUMBER(14,0), 
	"HEAD_ID" NUMBER(19,0) NOT NULL, 
	"BRANCH_ID" NUMBER(19,0) NOT NULL, 
	"ORGAN_ID" NUMBER(19,0) NOT NULL, 
	"POLICY_TYPE" CHAR(1 BYTE) NOT NULL, 
	"HAPPEN_TIME" DATE, 
	"DUE_TIME" DATE, 
	"AGENT_ID" NUMBER(19,0) NOT NULL, 
	"GRADE_ID" VARCHAR2(3 BYTE), 
	"COMMISION_RATE" NUMBER(5,4), 
	"NORMAL_COMMISION" NUMBER(18,6), 
	"DISCOUNT_RATE" NUMBER(8,6), 
	"COMMISION" NUMBER(18,6), 
	"COMMISION_ID" NUMBER(19,0), 
	"IS_PAY" NUMBER(2,0) DEFAULT 0, 
	"POLICY_YEAR" NUMBER(3,0), 
	"ASSIGN_RATE" NUMBER(4,3), 
	"SIGN_ID" NUMBER(19,0), 
	"MGR_RATE" NUMBER(4,3), 
	"AGENT_CATE" VARCHAR2(1 BYTE) NOT NULL, 
	"COMMISION_TYPE_ID" NUMBER(10,0) NOT NULL, 
	"DERIVATION" CHAR(1 BYTE) DEFAULT '0' NOT NULL, 
	"FEE_TYPE" NUMBER(3,0) DEFAULT 0 NOT NULL, 
	"GST_COMMISION" NUMBER(18,6) DEFAULT 0 NOT NULL, 
	"SUSPEND_CAUSE" NUMBER(1,0), 
	"ISSUE_MODE" NUMBER(2,0), 
	"PAYMENT_ID" NUMBER(19,0), 
	"POSTED" CHAR(1 BYTE) DEFAULT 'N' NOT NULL, 
	"CRED_ID" NUMBER(19,0), 
	"POST_ID" NUMBER(19,0), 
	"POLICY_ID" NUMBER(19,0), 
	"MONEY_ID" NUMBER(6,0), 
	"COMM_STATUS" NUMBER(2,0) DEFAULT 0, 
	"AGGREGATION_ID" NUMBER(19,0), 
	"BENEFIT_ITEM_ID" NUMBER(19,0), 
	"PRODUCT_ID" NUMBER(19,0), 
	"RELATED_ID" NUMBER(14,0), 
	"REVERSAL_POLICY_CHG_ID" NUMBER(19,0), 
	"COMM_SOURCE" NUMBER(2,0) NOT NULL, 
	"COMM_COMMENT" VARCHAR2(1000 BYTE), 
	"EXCHANGE_RATE" NUMBER(14,8) NOT NULL, 
	"CONFIRM_DATE" DATE, 
	"ACCOUNTING_DATE" DATE, 
	"JE_POSTING_ID" NUMBER(19,0), 
	"JE_CREATOR_ID" NUMBER(19,0), 
	"DR_SEG1" VARCHAR2(20 BYTE), 
	"DR_SEG2" VARCHAR2(20 BYTE), 
	"DR_SEG3" VARCHAR2(20 BYTE), 
	"DR_SEG4" VARCHAR2(20 BYTE), 
	"DR_SEG5" VARCHAR2(20 BYTE), 
	"DR_SEG6" VARCHAR2(20 BYTE), 
	"DR_SEG7" VARCHAR2(20 BYTE), 
	"DR_SEG8" VARCHAR2(20 BYTE), 
	"CR_SEG1" VARCHAR2(20 BYTE), 
	"CR_SEG2" VARCHAR2(20 BYTE), 
	"CR_SEG3" VARCHAR2(20 BYTE), 
	"CR_SEG4" VARCHAR2(20 BYTE), 
	"CR_SEG5" VARCHAR2(20 BYTE), 
	"CR_SEG6" VARCHAR2(20 BYTE), 
	"CR_SEG7" VARCHAR2(20 BYTE), 
	"CR_SEG8" VARCHAR2(20 BYTE), 
	"CONFIRM_EMP" NUMBER(19,0), 
	"CHILD_LEVEL" NUMBER(2,0), 
	"DR_SEG9" VARCHAR2(20 BYTE), 
	"DR_SEG10" VARCHAR2(20 BYTE), 
	"DR_SEG11" VARCHAR2(20 BYTE), 
	"DR_SEG12" VARCHAR2(20 BYTE), 
	"DR_SEG13" VARCHAR2(20 BYTE), 
	"DR_SEG14" VARCHAR2(20 BYTE), 
	"DR_SEG15" VARCHAR2(20 BYTE), 
	"DR_SEG16" VARCHAR2(20 BYTE), 
	"DR_SEG17" VARCHAR2(20 BYTE), 
	"DR_SEG18" VARCHAR2(20 BYTE), 
	"DR_SEG19" VARCHAR2(20 BYTE), 
	"DR_SEG20" VARCHAR2(20 BYTE), 
	"CR_SEG9" VARCHAR2(20 BYTE), 
	"CR_SEG10" VARCHAR2(20 BYTE), 
	"CR_SEG11" VARCHAR2(20 BYTE), 
	"CR_SEG12" VARCHAR2(20 BYTE), 
	"CR_SEG13" VARCHAR2(20 BYTE), 
	"CR_SEG14" VARCHAR2(20 BYTE), 
	"CR_SEG15" VARCHAR2(20 BYTE), 
	"CR_SEG16" VARCHAR2(20 BYTE), 
	"CR_SEG17" VARCHAR2(20 BYTE), 
	"CR_SEG18" VARCHAR2(20 BYTE), 
	"CR_SEG19" VARCHAR2(20 BYTE), 
	"CR_SEG20" VARCHAR2(20 BYTE), 
	"CHANNEL_ORG_ID" NUMBER(19,0), 
	"STREAM_ID" NUMBER(19,0), 
	"PREM_TYPE" VARCHAR2(1 BYTE), 
	"CHANGE_ID" NUMBER(19,0), 
	"POLICY_CHG_ID" NUMBER(19,0), 
	"INSERT_TIME" DATE DEFAULT sysdate NOT NULL, 
	"UPDATE_TIME" DATE DEFAULT sysdate NOT NULL, 
	"INSERTED_BY" NUMBER(19,0) DEFAULT 401 NOT NULL, 
	"UPDATED_BY" NUMBER(19,0) DEFAULT 401 NOT NULL, 
	"INSERT_TIMESTAMP" DATE DEFAULT sysdate NOT NULL, 
	"UPDATE_TIMESTAMP" DATE DEFAULT sysdate NOT NULL, 
	"COMMISION_RATE_EXTRA" NUMBER(5,4), 
	"SOURCE_TABLE" VARCHAR2(1 BYTE), 
	"SOURCE_ID" NUMBER(14,0), 
	"PRODUCT_VERSION" NUMBER(19,0), 
	"POL_CURRENCY_ID" NUMBER(6,0), 
	"POL_COMMISION" NUMBER(18,2), 
	"RETAIN_INDI" CHAR(1 BYTE), 
	"MANG_TAKEON_INDI" CHAR(1 BYTE), 
	"COMMISION_VERSION" NUMBER(19,0), 
	"DIVISION_INDI" CHAR(1 BYTE), 
	"STD_PREM_AF" NUMBER(18,2) DEFAULT 0, 
	"EXTRA_PREM_AF" NUMBER(18,2) DEFAULT 0, 
	"EXCHANGE_DATE" DATE, 
	"RESULT_LIST_ID" NUMBER(19,0), 
	"CHEQUE_INDI" CHAR(1 BYTE), 
	"PREM_ALLOCATE_YEAR" NUMBER(3,0), 
	"COMMENTS" VARCHAR2(200 BYTE), 
	"YEAR_MONTH" VARCHAR2(6 BYTE), 
	"CALC_RST_ID" NUMBER(19,0), 
	"CONVERSION_CATE" NUMBER(1,0), 
	"CHEQUE_YEAR_MONTH" VARCHAR2(6 BYTE), 
	"ENTRY_AGE_" NUMBER(3,0), 
	"PRODUCT_VERSION_" VARCHAR2(20 BYTE), 
	"INTERNAL_ID_" VARCHAR2(10 BYTE), 
	"INSURED_CATEGORY_" CHAR(1 BYTE), 
	"COVERAGE_YEAR_" NUMBER(3,0), 
	"COVERAGE_PERIOD_" CHAR(1 BYTE), 
	"CHARGE_YEAR_" NUMBER(3,0), 
	"CHARGE_PERIOD_" CHAR(1 BYTE), 
	"AMOUNT_" NUMBER(18,2), 
	"CHANNEL_CODE_" VARCHAR2(50 BYTE), 
	"INITIAL_TYPE_" CHAR(1 BYTE), 
	"RPT_EXCLUDE_INDI" VARCHAR2(1 BYTE) DEFAULT 'W', 
	"POLICY_PERIOD" NUMBER(4,0), 
	"CHECK_ENTER_TIME" DATE, 
	"SERVICE_ID" NUMBER(4,0), 
	"ORDER_ID" NUMBER(1,0), 
	 CONSTRAINT "PK_K_PRODUCT_COMMISION" PRIMARY KEY ("ITEM_ID")
)