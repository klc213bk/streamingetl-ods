CREATE TABLE K_JBPM_VARIABLEINSTANCE
   (	
   "ID_" NUMBER(19,0) NOT NULL, 
	"CLASS_" CHAR(1 CHAR) NOT NULL, 
	"VERSION_" NUMBER(10,0) NOT NULL, 
	"NAME_" VARCHAR2(255 CHAR), 
	"CONVERTER_" CHAR(1 CHAR), 
	"TOKEN_" NUMBER(19,0), 
	"TOKENVARIABLEMAP_" NUMBER(19,0), 
	"PROCESSINSTANCE_" NUMBER(19,0), 
	"BYTEARRAYVALUE_" NUMBER(19,0), 
	"DATEVALUE_" TIMESTAMP (6), 
	"DOUBLEVALUE_" FLOAT(126), 
	"LONGIDCLASS_" VARCHAR2(255 CHAR), 
	"LONGVALUE_" NUMBER(19,0), 
	"STRINGIDCLASS_" VARCHAR2(255 CHAR), 
	"STRINGVALUE_" VARCHAR2(4000 CHAR), 
	"TASKINSTANCE_" NUMBER(19,0), 
	 CONSTRAINT "PK_K_JBPM_VARIABLEINSTANCE" PRIMARY KEY ("ID_")
)