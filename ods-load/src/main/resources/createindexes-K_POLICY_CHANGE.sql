BEGIN
	execute immediate 'CREATE INDEX "IDX_PC__INSERT_TIME_SERVI" ON "K_POLICY_CHANGE" ("SERVICE_ID", "INSERT_TIME")';
	execute immediate 'CREATE INDEX "IDX_PC__VALIDATE_TIME_SER" ON "K_POLICY_CHANGE" ("SERVICE_ID", "VALIDATE_TIME")';
	execute immediate 'CREATE INDEX "IDX_POLICY_CHANGE__FINISH_TIME" ON "K_POLICY_CHANGE" ("SERVICE_ID", "FINISH_TIME")';
	execute immediate 'CREATE INDEX "IDX_POLICY_CHANGE__POLICY_SEQ" ON "K_POLICY_CHANGE" ("POLICY_ID", "CHANGE_SEQ")';
	execute immediate 'CREATE INDEX "IDX_POLICY_CHANGE__PRE_POLICY_" ON "K_POLICY_CHANGE" ("PRE_POLICY_CHG")';
	execute immediate 'CREATE INDEX "IDX_POL_CHANGE__MASTER_CHG_ID" ON "K_POLICY_CHANGE" ("MASTER_CHG_ID")';
END;	