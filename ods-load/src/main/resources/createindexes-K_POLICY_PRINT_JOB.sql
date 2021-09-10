BEGIN
	execute immediate 'ALTER TABLE "K_POLICY_PRINT_JOB" ADD CONSTRAINT "K_POLICY_PRINT_JOB_X" PRIMARY KEY ("JOB_ID")';
	execute immediate 'CREATE UNIQUE INDEX "K_POLICY_PRINT_JOB_ETL" ON "K_POLICY_PRINT_JOB" ("SCN","COMMIT_SCN","ROW_ID")';
END;	
