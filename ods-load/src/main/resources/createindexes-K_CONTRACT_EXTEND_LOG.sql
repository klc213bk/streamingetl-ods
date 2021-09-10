BEGIN
	execute immediate 'ALTER TABLE "K_CONTRACT_EXTEND_LOG" ADD CONSTRAINT "K_CONTRACT_EXTEND_LOG_X" PRIMARY KEY ("LOG_ID")';
	execute immediate 'CREATE UNIQUE INDEX "K_CONTRACT_EXTEND_LOG_ETL" ON "K_CONTRACT_EXTEND_LOG" ("SCN","COMMIT_SCN","ROW_ID")';
END;
