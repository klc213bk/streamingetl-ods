BEGIN
	execute immediate 'CREATE INDEX "IDX_SUPPL_LOG_SYNC_1" ON "K_SUPPL_LOG_SYNC" ("INSERT_TIME")';
END;	