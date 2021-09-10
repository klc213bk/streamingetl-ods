BEGIN
	execute immediate 'ALTER TABLE "K_IMAGE" ADD CONSTRAINT "K_IMAGE_X" PRIMARY KEY ("IMAGE_ID")';
	execute immediate 'CREATE INDEX "K_IMAGE_IDXI" ON "K_IMAGE" ("IMAGE_TYPE_ID")';
	execute immediate 'CREATE INDEX "K_IMAGE_IDXP" ON "K_IMAGE" ("POLICY_ID")';
	execute immediate 'CREATE UNIQUE INDEX "K_IMAGE_ETL" ON "K_IMAGE" ("SCN","COMMIT_SCN","ROW_ID")';
END;
