BEGIN
	execute immediate 'ALTER TABLE "K_CONTRACT_PRODUCT_LOG" ADD CONSTRAINT "K_CONTRACT_PRODUCT_LOG_X" PRIMARY KEY ("LOG_ID")';
END;	