BEGIN
  execute immediate 'CREATE INDEX "K_COMMISION_FEE_IDXF" ON "K_COMMISION_FEE" ("FEE_ID")';
END;