BEGIN
	execute immediate 'ALTER TABLE "K_JBPM_VARIABLEINSTANCE" ADD CONSTRAINT "K_JBPM_VARIABLEINSTANCE_X" PRIMARY KEY ("ID_")';
END;
