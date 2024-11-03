ifeq ($(OS), Windows_NT)
	HELP_CMD = Select-String "^[a-zA-Z_-]+:.*?\#\# .*$$" "./Makefile" | Foreach-Object { $$_data = $$_.matches -split ":.*?\#\# "; $$obj = New-Object PSCustomObject; Add-Member -InputObject $$obj -NotePropertyName ('Command') -NotePropertyValue $$_data[0]; Add-Member -InputObject $$obj -NotePropertyName ('Description') -NotePropertyValue $$_data[1]; $$obj } | Format-Table -HideTableHeaders @{Expression={ $$e = [char]27; "$$e[36m$$($$_.Command)$${e}[0m" }}, Description
else
	HELP_CMD = grep -E '^[a-zA-Z_-]+:.*?\#\# .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?\#\# "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'
endif

.DEFAULT_GOAL := run

.PHONY: docker-up
docker-up:
	@docker compose up -d

.PHONY: docker-down
docker-down:
	@docker compose -p sampe-cli-reconciliation-service down -v
	@rm -rf .docker/db/data

.PHONY: generate-sample
generate-sample: docker-up
	@chmod +x ./scripts/generate_sample.sh
	. ./scripts/generate_sample.sh _generate_sample `pwd`


.PHONY: import-reconcile
import-reconcile: docker-up
	@chmod +x ./scripts/report.sh
	. ./scripts/report.sh _report -s `pwd`/sample/system_trx -b `pwd`/sample/bank_trx -f "$$from" -t "$$to"
