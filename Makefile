.PHONY: test fmt validate dbt-parse

test:
	python3 -m pytest tests/ -v

fmt:
	terraform fmt -recursive terraform/

validate:
	terraform -chdir=terraform validate

dbt-parse:
	cd dbt_project && dbt deps && DBT_PROFILES_DIR=. REDSHIFT_HOST=localhost REDSHIFT_USER=ci REDSHIFT_PASSWORD=ci dbt parse --profiles-dir .
