PRISMA ?= npx --yes prisma@6.19.0

export DATABASE_URL

.PHONY: require-database-url db-migrate db-migrate-check db-reset

require-database-url:
	@if [ -z "$$DATABASE_URL" ]; then \
		echo "DATABASE_URL is required; use db-migrate-check for validation"; \
		exit 1; \
	fi

db-migrate: require-database-url
	$(PRISMA) migrate deploy --schema schema.prisma

db-migrate-check:
	@tmp_dir="$$(mktemp -d)"; \
	trap 'rm -rf "$$tmp_dir"' EXIT; \
	DATABASE_URL="file:$$tmp_dir/catalog.db" \
		$(PRISMA) migrate deploy --schema schema.prisma

db-reset: require-database-url
	@if [ "$(ALLOW_DB_RESET)" != "1" ]; then \
		echo "refusing db-reset without ALLOW_DB_RESET=1"; \
		exit 1; \
	fi
	$(PRISMA) migrate reset --force --skip-generate --schema schema.prisma
