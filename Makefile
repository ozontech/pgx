
PGX_TEST_DATABASE:="host=localhost user=test password=secret dbname=pgx_test"
PGX_TEST_DATABASE_PROXY="postgres://test:secret@localhost:6432/pgx_test"
PGX_TEST_PROXY_LOCALHOST="localhost:6432"
PGX_TEST_PROXY_REMOTEHOST="localhost:5432"


test:
	PGX_TEST_DATABASE=$(PGX_TEST_DATABASE) \
	PGX_TEST_DATABASE_PROXY=$(PGX_TEST_DATABASE_PROXY) \
	PGX_TEST_PROXY_LOCALHOST=$(PGX_TEST_PROXY_LOCALHOST) \
	PGX_TEST_PROXY_REMOTEHOST=$(PGX_TEST_PROXY_REMOTEHOST) \
	go test -v -cover -race ./...

test-e2e: db-restart db-probe
	docker compose exec -e PGPASSWORD="secret" postgres  \
		psql \
			-h localhost \
			-p 5432 \
			-U test \
			-d pgx_test \
			-c 'create domain uint64 as numeric(20,0)'
	PGX_TEST_DATABASE=$(PGX_TEST_DATABASE) go test -v ./...

db-stop:
	docker compose stop postgres
	docker compose rm --force postgres

db-start:
	docker compose up --detach postgres

db-restart: db-stop db-start

db-probe:
	docker compose run --rm postgres-probe

.PHONY: \
	test-e2e \
	test \
	up \
	db-start \
	db-restart \
	db-stop \
	db-probe