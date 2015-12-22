.PHONY: deps
deps:
	go get -d github.com/go-sql-driver/mysql
	go get -d gopkg.in/yaml.v2

.PHONY: test
test:
	go test -v
