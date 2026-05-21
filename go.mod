module github.com/blockberries/looseberry

go 1.25.6

require (
	github.com/blockberries/cramberry v1.6.0
	github.com/syndtr/goleveldb v1.0.0
)

require (
	github.com/golang/snappy v0.0.4 // indirect
	golang.org/x/net v0.23.0 // indirect
	golang.org/x/sys v0.18.0 // indirect
	gopkg.in/yaml.v2 v2.4.0 // indirect
)

replace github.com/blockberries/cramberry => ../cramberry
