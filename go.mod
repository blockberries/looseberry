module github.com/blockberries/looseberry

go 1.25.6

replace (
	github.com/blockberries/blockberry => ../blockberry
	github.com/blockberries/cramberry => ../cramberry
	github.com/blockberries/glueberry => ../glueberry
)

require (
	github.com/golang/snappy v0.0.0-20180518054509-2e65f85255db // indirect
	github.com/syndtr/goleveldb v1.0.0 // indirect
)
