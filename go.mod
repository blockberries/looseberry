module github.com/blockberries/looseberry

go 1.25.6

require (
	github.com/blockberries/cramberry v1.6.0
	github.com/syndtr/goleveldb v1.0.0
)

require github.com/golang/snappy v0.0.0-20180518054509-2e65f85255db // indirect

replace github.com/blockberries/cramberry => ../cramberry
