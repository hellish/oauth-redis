# Redis Storage for [OAuth 2.0](https://github.com/go-oauth2/oauth2)

[![Build][Build-Status-Image]][Build-Status-Url] [![Codecov][codecov-image]][codecov-url] [![ReportCard][reportcard-image]][reportcard-url] [![GoDoc][godoc-image]][godoc-url] [![License][license-image]][license-url]

## Install

``` bash
$ go get -u -v github.com/hellish/oauth-redis
```

## Usage

``` go
package main

import (
	"github.com/go-redis/redis"
	oredis "github.com/hellish/oauth-redis"
	"gopkg.in/hellish/oauth2.v3/manage"
)

func main() {
	manager := manage.NewDefaultManager()
	
	// use redis token store
	manager.MapTokenStorage(oredis.NewRedisStore(&redis.Options{
		Addr: "127.0.0.1:6379",
		DB: 15,
	}))

	// use redis cluster store
	// manager.MapTokenStorage(oredis.NewRedisClusterStore(&redis.ClusterOptions{
	// 	Addrs: []string{"127.0.0.1:6379"},
	// 	DB: 15,
	// }))
}
```

## MIT License

```
Copyright (c) 2016 Lyric
```
