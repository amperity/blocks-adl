blocks-adl
==========

[![CircleCI](https://circleci.com/gh/amperity/blocks-adl.svg?style=svg)](https://circleci.com/gh/amperity/blocks-adl)

This library implements a [content-addressable](https://en.wikipedia.org/wiki/Content-addressable_storage)
[block store](https://github.com/greglook/blocks) backed by an Azure Data Lake
storage account.


## Installation

Library releases are published on Clojars. To use the latest version with
Leiningen, add the following dependency to your project definition:

[![Clojars Project](http://clojars.org/amperity/blocks-adl/latest-version.svg)](http://clojars.org/amperity/blocks-adl)


## Usage

The `blocks.store.adl` namespaces provides the `adl-block-store` constructor. This accepts
the fully-qualified hostname of the datalake store and an optional root path for the block
store.

The record created eventually expects a `token-provider` which is a AccessTokenProvider from
the datalake oauth2 library. The example uses client credentials from an AD service principal,
but other methods are possible, like using a refresh token or user password.

The principal needs full authorization for the datalake store's path starting from the given root.

See the [Datalake ACL Docs][1] for more information about how this authorization works.


```clojure
=> (require '[blocks.core :as block]
            '[blocks.store.adl :refer [adl-block-store]])
=> (import (com.microsoft.azure.datalake.store.oauth2
             ClientCredsTokenProvider))
com.microsoft.azure.datalake.store.oauth2.ClientCredsTokenProvider

; Create the token provider. This example uses client credentials via the oauth2 token endpoint.
; This token provider will fetch access tokens as needed for the store. If a token has expired
; it will fetch a new one with the given credentials.
=> (def token-endpoint "https://login.microsoftonline.com/c6b6362b-b1d3-4bbe-bda9-2be62711f5e4/oauth2/token")
=> (def token-provider (ClientCredsTokenProvider. token-endpoint "theclientid" "theclientsecret"))
#'token-provider

; Create the block store backed by the  datalake (ADL) store "mystore" under the root "/example/blocks".
=> (def store' (adl-block-store "mystore.azuredatalakestore.net"
                 :root "/example/blocks"
                 :token-provider token-provider))
=> (require '[com.stuartsierra.component :as component])
; Start the store, which will fetch an access token and check the store status.
=> (def store (component/start store'))
INFO blocks.store.adl - Connecting Azure Data Lake client to mystore.azuredatalakestore.net
DEBUG com.microsoft.azure.datalake.store.oauth2.ClientCredsTokenProvider - AADToken: refreshing client-credential based token
DEBUG com.microsoft.azure.datalake.store.oauth2.AzureADAuthenticator - AADToken: fetched token with expiry Fri Nov 17 12:10:01 PST 2017
INFO blocks.store.adl - Store contains 0.0 MB in 0 blocks
#'store

=> (block/list store {})
()

=> (def b1 (block/read! "abcd" :sha2-512))
=> (def b2 (block/read! "1234" :sha1))
=> (count (block/put-batch! store [b1 b2]))
2
=> (def b2-id (-> (block/list store {:algorithm :sha1}) first :id))
=> (= (:id b2) b2-id)
true

=> (block/get store b2-id)
Block[hash:sha1:7110eda4d09e062aa5e4a390b0a572ac0d2c0220 4 ~]

; Like other persistent remote block stores, the block returned is lazy
; it must be opened to read content which will stream from ADL.
=> (def b2' *1)
=> (block/lazy? b2')
true
=> (= b2 b2')
true
=> (slurp (block/open b2'))
"1234"
```


## License

Licensed under the Apache License, Version 2.0. See the [LICENSE](LICENSE) file
for rights and restrictions.

[1]:https://docs.microsoft.com/en-us/azure/data-lake-store/data-lake-store-access-control
