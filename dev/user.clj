(ns user
  (:require
    [blocks.core :as block]
    [blocks.store.adl :refer [adl-block-store]]
    [clojure.java.io :as io]
    [clojure.repl :refer :all]
    [clojure.stacktrace :refer [print-cause-trace]]
    [clojure.string :as str]
    [clojure.tools.namespace.repl :refer [refresh]]
    [com.stuartsierra.component :as component]
    [multihash.core :as multihash]
    [multihash.digest :as digest])
  (:import
    (com.microsoft.azure.datalake.store
      ADLException
      ADLStoreClient
      DirectoryEntry
      IfExists)
    (com.microsoft.azure.datalake.store.oauth2
      AccessTokenProvider
      ClientCredsTokenProvider)))


(defn store [path]
  (let [store-fqdn (System/getenv "BLOCKS_ADL_TEST_STORE")
        token-provider (ClientCredsTokenProvider.
                           (System/getenv "AZ_AUTH_URL")
                           (System/getenv "AZ_APP_ID")
                           (System/getenv "AZ_APP_KEY"))]
    (->
      (adl-block-store store-fqdn
                       :root (or path "/blocks")
                       :token-provider token-provider)
      (component/start))))
