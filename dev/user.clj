(ns user
  (:require
    [blocks.core :as block]
    [blocks.store.adl :refer [adl-block-store]]
    [clojure.java.io :as io]
    [clojure.repl :refer :all]
    [clojure.stacktrace :refer [print-cause-trace]]
    [clojure.string :as str]
    [clojure.tools.namespace.repl :refer [refresh]]
    [multihash.core :as multihash])
  (:import
    (com.microsoft.azure.datalake.store
      ADLException
      ADLStoreClient
      DirectoryEntry
      IfExists)
    (com.microsoft.azure.datalake.store.oauth2
      AccessTokenProvider
      ClientCredsTokenProvider)))
