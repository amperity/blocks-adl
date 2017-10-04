(ns blocks.store.adl
  "Block storage backed by a directory in the Azure Data Lake store."
  (:require
    [blocks.core :as block]
    [blocks.data :as data]
    [blocks.store :as store]
    [clojure.string :as str]
    [clojure.tools.logging :as log]
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


(defrecord ADLBlockStore
  [client
   ,,,]

  store/BlockStore

  (-stat
    [this id]
    ,,,)


  (-list
    [this opts]
    ,,,)


  (-get
    [this id]
    ,,,)


  (-put!
    [this block]
    ,,,)


  (-delete!
    [this id]
    ,,,)


  ; TODO: other protocols like ErasableStore?
  )


;; ## Store Construction

(store/privatize-constructors! ADLBlockStore)


(defn adl-block-store
  "Creates a new Azure Data Lake block store.

  Supported options:

  - ..."
  [& {:as opts}]
  (map->ADLBlockStore opts))


(defmethod store/initialize "adl"
  [location]
  (let [uri (store/parse-uri location)]
    ; TODO: construct store
    ))
