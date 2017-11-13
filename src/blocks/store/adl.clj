(ns blocks.store.adl
  "Block storage backed by a directory in the Azure Data Lake store."
  (:require
    [blocks.core :as block]
    [blocks.data :as data]
    [blocks.store :as store]
    [clojure.java.io :as io]
    [clojure.string :as str]
    [clojure.tools.logging :as log]
    [com.stuartsierra.component :as component]
    [multihash.core :as multihash])
  (:import
    (com.microsoft.azure.datalake.store
      ADLException
      ADLStoreClient
      DirectoryEntry
      IfExists)
    (com.microsoft.azure.datalake.store.oauth2
      AccessTokenProvider)))


;; ## Storage Utilities

(defn- adl-uri
  "Constructs a URI referencing a file in ADLS."
  [fqdn path]
  (java.net.URI. "adl" fqdn path nil))


(defn- canonical-root
  "Ensures that a root path begins and ends with a slash."
  [root]
  (as-> root path
    (if (str/starts-with? path "/")
      path
      (str "/" path))
    (if (str/ends-with? path "/")
      path
      (str path "/"))))


(defn- id->path
  "Converts a multihash identifier to an ADLS file path, potentially applying a
  common root. Multihashes are rendered as hex strings."
  ^String
  [root id]
  (str root (multihash/hex id)))


(defn- path->id
  "Converts an ADLS file path into a multihash identifier, potentially stripping
  out a common root. The block filename must be a valid hex-encoded multihash."
  [root path]
  (some->
    path
    (store/check #(str/starts-with? % root)
      (log/warnf "ADLS file %s is not under root %s"
                 path (pr-str root)))
    (subs (count root))
    (store/check #(re-matches #"[0-9a-fA-F]+" %)
      (log/warnf "Encountered block filename with invalid hex: %s"
                 (pr-str value)))
    (multihash/decode)))


(defn- directory-entry->stats
  "Generates a metadata map from a `DirectoryEntry` object."
  [store-fqdn root ^DirectoryEntry entry]
  {:id (multihash/decode (.name entry))
   :size (.length entry)
   :source (adl-uri store-fqdn (.fullName entry))
   :stored-at (.lastModifiedTime entry)})


(defn- file->block
  "Creates a lazy block to read from the file identified by the stats map."
  [^ADLStoreClient client root stats]
  (let [id (:id stats)
        path (id->path root id)]
    (block/with-stats
      (data/lazy-block
        id (:size stats)
        (fn file-reader [] (.getReadStream client path)))
      (dissoc stats :id :size))))


(defn- list-directory-seq
  "Produces a lazy sequence of `DirectoryEntry` values representing the
  children of the given directory. This repeatedly calls `EnumerateDirectory`
  as the sequence is consumed."
  [^ADLStoreClient client path limit after]
  (when (or (nil? limit) (pos? limit))
    (lazy-seq
      (log/debugf "EnumerateDirectory in %s after %s limit %s"
                  path (pr-str after) (pr-str limit))
      (let [listing (if limit
                      (.enumerateDirectory client ^String path ^long limit ^String after)
                      (.enumerateDirectory client ^String path ^String after))]
        (when (seq listing)
          (concat listing
                  (list-directory-seq
                    client path
                    (and limit (- limit (count listing)))
                    (.name ^DirectoryEntry (last listing)))))))))



;; ## Block Store

(defrecord ADLBlockStore
  [^AccessTokenProvider token-provider
   ^ADLStoreClient client
   ^String store-fqdn
   ^String root]

  component/Lifecycle

  (start
    [this]
    (log/info "Connecting Azure Data Lake client to" store-fqdn)
    (let [client (ADLStoreClient/createClient store-fqdn token-provider)]
      (when-not (.checkAccess client root "rwx")
        (throw (IllegalStateException.
                 (str "Cannot access Azure Data Lake block store at "
                      (adl-uri store-fqdn root)))))
      (let [summary (.getContentSummary client root)]
        (log/infof "Store contains %.1f MB in %d blocks"
                   (/ (.spaceConsumed summary) 1024.0 1024.0)
                   (.fileCount summary)))
      (assoc this :client client)))


  (stop
    [this]
    (assoc this :client nil))


  store/BlockStore

  (-stat
    [this id]
    (try
      (let [path (id->path root id)
            entry (.getDirectoryEntry client path)]
        (directory-entry->stats store-fqdn root entry))
      (catch ADLException ex
        ; Check for not-found errors and return nil.
        (when (not= 404 (.httpResponseCode ex))
          (throw ex)))))


  (-list
    [this opts]
    (->> (list-directory-seq client root (:limit opts) (:after opts))
         (map (partial directory-entry->stats store-fqdn root))
         (store/select-stats opts)))


  (-get
    [this id]
    (when-let [stats (.-stat this id)]
      (file->block client root stats)))


  (-put!
    [this block]
    (let [path (id->path root (:id block))]
      (when-not (.checkExists client path)
        (with-open [;output (.createFile client path IfExists/OVERWRITE "440" false)
                    output (.createFile client path IfExists/FAIL)
                    content (block/open block)]
          (io/copy content output)))
      (.-get this (:id block))))


  (-delete!
    [this id]
    (let [path (id->path root id)]
      (log/debugf "Deleting file %s" (adl-uri store-fqdn path))
      (.delete client path))))


;; ## Store Construction

(store/privatize-constructors! ADLBlockStore)


(defn adl-block-store
  "Creates a new Azure Data Lake block store.

  Supported options:

  - `:root`
    Path root to use for all store operations. Defaults to `/`.
  - `:token-provider`
    Azure `AccessTokenProvider` instance to use for authentication. May be
    injected after construction."
  [store-fqdn & {:as opts}]
  (when (str/blank? store-fqdn)
    (throw (IllegalArgumentException.
             (str "Data lake store FQDN must be a non-empty string, got: "
                  (pr-str store-fqdn)))))
  (map->ADLBlockStore
    (assoc opts
           :store-fqdn store-fqdn
           :root (canonical-root (:root opts "/")))))


(defmethod store/initialize "adl"
  [location]
  (let [uri (store/parse-uri location)]
    (adl-block-store
      (:host uri)
      :root (:path uri))))
