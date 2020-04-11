(ns blocks.store.adl
  "Block storage backed by a directory in the Azure Data Lake store."
  (:require
    [blocks.data :as data]
    [blocks.store :as store]
    [clojure.java.io :as io]
    [clojure.string :as str]
    [clojure.tools.logging :as log]
    [com.stuartsierra.component :as component]
    [manifold.deferred :as d]
    [manifold.stream :as s]
    [multiformats.hash :as multihash])
  (:import
    (com.microsoft.azure.datalake.store
      ADLException
      ADLStoreClient
      DirectoryEntry
      DirectoryEntryType
      IfExists)
    (com.microsoft.azure.datalake.store.oauth2
      AccessTokenProvider)
    java.net.URI
    java.time.Instant
    (org.apache.commons.io.input
      BoundedInputStream)))


;; ## Directory Entries

(def ^:private ^:const write-permission
  "Octal flag string for read-write permissions."
  "640")


(def ^:private ^:const pre-write-path-suffix
  "A suffix to apply to in-flight writes."
  ".ATTEMPT")


(defn- hex?
  "True if the valu eis a valid hexadecimal string."
  [x]
  (and (string? x) (re-matches #"[0-9a-fA-F]+" x)))


(defn- file-entry?
  "True if the given entry is a file."
  [^DirectoryEntry entry]
  (= DirectoryEntryType/FILE (.-type entry)))


(defn- block-file?
  "True if the given entry appears to be a block file."
  [^DirectoryEntry entry]
  (when entry
    (and (file-entry? entry)
         (hex? (.name entry)))))


(defn- directory-entry->stats
  "Generates a metadata map from a `DirectoryEntry` object."
  [store-fqdn root ^DirectoryEntry entry]
  (with-meta
    {:id (multihash/parse (.name entry))
     :size (.length entry)
     :stored-at (.lastModifiedTime entry)}
    {::store store-fqdn
     ::path (.fullName entry)}))



;; ## Location Utilities

(defn- adl-uri
  "Constructs a URI referencing a file in ADLS."
  [fqdn path]
  (URI. "adl" fqdn path nil))


(defn- canonical-root
  "Ensures that a root path begins and ends with a slash."
  [root]
  (let [pre? (str/starts-with? root "/")
        post? (str/ends-with? root "/")]
    (if (and pre? post?)
      root
      (str (when-not pre? "/") root (when-not post? "/")))))


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
  (when path
    (if (str/starts-with? path root)
      (let [file-name (subs path root)]
        (if (hex? file-name)
          (multihash/parse file-name)
          (log/warnf "Encountered block filename with invalid hex: %s"
                     (pr-str file-name))))
      (log/warnf "ADLS file %s is not under root %s"
                 (pr-str path)
                 (pr-str root)))))


(defn- get-file-stats
  "Look up a file in ADL. Returns the stats map if it exists, otherwise nil."
  [^ADLStoreClient client store-fqdn root id]
  (try
    (let [path (id->path root id)
          entry (.getDirectoryEntry client path)]
      (when (block-file? entry)
        (directory-entry->stats store-fqdn root entry)))
    (catch ADLException ex
      ;; Check for not-found errors and return nil.
      (when (not= 404 (.httpResponseCode ex))
        (throw ex)))))


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


;; TODO: rewrite as macro? use manifold.time
(defn- try-until
  "Call zero-arity predicate function `ready?` up to `tries`
  times, waiting `wait-period` before a new attempt. If no
  tries remain, log a message with `intent`, return nil."
  [ready? {:keys [tries wait-period intent]
           :or {tries 1 wait-period 20} :as opts}]
  (if-not (pos? tries)
    (log/infof "Timed out waiting for eventual consistency%s."
               (or (and intent (str " (for: " intent ")")) ""))
    (when-not (ready?)
      (Thread/sleep wait-period)
      (recur ready? (update opts :tries dec)))))



;; ## File Content

(deftype ADLFileReader
  [^ADLStoreClient client
   ^String path]

  data/ContentReader

  (read-all
    [this]
    (log/tracef "Opening ADL file %s" path)
    (.getReadStream client path))


  (read-range
    [this start end]
    (log/tracef "Opening ADL file %s byte range %d - %d" path start end)
    (let [stream (.getReadStream client path)]
      (when (pos-int? start)
        (.seek stream start))
      (if (pos-int? end)
        (BoundedInputStream. stream (- end (or start 0)))
        stream))))


(alter-meta! #'->ADLFileReader assoc :private true)


(defn- file->block
  "Creates a lazy block to read from the given ADL file."
  [client stats]
  (let [stat-meta (meta stats)]
    (with-meta
      (data/create-block
        (:id stats)
        (:size stats)
        (:stored-at stats)
        (->ADLFileReader client (::path stat-meta)))
      stat-meta)))



;; ## Block Store

(defrecord ADLBlockStore
  [token-provider
   ^ADLStoreClient client
   ^String store-fqdn
   ^String root
   token-provider-key]

  component/Lifecycle

  (start
    [this]
    (log/info "Connecting Azure Data Lake client to" store-fqdn)
    (let [^AccessTokenProvider provider (if token-provider-key
                                          (token-provider-key token-provider)
                                          token-provider)
          client (ADLStoreClient/createClient store-fqdn provider)]
      (when-not (.checkAccess client root "rwx")
        (throw (IllegalStateException.
                 (str "Cannot access Azure Data Lake block store at "
                      (adl-uri store-fqdn root)))))
      (when (:check-summary? this)
        (let [summary (.getContentSummary client root)]
          (log/infof "Store contains %.1f MB in %d blocks"
                     (/ (.spaceConsumed summary) 1024.0 1024.0)
                     (.fileCount summary))))
      (assoc this :client client)))


  (stop
    [this]
    (assoc this :client nil))


  store/BlockStore

  (-list
    [this opts]
    ;; FIXME: rewrite as stream
    (->> (list-directory-seq client root (:limit opts) (:after opts))
         (filter block-file?)
         (map (partial directory-entry->stats store-fqdn root))
         (store/select-stats opts)))


  (-stat
    [this id]
    (store/future'
      (get-file-stats client store-fqdn root id)))


  (-get
    [this id]
    (store/future'
      (when-let [stats (get-file-stats client store-fqdn root id)]
        (file->block client root stats))))


  (-put!
    [this block]
    (store/future'
      (if-let [stats (get-file-stats client store-fqdn root (:id block))]
        ;; Block already stored, return it.
        (file->block client root stats)
        ;; Upload block to ADL.
        (let [path (id->path root (:id block))
              pre-write-path (str path pre-write-path-suffix)]
          (with-open [output (.createFile client pre-write-path IfExists/FAIL write-permission true)
                      content (data/content-stream block nil nil)]
            (io/copy content output))
          (try-until #(= (:size block) (.length (.getDirectoryEntry client pre-write-path)))
                     {:tries 5 :wait-period 200 :intent "upload complete"})
          (.rename client pre-write-path path)
          (file->block
            client root
            (with-meta
              {:id (:id block)
               :size (:size block)
               :stored-at (Instant/now)}
              {::store store-fqdn
               ::path path}))))))


  (-delete!
    [this id]
    (store/future'
      (let [path (id->path root id)]
        (log/debugf "Deleting file %s" (adl-uri store-fqdn path))
        (.delete client path))))


  store/ErasableStore

  (-erase!
    [this]
    (store/future'
      (log/debugf "Erasing all files under %s" (adl-uri store-fqdn root))
      (.deleteRecursive client root))))



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
