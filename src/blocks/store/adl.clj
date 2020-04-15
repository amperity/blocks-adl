(ns blocks.store.adl
  "Block storage backed by a directory in the Azure Data Lake store."
  (:require
    [blocks.data :as data]
    [blocks.store :as store]
    [clojure.java.io :as io]
    [clojure.string :as str]
    [clojure.tools.logging :as log]
    [com.stuartsierra.component :as component]
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


(def ^:private ^:const landing-suffix
  "A suffix to apply to in-flight writes."
  ".ATTEMPT")


(defn- hex?
  "True if the value is a valid hexadecimal string."
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


(defn- entry-stats
  "Generates a metadata map from a `DirectoryEntry` object."
  [store-fqdn ^DirectoryEntry entry]
  (when (block-file? entry)
    (with-meta
      {:id (multihash/parse (.name entry))
       :size (.length entry)
       :stored-at (.toInstant (.lastModifiedTime entry))}
      {::store store-fqdn
       ::path (.fullName entry)})))



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



;; ## File Functions

(defn- get-file-stats
  "Look up a file in ADL. Returns the stats map if it exists, otherwise nil."
  [^ADLStoreClient client store-fqdn root id]
  (try
    (let [path (id->path root id)
          entry (.getDirectoryEntry client path)]
      (when (block-file? entry)
        (entry-stats store-fqdn entry)))
    (catch ADLException ex
      ;; Check for not-found errors and return nil.
      (when (not= 404 (.httpResponseCode ex))
        (throw ex)))))


(defn- run-entries!
  "Run the provided function over all entries in ADL matching the given query.
  The loop will terminate if the function returns false or nil."
  [^ADLStoreClient client path query f]
  (loop [after (:after query)
         limit (:limit query)]
    (when (or (nil? limit) (pos? limit))
      (log/tracef "EnumerateDirectory in %s after %s limit %s"
                  path (pr-str after) (pr-str limit))
      (let [entries (try
                      (if limit
                        (.enumerateDirectory client ^String path ^long limit ^String after)
                        (.enumerateDirectory client ^String path ^String after))
                      (catch ADLException ex
                        ;; Interpret missing directories as empty.
                        (when (not= 404 (.httpResponseCode ex))
                          (throw ex))))]
        (and
          ;; Check whether there are more entries to process.
          (seq entries)
          ;; Run f on each object entry while it keeps returning true.
          (loop [entries entries]
            (if-let [entry ^DirectoryEntry (first entries)]
              (when (and (or (nil? (:before query))
                             (pos? (compare (:before query) (.name entry))))
                         (f entry))
                (recur (next entries)))
              true))
          ;; Recur to continue listing.
          (recur (.name ^DirectoryEntry (last entries))
                 (and limit (- limit (count entries)))))))))



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
    (let [out (s/stream 1000)]
      (store/future'
        (try
          (run-entries!
            client root opts
            (fn stream-block
              [entry]
              (if-let [stats (entry-stats store-fqdn entry)]
                ;; Publish block to stream.
                @(s/put! out (file->block client stats))
                ;; Doesn't look like a block - ignore and continue.
                true)))
          (catch Exception ex
            (log/error ex "Failure listing ADL directory")
            (s/put! out ex))
          (finally
            (s/close! out))))
      (s/source-only out)))


  (-stat
    [this id]
    (store/future'
      (get-file-stats client store-fqdn root id)))


  (-get
    [this id]
    (store/future'
      (when-let [stats (get-file-stats client store-fqdn root id)]
        (file->block client stats))))


  (-put!
    [this block]
    (store/future'
      (if-let [stats (get-file-stats client store-fqdn root (:id block))]
        ;; Block already stored, return it.
        (file->block client stats)
        ;; Upload block to ADL.
        (let [path (id->path root (:id block))
              landing-path (str path landing-suffix)]
          ;; Create and write block content to file.
          (with-open [output (.createFile client landing-path IfExists/FAIL write-permission true)
                      content (data/content-stream block nil nil)]
            (io/copy content output))
          ;; Wait for upload to complete.
          (loop [tries 5]
            (when (not= (:size block) (.length (.getDirectoryEntry client landing-path)))
              (if (pos? tries)
                (do (Thread/sleep 200)
                    (recur (dec tries)))
                (log/warn "Timed out waiting for upload write consistency to"
                          landing-path))))
          ;; Atomically rename to the target block file.
          (.rename client landing-path path)
          ;; Return block for file.
          (file->block
            client
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
