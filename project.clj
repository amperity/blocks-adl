(defproject amperity/blocks-adl "0.1.0-SNAPSHOT"
  :description "Content-addressable Azure DataLake block store."
  :url "https://github.com/amperity/blocks-adls"
  :license {:name "Apache License 2.0"
            :url "http://www.apache.org/licenses/LICENSE-2.0"}

  :deploy-branches ["master"]
  :pedantic? :abort

  :dependencies
  [[org.clojure/clojure "1.8.0"]
   [org.clojure/tools.logging "0.4.0"]
   [mvxcvi/blocks "0.9.1"]
   [mvxcvi/multihash "2.0.1"]
   [com.microsoft.azure/azure-data-lake-store-sdk "2.2.3"]]

  :profiles
  {:dev
   {:dependencies [[commons-logging "1.2"]]}

   :repl
   {:source-paths ["dev"]
    :dependencies [[org.clojure/tools.namespace "0.2.11"]]
    :jvm-opts ["-Dorg.apache.commons.logging.Log=org.apache.commons.logging.impl.SimpleLog"
               #_"-Dorg.apache.commons.logging.simplelog.defaultlog=trace"]}

   :test
   {:jvm-opts ["-Dorg.apache.commons.logging.Log=org.apache.commons.logging.impl.NoOpLog"]}})
