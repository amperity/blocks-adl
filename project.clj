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
   [com.microsoft.azure/azure-data-lake-store-sdk "2.2.3"]
   [com.stuartsierra/component "0.3.2"]
   [mvxcvi/blocks "1.0.0"]
   [mvxcvi/multihash "2.0.2"]]

  :test-selectors
  {:default (complement :integration)
   :integration :integration}

  :profiles
  {:dev
   {:dependencies
    [[org.slf4j/slf4j-simple "1.7.21"]
     [mvxcvi/test.carly "0.4.1"]]}

   :repl
   {:source-paths ["dev"]
    :dependencies [[org.clojure/tools.namespace "0.2.11"]]
    :jvm-opts ["-Dorg.slf4j.simpleLogger.defaultLogLevel=debug"]}

   :test
   {:jvm-opts ["-Dorg.slf4j.simpleLogger.defaultLogLevel=debug"]}})
