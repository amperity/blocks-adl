(ns blocks.store.adl-test
  (:require
    [blocks.core :as block]
    [blocks.store.adl :as adl :refer [adl-block-store]]
    [blocks.store.tests :as tests]
    [clojure.test :refer :all]
    [com.stuartsierra.component :as component]
    [multiformats.hash :as multihash])
  (:import
    (com.microsoft.azure.datalake.store
      ADLStoreClient)
    (com.microsoft.azure.datalake.store.oauth2
      ClientCredsTokenProvider)))


;; ## Integration Tests

(def az-token-endpoint (System/getenv "AZ_TOKEN_URL"))
(def az-client-id (System/getenv "AZ_CLIENT_ID"))
(def az-client-secret (System/getenv "AZ_CLIENT_SECRET"))


(deftest ^:integration check-adl-store
  (if-let [store-fqdn (System/getenv "BLOCKS_ADL_TEST_STORE")]
    (let [token-provider (ClientCredsTokenProvider.
                           az-token-endpoint
                           az-client-id
                           az-client-secret)
          client (ADLStoreClient/createClient store-fqdn token-provider)
          root-path (str "/testing/blocks/test-" (rand-int 1000))
          counter (atom 0)]
      (.createDirectory client root-path "770")
      ;; NOTE: can run concurrent tests by making this `check-store*` instead.
      (tests/check-store
        (fn [& _]
          (let [path (format "%s/%08d" root-path (swap! counter inc))
                store (adl-block-store
                        store-fqdn
                        :root path
                        :token-provider token-provider)]
            (.createDirectory client path "770")
            (component/start store)))))
    (println "No BLOCKS_ADL_TEST_STORE in environment, skipping integration test!")))
