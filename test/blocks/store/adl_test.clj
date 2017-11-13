(ns blocks.store.adl-test
  (:require
    [blocks.core :as block]
    [blocks.store.adl :as adl :refer [adl-block-store]]
    [blocks.store.tests :as tests]
    [clojure.test :refer :all]
    [multihash.core :as multihash])
  (:import
    (com.microsoft.azure.datalake.store.oauth2
      ClientCredsTokenProvider)))


;; ## Integration Tests

(def az-auth-url (System/getenv "AZ_AUTH_URL"))
(def az-app-id (System/getenv "AZ_APP_ID"))
(def az-app-key (System/getenv "AZ_APP_KEY"))

(deftest ^:integration check-adl-store
  (if-let [store-fqdn (System/getenv "BLOCKS_ADL_TEST_STORE")]
    (let [token-provider (ClientCredsTokenProvider.
                           az-auth-url
                           az-app-id
                           az-app-key)]
      ; NOTE: can run concurrent tests by making this `check-store*` instead.
      (tests/check-store
        #(adl-block-store store-fqdn
                          :root "/blocks"
                          :token-provider token-provider)))
    (println "No BLOCKS_ADL_TEST_STORE in environment, skipping integration test!")))
