(ns riemann-discovery.file-test
  (:require [riemann-discovery.file :as file]
            [riemann-discovery.core :as discovery]
            [riemann.time.controlled :refer :all]
            [riemann.time :refer [unix-time]]
            [riemann-discovery.test-utils :refer [with-mock]]
            [clojure.test :refer :all]))

(use-fixtures :once control-time!)
(use-fixtures :each reset-time!)

(deftest get-extension-test
  (is (= "clj" (file/get-extension "foo.clj")))
  (is (= "clj" (file/get-extension "/bar/foo.clj")))
  (is (= "clj" (file/get-extension ".clj")))
  (is (= nil (file/get-extension "")))
  (is (= nil (file/get-extension "foo")))
  (is (= nil (file/get-extension "/bar/foo"))))

(deftest get-edn-files-test
  (let [path (file/get-edn-files "test/riemann_discovery/edn/")]
    (is (= 2 (count path)))
    (is (some #{"test/riemann_discovery/edn/f1.edn"} path))
    (is (some #{"test/riemann_discovery/edn/f2.edn"} path))))

(deftest read-edn-file-test
  (is (= (file/read-edn-file "test/riemann_discovery/edn/f1.edn")
         [{:ttl 120
           :services [{:hosts ["kafka1" "kafka2"]
                       :name "kafka"
                       :ttl 60}
                      {:hosts ["api1"]
                       :name "api"}]}
          {:services [{:hosts ["zookeeper1"]
                       :name "zookeeper"
                       :ttl 60}]}])))

(deftest read-edn-files-test
  (let [result (file/read-edn-files ["test/riemann_discovery/edn/"])]
    (is (= (count result) 3))
    (is (some #{{:ttl 120
                 :services [{:hosts ["kafka1" "kafka2"]
                             :name "kafka"
                             :ttl 60}
                            {:hosts ["api1"]
                             :name "api"}]}} result))
    (is (some #{{:services [{:hosts ["zookeeper1"]
                             :name "zookeeper"
                             :ttl 60}]}} result))
    (is {:ttl 120
         :services [{:hosts ["cassandra1"]
                     :name "cassandra"}]})))

(deftest file-discovery-test
  (with-mock [calls riemann-discovery.core/reinject-events]
    (let [d (discovery/discovery {:type :file}
                                 {:path ["test/riemann_discovery/edn/"]})]
      (is (= (count @calls) 0))
      (advance! 9)
      (is (= (count @calls) 0))
      (advance! 10)
      (is (= (count @calls) 1))
      (let [events (first (last @calls))]
        (is (= (count events) 5))
        (is (some #{{:host "kafka1"
                     :service "kafka-discovery"
                     :ttl 60
                     :time 10
                     :state "added"
                     :tags ["riemann-discovery"]}} events))
        (is (some #{{:host "kafka2"
                     :service "kafka-discovery"
                     :ttl 60
                     :time 10
                     :state "added"
                     :tags ["riemann-discovery"]}} events))
        (is (some #{{:host "api1"
                     :service "api-discovery"
                     :ttl 120
                     :time 10
                     :state "added"
                     :tags ["riemann-discovery"]}} events))
        (is (some #{{:host "zookeeper1"
                     :service "zookeeper-discovery"
                     :ttl 60
                     :time 10
                     :state "added"
                     :tags ["riemann-discovery"]}} events))
        (is (some #{{:host "cassandra1"
                     :service "cassandra-discovery"
                     :ttl 120
                     :time 10
                     :state "added"
                     :tags ["riemann-discovery"]}} events)))
      (advance! 69)
      (is (= (count @calls) 1))
      (advance! 70)
      (is (= (count @calls) 2))
      (let [events (first (last @calls))]
        (is (= (count events) 5))
        (is (some #{{:host "kafka1"
                     :service "kafka-discovery"
                     :ttl 60
                     :time 70
                     :state "added"
                     :tags ["riemann-discovery"]}} events))
        (is (some #{{:host "kafka2"
                     :service "kafka-discovery"
                     :ttl 60
                     :time 70
                     :state "added"
                     :tags ["riemann-discovery"]}} events))
        (is (some #{{:host "api1"
                     :service "api-discovery"
                     :ttl 120
                     :time 70
                     :state "added"
                     :tags ["riemann-discovery"]}} events))
        (is (some #{{:host "zookeeper1"
                     :service "zookeeper-discovery"
                     :ttl 60
                     :time 70
                     :state "added"
                     :tags ["riemann-discovery"]}} events))
        (is (some #{{:host "cassandra1"
                     :service "cassandra-discovery"
                     :ttl 120
                     :time 70
                     :state "added"
                     :tags ["riemann-discovery"]}} events))))))
