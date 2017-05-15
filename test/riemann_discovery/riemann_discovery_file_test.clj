(ns riemann-discovery.riemann-discovery-file-test
  (:require [riemann.plugin.riemann-discovery-file :as file]
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
  (with-mock [calls riemann.plugin.riemann-discovery-util/reinject-events]
    (let [d (file/file-discovery {:path ["test/riemann_discovery/edn/"]})]
      (is (= (count @calls) 0))
      (advance! 29)
      (is (= (count @calls) 0))
      (advance! 30)
      (is (= (count @calls) 1))
      (let [events (first (last @calls))]
        (is (= (count events) 5))
        (is (some #{{:host "kafka1"
                     :service "kafka"
                     :ttl 60
                     :time 30
                     :state "added"
                     :tags ["riemann-discovery"]}} events))
        (is (some #{{:host "kafka2"
                     :service "kafka"
                     :ttl 60
                     :time 30
                     :state "added"
                     :tags ["riemann-discovery"]}} events))
        (is (some #{{:host "api1"
                     :service "api"
                     :ttl 120
                     :time 30
                     :state "added"
                     :tags ["riemann-discovery"]}} events))
        (is (some #{{:host "zookeeper1"
                     :service "zookeeper"
                     :ttl 60
                     :time 30
                     :state "added"
                     :tags ["riemann-discovery"]}} events))
        (is (some #{{:host "cassandra1"
                     :service "cassandra"
                     :ttl 120
                     :time 30
                     :state "added"
                     :tags ["riemann-discovery"]}} events)))
      (advance! 89)
      (is (= (count @calls) 1))
      (advance! 90)
      (is (= (count @calls) 2))
      (is (= [] (first (last @calls))))
      (advance! 149)
      (is (= (count @calls) 2))
      (advance! 150)
      (is (= (count @calls) 3))
      (is (= [] (first (last @calls))))
      (advance! 209)
      (is (= (count @calls) 3))
      (advance! 210)
      (is (= (count @calls) 4))
      ;; 60 + 2*60 = 180
      (let [events (first (last @calls))]
        (is (= (count events) 3))
        (is (some #{{:host "kafka1"
                     :service "kafka"
                     :ttl 60
                     :time 210
                     :state "added"
                     :tags ["riemann-discovery"]}} events))
        (is (some #{{:host "kafka2"
                     :service "kafka"
                     :ttl 60
                     :time 210
                     :state "added"
                     :tags ["riemann-discovery"]}} events))
        (is (some #{{:host "zookeeper1"
                     :service "zookeeper"
                     :ttl 60
                     :time 210
                     :state "added"
                     :tags ["riemann-discovery"]}} events)))
      (advance! 270)
      (is (= (count @calls) 5))
      (advance! 330)
      (is (= (count @calls) 6))
      ;; 60 + 2*120 = 300
      (let [events (first (last @calls))]
        (is (= (count events) 2))
        (is (some #{{:host "cassandra1"
                     :service "cassandra"
                     :ttl 120
                     :time 330
                     :state "added"
                     :tags ["riemann-discovery"]}} events))
        (is (some #{{:host "api1"
                     :service "api"
                     :ttl 120
                     :time 330
                     :state "added"
                     :tags ["riemann-discovery"]}} events))))))
