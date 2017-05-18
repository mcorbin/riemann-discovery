(ns riemann-discovery.riemann-discovery-config-test
  (:require [riemann.plugin.riemann-discovery :as discovery]
            [riemann.time.controlled :refer :all]
            [riemann.time :refer [unix-time]]
            [riemann-discovery.test-utils :refer [with-mock]]
            [clojure.test :refer :all]))

(use-fixtures :once control-time!)
(use-fixtures :each reset-time!)

(deftest config-discovery-test
  (with-mock [calls riemann.plugin.riemann-discovery-util/reinject-events]
    (let [d (discovery/discovery {:type :config}
                                 [{:ttl 120
                                   :services [{:hosts ["kafka1" "kafka2"]
                                               :name "kafka"
                                               :ttl 60}
                                              {:hosts ["api1"]
                                               :name "api"}]}

                                  {:services [{:hosts ["zookeeper1"]
                                               :name "zookeeper"
                                               :ttl 60}]}])]
      (is (= (count @calls) 0))
      (advance! 29)
      (is (= (count @calls) 0))
      (advance! 30)
      (is (= (count @calls) 1))
      (let [events (first (last @calls))]
        (is (= (count events) 4))
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
                     :tags ["riemann-discovery"]}} events))))))


