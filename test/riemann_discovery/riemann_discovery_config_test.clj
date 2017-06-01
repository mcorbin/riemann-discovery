(ns riemann-discovery.riemann-discovery-config-test
  (:require [riemann.plugin.riemann-discovery :as discovery]
            [riemann.plugin.riemann-discovery-config :as config]
            [riemann.time.controlled :refer :all]
            [riemann.time :refer [unix-time]]
            [riemann-discovery.test-utils :refer [with-mock]]
            [clojure.test :refer :all]))

(use-fixtures :once control-time!)
(use-fixtures :each reset-time!)

(deftest config-discovery-test
  (with-mock [calls riemann.plugin.riemann-discovery/reinject-events]
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

(defn remove-time
  [service-map]
  (into {} (map (fn [[k v]] [k (dissoc v :time)]) service-map)))

(deftest reduce-hosts-test
  (is (= (remove-time (config/reduce-hosts {:name "kafka"
                                                    :ttl 60} 120 []))
         {[nil "kafka"] {:tags ["riemann-discovery"]
                          :ttl 60}}))
  (is (= (remove-time (config/reduce-hosts {:hosts ["foo.bar"]
                                                    :ttl 60} 120 []))
         {["foo.bar" nil] {:tags ["riemann-discovery"]
                           :ttl 60}}))
  (is (= (remove-time (config/reduce-hosts {:hosts ["foo.bar" "foobar.bar"]
                                                    :name "kafka"
                                                    :ttl 60} 120 []))
         {["foo.bar" "kafka"] {:tags ["riemann-discovery"]
                               :ttl 60}
          ["foobar.bar" "kafka"] {:tags ["riemann-discovery"]
                                  :ttl 60}}))
  (is (= (remove-time (config/reduce-hosts {:hosts ["foo.bar"]
                                                    :name "kafka"} 120 []))
         {["foo.bar" "kafka"] {:tags ["riemann-discovery"]
                               :ttl 120}}))
  (is (= (remove-time (config/reduce-hosts {:hosts ["foo.bar"]
                                                    :name "kafka"} nil []))
         {["foo.bar" "kafka"] {:tags ["riemann-discovery"]
                               :ttl nil}}))
  (is (= (remove-time (config/reduce-hosts {:hosts ["foo.bar"]
                                                    :name "kafka"} nil ["foo"]))
         {["foo.bar" "kafka"] {:tags ["riemann-discovery" "foo"]
                               :ttl nil}}))
  (is (= (remove-time (config/reduce-hosts {:hosts ["foo.bar"]
                                                    :tags ["bar"]
                                                    :name "kafka"} nil ["foo"]))
         {["foo.bar" "kafka"] {:tags ["riemann-discovery" "foo" "bar"]
                               :ttl nil}})))

(deftest reduce-services-test
  (is (= (remove-time (config/reduce-services
                       {:ttl 120
                        :services [{:hosts ["foo.bar" "foobar.bar"]
                                    :name "kafka"
                                    :ttl 60}
                                   {:hosts ["baz.boo"]
                                    :name "api"}]}))
         {["foo.bar" "kafka"] {:tags ["riemann-discovery"]
                               :ttl 60}
          ["foobar.bar" "kafka"] {:tags ["riemann-discovery"]
                                  :ttl 60}
          ["baz.boo" "api"] {:tags ["riemann-discovery"]
                                  :ttl 120}})))

(deftest discover-test
  (is (= (remove-time (config/discover
                       [{:ttl 120
                         :services [{:hosts ["foo.bar" "foobar.bar"]
                                     :name "kafka"
                                     :ttl 60}
                                    {:hosts ["baz.boo"]
                                     :name "api"}]}]))
         {["foo.bar" "kafka"] {:tags ["riemann-discovery"]
                               :ttl 60}
          ["foobar.bar" "kafka"] {:tags ["riemann-discovery"]
                                  :ttl 60}
          ["baz.boo" "api"] {:tags ["riemann-discovery"]
                             :ttl 120}}))
  (is (= (remove-time (config/discover
                       [{:ttl 120
                         :services [{:hosts ["foo.bar" "foobar.bar"]
                                     :name "kafka"
                                     :ttl 60}
                                    {:hosts ["baz.boo"]
                                     :name "api"}]}
                        {:ttl 180
                         :services [{:hosts ["foo.bar" "foobar.bar"]
                                     :name "toto"
                                     :ttl 60}
                                    {:hosts ["baz.baz"]
                                     :name "api"}]}]))
         {["foo.bar" "kafka"] {:tags ["riemann-discovery"]
                               :ttl 60}
          ["foobar.bar" "kafka"] {:tags ["riemann-discovery"]
                                  :ttl 60}
          ["baz.boo" "api"] {:tags ["riemann-discovery"]
                             :ttl 120}
          ["foo.bar" "toto"] {:tags ["riemann-discovery"]
                              :ttl 60}
          ["foobar.bar" "toto"] {:tags ["riemann-discovery"]
                                 :ttl 60}
          ["baz.baz" "api"] {:tags ["riemann-discovery"]
                             :ttl 180}})))
