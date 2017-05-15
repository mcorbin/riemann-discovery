(ns riemann-discovery.riemann-discovery-util-test
  (:require [clojure.test :refer :all]
            [riemann.time.controlled :refer :all]
            [riemann.time :refer [unix-time]]
            [riemann-discovery.test-utils :refer [with-mock]]
            [riemann.plugin.riemann-discovery-util :as discovery]))

(use-fixtures :once control-time!)
(use-fixtures :each reset-time!)

(defn remove-time
  [service-map]
  (into {} (map (fn [[k v]] [k (dissoc v :time)]) service-map)))

(deftest get-service-map-test
  (is (= (remove-time (discovery/get-service-map {:name "kafka"
                                                  :ttl 60} 120)))
      {[nil "kafka"] {:tags ["riemann-discovery"]
                      :ttl 60}})
  (is (= (remove-time (discovery/get-service-map {:hosts ["foo.bar"]
                                                  :ttl 60} 120)))
      {["foo.bar" nil] {:tags ["riemann-discovery"]
                        :ttl 60}})
  (is (= (remove-time (discovery/get-service-map {:hosts ["foo.bar" "foobar.bar"]
                                                  :name "kafka"
                                                  :ttl 60} 120)))
      {["foo.bar" "kafka"] {:tags ["riemann-discovery"]
                            :ttl 60}
       ["foobar.bar" "kafka"] {:tags ["riemann-discovery"]
                               :ttl 60}})
  (is (= (remove-time (discovery/get-service-map {:hosts ["foo.bar"]
                                                  :name "kafka"} 120)))
      {["foo.bar" "kafka"] {:tags ["riemann-discovery"]
                            :ttl 120}
       ["foobar.bar" "kafka"] {:tags ["riemann-discovery"]
                               :ttl 120}})
  (is (= (remove-time (discovery/get-service-map {:hosts ["foo.bar"]
                                                  :name "kafka"} nil)))
      {["foo.bar" "kafka"] {:tags ["riemann-discovery"]
                            :ttl nil}}))

(deftest get-services-from-configuration-elem-test
  (is (= (remove-time (discovery/get-services-from-configuration-elem
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

(deftest get-services-from-configuration-test
  (is (= (remove-time (discovery/get-services-from-configuration
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
  (is (= (remove-time (discovery/get-services-from-configuration
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

(deftest generate-events-test
  (is (= (vec (discovery/generate-events {["foo.bar" "kafka"] {:tags ["riemann-discovery"]
                                                               :ttl 60}
                                          ["foobar.bar" "kafka"] {:tags ["riemann-discovery"]
                                                                  :ttl 60}
                                          ["baz.boo" "api"] {:tags ["riemann-discovery"]
                                                             :ttl 120}}
                                         "removed"))
         [{:host "foo.bar"
           :service "kafka"
           :state "removed"
           :ttl 60
           :tags ["riemann-discovery"]
           :time nil}
          {:host "foobar.bar"
           :state "removed"
           :service "kafka"
           :ttl 60
           :tags ["riemann-discovery"]
           :time nil}
          {:host "baz.boo"
           :state "removed"
           :service "api"
           :ttl 120
           :tags ["riemann-discovery"]
           :time nil}]
         )))


(deftest get-new-state-test
  (with-mock [calls discovery/reinject-events]
    (testing "first call"
      (is (= (discovery/get-new-state
              {}
              {["foo.bar" "kafka"] {:tags ["riemann-discovery"]
                                    :time 10
                                    :ttl 60}
               ["foobar.bar" "kafka"] {:tags ["riemann-discovery"]
                                       :time 10
                                       :ttl 60}})
             {["foo.bar" "kafka"] {:tags ["riemann-discovery"]
                                   :time 10
                                   :ttl 60}
              ["foobar.bar" "kafka"] {:tags ["riemann-discovery"]
                                      :time 10
                                      :ttl 60}})))
    (is (= (vec (first (last @calls)))
           [{:host "foo.bar"
             :service "kafka"
             :state "added"
             :time 10
             :ttl 60
             :tags ["riemann-discovery"]}
            {:host "foobar.bar"
             :service "kafka"
             :state "added"
             :time 10
             :ttl 60
             :tags ["riemann-discovery"]}]))
    (testing "same configuration"
      (is (= (discovery/get-new-state
              {["foo.bar" "kafka"] {:tags ["riemann-discovery"]
                                    :time 10
                                    :ttl 60}
               ["foobar.bar" "kafka"] {:tags ["riemann-discovery"]
                                       :time 10
                                       :ttl 60}}
              {["foo.bar" "kafka"] {:tags ["riemann-discovery"]
                                    :time 10
                                    :ttl 60}
               ["foobar.bar" "kafka"] {:tags ["riemann-discovery"]
                                       :time 10
                                       :ttl 60}})
             {["foo.bar" "kafka"] {:tags ["riemann-discovery"]
                                   :time 10
                                   :ttl 60}
              ["foobar.bar" "kafka"] {:tags ["riemann-discovery"]
                                      :time 10
                                      :ttl 60}})))
    (is (= (vec (first (last @calls)))
           []))
    (testing "remove service"
      (is (= (discovery/get-new-state
              {["foo.bar" "kafka"] {:tags ["riemann-discovery"]
                                    :time 10
                                    :ttl 60}
               ["foobar.bar" "kafka"] {:tags ["riemann-discovery"]
                                       :time 10
                                       :ttl 60}}
              {["foo.bar" "kafka"] {:tags ["riemann-discovery"]
                                    :time 10
                                    :ttl 60}})
             {["foo.bar" "kafka"] {:tags ["riemann-discovery"]
                                   :time 10
                                   :ttl 60}})))
    (is (= (vec (first (last @calls)))
           [{:host "foobar.bar"
             :service "kafka"
             :state "removed"
             :time 10
             :ttl 60
             :tags ["riemann-discovery"]}]))
    (testing "add service"
      (is (= (discovery/get-new-state
              {["foo.bar" "kafka"] {:tags ["riemann-discovery"]
                                    :time 10
                                    :ttl 60}}
              {["foo.bar" "kafka"] {:tags ["riemann-discovery"]
                                    :time 10
                                    :ttl 60}
               ["foobar.bar" "kafka"] {:tags ["riemann-discovery"]
                                       :time 10
                                       :ttl 60}})
             {["foo.bar" "kafka"] {:tags ["riemann-discovery"]
                                   :time 10
                                   :ttl 60}
              ["foobar.bar" "kafka"] {:tags ["riemann-discovery"]
                                      :time 10
                                      :ttl 60}})))
    (is (= (vec (first (last @calls)))
           [{:host "foobar.bar"
             :service "kafka"
             :state "added"
             :time 10
             :ttl 60
             :tags ["riemann-discovery"]}]))
    (testing "expiration"
      (is (= (discovery/get-new-state
              {["foo.bar" "kafka"] {:tags ["riemann-discovery"]
                                    :time 10
                                    :ttl 60}}
              {["foo.bar" "kafka"] {:tags ["riemann-discovery"]
                                    :time 60
                                    :ttl 60}})
             {["foo.bar" "kafka"] {:tags ["riemann-discovery"]
                                   :time 10
                                   :ttl 60}}))
      (is (= (vec (first (last @calls)))
           []))
      (advance! 129)
      (is (= (discovery/get-new-state
              {["foo.bar" "kafka"] {:tags ["riemann-discovery"]
                                    :time 10
                                    :ttl 60}}
              {["foo.bar" "kafka"] {:tags ["riemann-discovery"]
                                    :time 60
                                    :ttl 60}})
             {["foo.bar" "kafka"] {:tags ["riemann-discovery"]
                                   :time 10
                                   :ttl 60}}))
      (is (= (vec (first (last @calls)))
           []))
      (advance! 131)
      (is (= (discovery/get-new-state
              {["foo.bar" "kafka"] {:tags ["riemann-discovery"]
                                    :time 10
                                    :ttl 60}}
              {["foo.bar" "kafka"] {:tags ["riemann-discovery"]
                                    :time 120
                                    :ttl 60}})
             {["foo.bar" "kafka"] {:tags ["riemann-discovery"]
                                   :time 120
                                   :ttl 60}}))
      (is (= (vec (first (last @calls)))
           [{:host "foo.bar"
             :service "kafka"
             :state "added"
             :time 120
             :ttl 60
             :tags ["riemann-discovery"]}])))))
