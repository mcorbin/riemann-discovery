(ns riemann-discovery.core-test
  (:require [riemann-discovery.core :as discovery]
            [riemann.time.controlled :refer :all]
            [riemann.time :refer [unix-time]]
            [riemann-discovery.test-utils :refer [with-mock]]
            [clojure.test :refer :all]))

(use-fixtures :once control-time!)
(use-fixtures :each reset-time!)

(deftest generate-events-test
  (is (= (vec (discovery/generate-events {["foo.bar" "kafka"] {:ttl 60}
                                          ["foobar.bar" "kafka"] {:tags []
                                                                  :ttl 60}
                                          ["baz.boo" "api"] {:tags []
                                                             :ttl 120}}
                                         "removed"))
         [{:host "foo.bar"
           :service "kafka-discovery"
           :state "removed"
           :ttl 60
           :tags ["riemann-discovery"]
           :time nil}
          {:host "foobar.bar"
           :state "removed"
           :service "kafka-discovery"
           :ttl 60
           :tags ["riemann-discovery"]
           :time nil}
          {:host "baz.boo"
           :state "removed"
           :service "api-discovery"
           :ttl 120
           :tags ["riemann-discovery"]
           :time nil}])))

(defn get-random-state
  [number]
  (reduce #(assoc %1 [(* %2 2) %2] {:time 100 :ttl 80}) {} (range number)))

(deftest reinject-events-test
  (with-mock [calls riemann.config/reinject]
    (discovery/reinject-events [{:host "foo"
                                 :service "bar"
                                 :time 1
                                 :ttl 60
                                 :tags ["kafka"]
                                 :state "added"}
                                {:host "foo"
                                 :service "baz"
                                 :time 1
                                 :ttl 60
                                 :tags ["riemann"]
                                 :state "added"}])
    (is (= (first (first @calls)))
        {:host "foo"
         :service "bar"
         :time 1
         :ttl 60
         :tags ["kafka"]
         :state "added"})
    (is (= (first (second @calls)))
        {:host "foo"
         :service "baz"
         :time 1
         :ttl 60
         :tags ["riemann"]
         :state "added"})))

(deftest discovery-stream-test
  (let [index (riemann.config/index)
        stream (discovery/discovery-stream index)]
    (stream {:host "foo"
             :service "bar"
             :time 1
             :state "added"
             :tags ["riemann-discovery"]})
    (is (= (riemann.index/lookup index "foo" "bar"))
        {:host "foo"
         :service "bar"
         :time 1
         :state "added"
         :tags ["riemann-discovery"]})
    ;; does not override
    (stream {:host "foo"
             :service "bar"
             :time 2
             :state "added"
             :tags ["riemann-discovery"]})
    (is (= (riemann.index/lookup index "foo" "bar"))
        {:host "foo"
         :service "bar"
         :time 1
         :state "added"
         :tags ["riemann-discovery"]})
    (stream {:host "foo"
             :service "bar"
             :time 2
             :state "removed"
             :tags ["riemann-discovery"]})
    (is (= (riemann.index/lookup index "foo" "bar")) nil)))


(deftest filtre-current-state-test
  (is (= (discovery/filter-current-state {["foo.bar" "kafka"] {:tags ["foo"]}}
                                         [])
         {["foo.bar" "kafka"] {:tags ["foo"]}}))
  (is (= (discovery/filter-current-state {["foo.bar" "kafka"] {:tags ["foo"]}}
                                         nil)
         {["foo.bar" "kafka"] {:tags ["foo"]}}))
  (is (= (discovery/filter-current-state {["foo.bar" "kafka"] {:tags ["foo"]}}
                                         ["foo"])
         {["foo.bar" "kafka"] {:tags ["foo"]}}))
  (is (= (discovery/filter-current-state {["foo.bar" "kafka"] {:tags ["foo" "bar"]}}
                                         ["foo"])
         {["foo.bar" "kafka"] {:tags ["foo" "bar"]}}))
  (is (= (discovery/filter-current-state {["foo.bar" "kafka"] {:tags ["baz"]}}
                                         ["foo"])
         {})))


(deftest get-removed-events-test
  (is (= (discovery/get-removed-events {["foo" "bar"] {:tags ["a"]}}
                                       {})
         {["foo" "bar"] {:tags ["a"]}}))
  (is (= (discovery/get-removed-events {["foo" "bar"] {:tags ["a"]}}
                                       {["foo" "bar"] {:tags ["b"]}})
         {["foo" "bar"] {:tags ["a"]}}))
  (is (= (discovery/get-removed-events {["foo" "bar"] {:tags ["a"]}}
                                       {["foo" "bar"] {:tags ["a"]}})
         {})))
