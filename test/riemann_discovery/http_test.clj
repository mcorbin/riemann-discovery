(ns riemann-discovery.http-test
  (:require [clojure.test :refer :all]
            [riemann-discovery.http :refer [discover]]
            [riemann-discovery.core :as discovery]
            [org.httpkit.server :as http]
            [cheshire.core :as json]
            [riemann-discovery.test-utils :refer [with-mock]]
            [riemann.time.controlled :refer :all]))

(defonce server (atom nil))

(def services [{:ttl 120
                :services [{:hosts ["kafka1" "kafka2"]
                            :name "kafka"
                            :ttl 60}
                           {:hosts ["api1"]
                            :name "api"}]}
               {:services [{:hosts ["zookeeper1"]
                            :name "zookeeper"
                            :ttl 60}]}])

(def server-services (atom services))

(defn handler
  [req]
  {:status 200
   :headers {"Content-Type" "application/json"}
   :body (json/generate-string @server-services)})

(defn stop-server
  []
  (when-not (nil? @server)
    (@server :timeout 100)
    (reset! server nil)))

(defn start-server
  []
  (reset! server (http/run-server handler {:port 9999})))

(use-fixtures :each reset-time!)
(use-fixtures :once (fn [t]
                      (reset! server-services services)
                      (start-server)
                      (with-controlled-time! (t))
                      (stop-server)))

(deftest discover-test
  (let [result (discover {:url "http://localhost:9999"})]
    (is (= result (riemann-discovery.config/discover services)))))

(deftest http-discovery-test
  (with-mock [calls riemann-discovery.core/reinject-events]
    (let [d (discovery/discovery {:type :http}
                                 {:url "http://localhost:9999"})]
      (is (= (count @calls) 0))
      (advance! 9)
      (is (= (count @calls) 0))
      (advance! 10)
      (is (= (count @calls) 1))
      (let [events (first (last @calls))]
        (is (= (count events) 4))
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
                     :tags ["riemann-discovery"]}} events)))
      (advance! 69)
      (is (= (count @calls) 1))
      (advance! 70)
      (is (= (count @calls) 2))
      (let [events (first (last @calls))]
        (is (= (count events) 4))
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
                     :tags ["riemann-discovery"]}} events)))

      (reset! server-services [(last services)])
      (advance! 130)
      (let [events (first (last @calls))]
        (is (= (count events) 4))
        (is (some #{{:host "kafka1"
                     :service "kafka-discovery"
                     :ttl 60
                     :time 70
                     :state "removed"
                     :tags ["riemann-discovery"]}} events))
        (is (some #{{:host "kafka2"
                     :service "kafka-discovery"
                     :ttl 60
                     :time 70
                     :state "removed"
                     :tags ["riemann-discovery"]}} events))
        (is (some #{{:host "api1"
                     :service "api-discovery"
                     :ttl 120
                     :time 70
                     :state "removed"
                     :tags ["riemann-discovery"]}} events))
        (is (some #{{:host "zookeeper1"
                     :service "zookeeper-discovery"
                     :ttl 60
                     :time 130
                     :state "added"
                     :tags ["riemann-discovery"]}} events)))
      (reset! server-services services)
      (advance! 190)
      (let [events (first (last @calls))]
        (is (= (count events) 4))
        (is (some #{{:host "kafka1"
                     :service "kafka-discovery"
                     :ttl 60
                     :time 190
                     :state "added"
                     :tags ["riemann-discovery"]}} events))
        (is (some #{{:host "kafka2"
                     :service "kafka-discovery"
                     :ttl 60
                     :time 190
                     :state "added"
                     :tags ["riemann-discovery"]}} events))
        (is (some #{{:host "api1"
                     :service "api-discovery"
                     :ttl 120
                     :time 190
                     :state "added"
                     :tags ["riemann-discovery"]}} events))
        (is (some #{{:host "zookeeper1"
                     :service "zookeeper-discovery"
                     :ttl 60
                     :time 190
                     :state "added"
                     :tags ["riemann-discovery"]}} events))))))












