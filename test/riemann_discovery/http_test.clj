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

(defn handler
  [req]
  {:status 200
   :headers {"Content-Type" "application/json"}
   :body (json/generate-string services)})

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
                     :service "kafka"
                     :ttl 60
                     :time 10
                     :state "added"
                     :tags ["riemann-discovery"]}} events))
        (is (some #{{:host "kafka2"
                     :service "kafka"
                     :ttl 60
                     :time 10
                     :state "added"
                     :tags ["riemann-discovery"]}} events))
        (is (some #{{:host "api1"
                     :service "api"
                     :ttl 120
                     :time 10
                     :state "added"
                     :tags ["riemann-discovery"]}} events))
        (is (some #{{:host "zookeeper1"
                     :service "zookeeper"
                     :ttl 60
                     :time 10
                     :state "added"
                     :tags ["riemann-discovery"]}} events)))
      (advance! 69)
      (is (= (count @calls) 1))
      (advance! 70)
      (is (= (count @calls) 2))
      (is (= [] (first (last @calls))))
      (advance! 129)
      (is (= (count @calls) 2))
      (advance! 130)
      (is (= (count @calls) 3))
      (is (= [] (first (last @calls))))
      (advance! 189)
      (is (= (count @calls) 3))
      (advance! 190)
      (is (= (count @calls) 4))
      ;; 60 + 2*60 = 180
      (let [events (first (last @calls))]
        (is (= (count events) 3))
        (is (some #{{:host "kafka1"
                     :service "kafka"
                     :ttl 60
                     :time 190
                     :state "added"
                     :tags ["riemann-discovery"]}} events))
        (is (some #{{:host "kafka2"
                     :service "kafka"
                     :ttl 60
                     :time 190
                     :state "added"
                     :tags ["riemann-discovery"]}} events))
        (is (some #{{:host "zookeeper1"
                     :service "zookeeper"
                     :ttl 60
                     :time 190
                     :state "added"
                     :tags ["riemann-discovery"]}} events)))
      (advance! 250)
      (is (= (count @calls) 5))
      (advance! 310)
      (is (= (count @calls) 6))
      ;; 60 + 2*120 = 300
      (let [events (first (last @calls))]
        (is (= (count events) 1))
        (is (some #{{:host "api1"
                     :service "api"
                     :ttl 120
                     :time 310
                     :state "added"
                     :tags ["riemann-discovery"]}} events))))))












