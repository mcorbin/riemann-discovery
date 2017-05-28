(ns riemann-discovery.riemannd-discovery-http-test
  (:require [clojure.test :refer :all]
            [riemann.plugin.riemann-discovery-http :refer [discover]]
            [org.httpkit.server :as http]
            [riemann.time.controlled :refer :all]
            [cheshire.core :as json]
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
    (is (= result services))
    ))












