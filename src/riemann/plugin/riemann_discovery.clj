(ns riemann.plugin.riemann-discovery
  (:require [riemann.plugin.riemann-discovery-file :as file]
            [riemann.plugin.riemann-discovery-util :as util]
            [riemann.time :refer [every!]]
            [riemann.streams :refer [expired?
                                     where
                                     tagged]]))
(defn discovery-stream
  "You can use this stream to automatically index/remove events emitted by riemann-discovery"
  [index]
  (where (tagged "riemann-discovery")
    (fn [event]
      (cond
        (= "added" (:state event)) (index event)
        (= "removed" (:state event)) (index event)))))

(defn discovery-task
  [global-config discovery-config]
  (let [services (atom {})]
    (fn []
      (let [new-services (-> (condp = (:type global-config)
                               :config discovery-config
                               :file (file/get-services discovery-config))
                             (util/get-services-from-configuration))
            current-state @services
            new-state (util/get-new-state current-state new-services)]
        (reset! services new-state)))))

(defn discovery
  ([global-config discovery-config]
   (every! (:interval global-config 60) 30
           (discovery-task global-config discovery-config))))
