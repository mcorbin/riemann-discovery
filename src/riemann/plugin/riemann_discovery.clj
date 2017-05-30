(ns riemann.plugin.riemann-discovery
  (:require [riemann.plugin.riemann-discovery-file :as file]
            [riemann.plugin.riemann-discovery-util :as util]
            [riemann.plugin.riemann-discovery-http :as http]
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
  "takes the discovery global configuration and the discovery specific configuration
   Returns a fn which get the current services, send events to the Riemann index, and update the services atom."
  [global-config discovery-config]
  (let [services (atom {})] ;; contains current view of the world
    (fn []
      (let [;; get actual services running using a discovery mechanism
            current-state (-> (condp = (:type global-config)
                                :config discovery-config
                                :file (file/discover discovery-config)
                                :http (http/discover discovery-config))
                              (util/configuration-vec->services))
            ;; get the old state
            old-state @services
            ;; get the new state using current-state and old state
            new-state (util/get-new-state old-state current-state)]
        ;; update the atom
        (reset! services new-state)))))

(defn discovery
  "Takes 2 parameters:

   global-config    : a map containing global discovery options (common to all discovery mechanisms). Keys are :
      :type         : discovery mechanism (`:file`,`:config`)
      :interval     : refresh interval (default `60`)
      :tags         : a list of tags (default `[]`)

   discovery-config : a map containing the configuration for the discovery mechanism"
  ([global-config discovery-config]
   (every! (:interval global-config 60) 30
           (discovery-task global-config discovery-config))))
