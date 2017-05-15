(ns riemann.plugin.riemann-discovery
  (:require [riemann.plugin.riemann-discovery-file :as file]
            [riemann.streams :refer [expired?
                                     where
                                     tagged]]))

(defmulti discovery (fn [discovery-config global-config] (:type global-config)))
(defmethod discovery :file
  [discovery-config global-config]
  (file/file-discovery discovery-config global-config))

(defn discovery-stream
  "You can use this stream to automatically index/remove events emitted by riemann-discovery"
  [index]
  (where (tagged "riemann-discovery")
    (fn [event]
      (let [event (update event :service #(str "discovery-" %))]
        (cond
          (= "added" (:state event)) (index event)
          (= "removed" (:state event)) (index event))))))
