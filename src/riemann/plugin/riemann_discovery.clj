(ns riemann.plugin.riemann-discovery
  (:require [riemann.plugin.riemann-discovery-file :as file]))

(defmulti discovery (fn [discovery-config global-config] (:type global-config)))
(defmethod discovery :file
  [discovery-config global-config]
  (file/file-discovery discovery-config global-config))
