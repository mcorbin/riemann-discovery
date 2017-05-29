(ns riemann.plugin.riemann-discovery-http
    (:require [clj-http.client :as http]
              [cheshire.core :as json]))


(def default-http-options {:socket-timeout 1000 :conn-timeout 1000 :accept :json})

(defn discover
  "returns all current services"
  [discovery-config]
  (json/parse-string (:body
                      (http/get (:url discovery-config)
                                (merge default-http-options
                                       (:http-options discovery-config))))
                     true))

