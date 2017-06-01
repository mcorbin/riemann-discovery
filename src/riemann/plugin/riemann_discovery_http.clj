(ns riemann.plugin.riemann-discovery-http
  (:require [clj-http.client :as http]
            [riemann.plugin.riemann-discovery-config :as config]
            [cheshire.core :as json]))

(def default-http-options {:socket-timeout 1000 :conn-timeout 1000 :accept :json})

(defn discover
  "Takes a map with these options:

   `:url`            HTTP url
   `:http-options`   Optional http options

  GET the configuration from `:url`, and returns it.

  The url should returns a vector describing the services, example:

   [{:ttl 120
     ;tags [\"foo\"]
     :services [{:hosts [\"foo.bar\" \"foobar.bar\"]
                 :name \"kafka\"
                 :tags [\"bar\"]
                 :ttl 60}
                {:hosts [\"baz.boo\"]
                 :name \"api\"}]}]"
  [discovery-config]
  (-> (json/parse-string (:body
                          (http/get (:url discovery-config)
                                    (merge default-http-options
                                           (:http-options discovery-config))))
                         true)
      (config/discover)))

