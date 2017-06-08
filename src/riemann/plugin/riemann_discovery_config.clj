(ns riemann.plugin.riemann-discovery-config
  (:require [riemann.time :as time]))

(defn reduce-hosts
  "Takes an element of the `:services` key from config-elem, and generates a configuration for it."
  [service default-ttl default-tags]
  (reduce #(assoc %1 [%2 (:name service)] {:tags (into []
                                                       (concat default-tags
                                                               (:tags service [])))
                                           :time (time/unix-time)
                                           :ttl (or (:ttl service) default-ttl)})
          {}
          (:hosts service [nil])))

(defn reduce-services
  "Takes an element of discovery-config and generates a configuration for it."
  [config-elem]
  (reduce #(merge %1 (reduce-hosts %2
                                   (:ttl config-elem)
                                   (:tags config-elem [])))
          {}
          (:services config-elem)))

(defn discover
  "Takes a a vec of map representing the services, example :

   [{:ttl 120
     ;tags [\"foo\"]
     :services [{:hosts [\"foo.bar\" \"foobar.bar\"]
                 :name \"kafka\"
                 :tags [\"bar\"]
                 :ttl 60}
                {:hosts [\"baz.boo\"]
                 :name \"api\"}]}]

   returns a configuration"
  [discovery-config]
  (reduce #(merge %1 (reduce-services %2)) {} discovery-config))
