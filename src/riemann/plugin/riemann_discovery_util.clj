(ns riemann.plugin.riemann-discovery-util
  (:require [riemann.time :as time]
            [riemann.config :refer [reinject core]]
            [riemann.index :refer [insert delete]]
            [riemann.streams :refer [expired?]]))

;; A `service` is a map like :

;; {:hosts ["foo.bar" "foobar.bar"]
;;  :name "kafka"
;;  :ttl 60}

;; It represents a service running in hosts

;; It can be converted into a `services` map like :

;; {["foo.bar" "kafka"] {:tags ["riemann-discovery"]
;;                       :ttl 60}
;;  ["foobar.bar" "kafka"] {:tags ["riemann-discovery"]
;;                          :ttl 60}}

;; A `configuration-elem` is a map with a `:ttl` key (default :ttl for services), a `:services` key containing a vector of `service` :

;; {:ttl 120
;;  :services [{:hosts ["foo.bar" "foobar.bar"]
;;              :name "kafka"
;;              :ttl 60}
;;             {:hosts ["baz.boo"]
;;              :name "api"}]}

;; A `configuration` is a vector of `configuration-elem`:

;; [{:ttl 120
;;   :services [{:hosts ["foo.bar" "foobar.bar"]
;;               :name "kafka"
;;               :ttl 60}
;;              {:hosts ["baz.boo"]
;;               :name "api"}]}]

;; All service discovery mechanisms should returns a `configuration`.
;; This configuration is converted into a `services` map.

(defn service->services
  "Takes a service and a default ttl, generate a `services` map for all hosts/services"
  [service default-ttl]
  (reduce #(assoc %1 [%2 (:name service)] {:tags ["riemann-discovery"]
                                           :time (time/unix-time)
                                           :ttl (or (:ttl service) default-ttl)})
          {}
          (:hosts service [nil])))

(defn configuration-elem->services
  "takes a `configuration-elem` and generates a `services` map for all hosts/services"
  [config-elem]
  (reduce #(merge %1 (service->services %2 (:ttl config-elem))) {}
          (:services config-elem)))

(defn configuration->services
  "Takes a configuration, generate a `services` map for all hosts/services in each configuration elem"
  [config]
  (reduce #(merge %1 (configuration-elem->services %2)) {} config))

(defn generate-events
  "takes a list of services and generates a list of events"
  [services state]
  (map (fn [[[host service] {ttl :ttl time :time}]]
         {:host host
          :service service
          :time time
          :tags ["riemann-discovery"]
          :state state
          :ttl ttl}) services))

(defn reinject-events
  "reinject events into Riemann"
  [events]
  (doseq [event events]
    (reinject event)))

(defn get-new-state
  "takes the current and the new state, reinject events, returns the next state
  TODO : optimize this function (transient, loop... ?)"
  [old-state current-state]
  (let [old-state-set (set (keys old-state))
        current-state-set (set (keys current-state))
        ;; services removed in the new state
        removed-services (->> (clojure.set/difference old-state-set
                                                           current-state-set)
                                   (select-keys old-state))
        ;; services added in the new state
        added-services (->> (clojure.set/difference current-state-set
                                                         old-state-set)
                            (select-keys current-state))
        ;; keys for services common services between the old and the current state
        common-services-keys (clojure.set/intersection old-state-set
                                                       current-state-set)
        ;; updates-services are common services that need to emitted
        ;; (because they are expired)
        ;; old-services are common services that need to be present in the next state
        ;; because they are not expired
        [updated-services old-services]
        (reduce (fn [result k]
                  ;; multiply by 2 the ttl to give a chance to detect
                  ;; a missing service
                  (if (expired? (update (get old-state k) :ttl * 2))
                    (update result 0 #(assoc % k (get current-state k)))
                    (update result 1 #(assoc % k (get old-state k)))))
                [{} {}] common-services-keys)
        ;; the new state
        result-state (merge updated-services old-services added-services)
        ;; we should emit these events
        events (concat (generate-events updated-services "added")
                       (generate-events added-services "added")
                       (generate-events removed-services "removed"))]
    ;; reinject events
    (reinject-events events)
    ;; returns the new state
    result-state))
