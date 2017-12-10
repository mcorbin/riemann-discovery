(ns riemann-discovery.core
  (:require [riemann-discovery.file :as file]
            [riemann-discovery.config :as config]
            [riemann-discovery.http :as http]
            [riemann.time :refer [every!]]
            [riemann.config :refer [reinject]]
            [riemann.streams :refer [expired?
                                     where
                                     tagged]]))

(defn generate-events
  "takes a configuration, generates a list of events with `:state` = state"
  [services state]
  (map (fn [[[host service] {ttl :ttl time :time tags :tags}]]
         {:host host
          :service (str service "-discovery")
          :time time
          :tags (into [] (concat ["riemann-discovery"] tags))
          :state state
          :ttl ttl}) services))

(defn reinject-events
  "reinject events into Riemann."
  [events]
  (doseq [event events]
    (reinject event)))

(defn filter-current-state
  "Filter the current state using tags"
  [current-state tags]
  (if (and tags (not (empty? tags)))
    (into {} (filter (fn [state] (clojure.set/subset? (set tags) (set (:tags (second state))))) current-state))
      current-state))

(defn get-new-state
  "takes the current and the new state (2 configurations), reinject events, returns the next state"
  [old-state current-state global-config]
  (let [old-state-set (set (keys old-state))
        current-state-set (set (keys (filter-current-state current-state
                                                           (:tags global-config))))
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
        ;; updates-services are common services who will be emitted
        ;; (because they are expired)
        ;; old-services are common services that need to be present in the next state
        ;; because they are not expired
        [updated-services old-services]
        ;; todo : dedicated functions ?
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

(defn discovery-stream
  "You can use this stream to automatically index/remove events emitted by riemann-discovery"
  [index]
  (where (tagged "riemann-discovery")
    (fn [event]
      (cond
        (= "added" (:state event)) (index event)
        (= "removed" (:state event)) (riemann.index/delete-exactly index event)))))

;; all discovery mechanisms (each discover fn) should returns a map like this one:

;; {["foo.bar" "kafka"] {:tags ["riemann-discovery"]
;;                       :ttl 60}
;;  ["foobar.bar" "kafka"] {:tags ["riemann-discovery" "kafka"]
;;                          :ttl 60}}

(defn discovery-task
  "takes the discovery global configuration and the discovery specific configuration
   Returns a fn which get the current services, send events to the Riemann index, and update the services atom."
  [global-config discovery-config]
  (let [services (atom {})] ;; contains current view of the world
    (fn []
      (let [;; get actual services running using a discovery mechanism
            current-state (-> (condp = (:type global-config)
                                :config (config/discover discovery-config)
                                :file (file/discover discovery-config)
                                :http (http/discover discovery-config)))
            ;; get the old state
            old-state @services
            ;; get the new state using current-state and old state
            new-state (get-new-state old-state current-state global-config)]
        ;; update the atom
        (reset! services new-state)))))

(defn discovery
  "Takes 2 parameters:

   `global-config` : a map containing global discovery options (common to all discovery mechanisms). Keys are:

  `:type`         discovery mechanism (`:file`,`:config`)
  `:interval`     refresh interval (default `60`)
  `:tags`         A vector of tags to filter events generated by the plugin. Keep all events tagged with all of these tags.

   `discovery-config` : a map containing the configuration for the discovery mechanism"
  ([global-config discovery-config]
   (every! (:interval global-config 60) 10
           (discovery-task global-config discovery-config))))
