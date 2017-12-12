(ns riemann-discovery.core
  (:require [riemann-discovery.file :as file]
            [riemann-discovery.config :as config]
            [riemann-discovery.http :as http]
            [riemann.time :refer [every!]]
            [riemann.common :refer [pkey]]
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

(defn discovery-stream
  "You can use this stream to automatically index/remove events emitted by riemann-discovery"
  [index]
  (where (tagged "riemann-discovery")
    (fn [event]
      (cond
        (= "added" (:state event)) (when-not (riemann.index/lookup index
                                                                   (:host event)
                                                                   (:service event))
                                     (index event))
        (= "removed" (:state event)) (riemann.index/delete-exactly index event)))))

(defn get-removed-events
  "get the old and the new state, returns the events present in the old state
  but absent in the new state."
  [old-state new-state]
  (reduce (fn [s [k v]]
            (if (= (dissoc (get new-state k) :time) (dissoc v :time))
              s
              (assoc s k v)))
          {}
          old-state))

;; all discovery mechanisms (each discover fn) should returns a map like this one:

;; {["foo.bar" "kafka"] {:tags ["riemann-discovery"]
;;                       :ttl 60}
;;  ["foobar.bar" "kafka"] {:tags ["riemann-discovery" "kafka"]
;;                          :ttl 60}}

(defn discovery-task
  "takes the discovery global configuration and the discovery specific configuration
   Returns a fn which get the current services, send events to the Riemann index, and update the services atom."
  [global-config discovery-config]
  (let [state (atom {})] ;; contains current view of the world
    (fn []
      (let [;; get actual services running using a discovery mechanism
            old-state @state
            ;; the next state
            new-state (-> (condp = (:type global-config)
                                :config (config/discover discovery-config)
                                :file (file/discover discovery-config)
                                :http (http/discover discovery-config))
                                 (filter-current-state (:tags global-config)))
            ;; added events
            added-events (generate-events new-state "added")
            ;; removed events
            removed-events (-> (get-removed-events old-state new-state)
                               (generate-events "removed"))]
        (reset! state new-state)
        (reinject-events (reduce #(conj %1 %2) removed-events added-events))))))

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
