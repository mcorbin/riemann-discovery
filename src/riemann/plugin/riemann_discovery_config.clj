(ns riemann.plugin.riemann-discovery-config
  (:require [riemann.plugin.riemann-discovery-util :refer [Discovery
                                                           initialize
                                                           get-new-state
                                                           get-services-from-configuration]]
            [riemann.time :refer [every!]]))

(defn get-config-discovery
  []
  (let [services (atom {})]
    (reify
      Discovery
      (initialize [this discovery-config global-config]
        (let [current-state @services
              new-services (get-services-from-configuration discovery-config)
              new-state (get-new-state current-state new-services)]
          (reset! services new-state))))))

(defn config-discovery
  ([discovery-config] (config-discovery discovery-config {:interval 60}))
  ([discovery-config global-config]
   (let [services (atom {})
         d (get-config-discovery)]
     (every! (:interval global-config) 30
             (fn [] (initialize d discovery-config global-config))))))
