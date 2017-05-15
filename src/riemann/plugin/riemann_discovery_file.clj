(ns riemann.plugin.riemann-discovery-file
  (:require [riemann.plugin.riemann-discovery-util :refer [Discovery
                                                           initialize
                                                           get-new-state
                                                           get-services-from-configuration]]
            [riemann.time :refer [every!]]
            [clojure.edn :as edn])
  (:import java.io.File))

(defn get-extension
  "takes a file path and returns the file extension"
  [filename]
  (let [index (.lastIndexOf filename ".")]
    (when (>= index 0)
      (.substring filename (inc index)))))

(defn get-edn-files
  "returns all edn files path in a directory"
  [directory-path]
  (reduce (fn [result file]
            (if (and (.isFile file) (= "edn" (get-extension (.getPath file))))
           (conj result (.getPath file))
           result))
          []
       (.listFiles (File. directory-path))))

(defn read-edn-file
  "read all edn files in path and return the list of items"
  [path]
  (binding [*read-eval* false]
    (with-open [r (java.io.PushbackReader. (clojure.java.io/reader path))]
      (let [lazy-edn (repeatedly #(try (edn/read r)
                                       (catch java.lang.RuntimeException e
                                         (if (= "EOF while reading" (.getMessage e))
                                           nil
                                           (throw e)))))]
        (reduce #(if %2 (conj %1 %2) (reduced %1)) [] lazy-edn)))))

(defn read-edn-files
  "takes a vec of directory path and returns the content of all edn files in them"
  [directory-path-vec]
  (flatten (map #(->> (get-edn-files %)
                      (map read-edn-file)
                      (flatten))
                directory-path-vec)))

(defn get-file-discovery
  []
  (let [services (atom {})]
    (reify
      Discovery
      (initialize [this discovery-config global-config]
        (let [config (read-edn-files (:path discovery-config))
              current-state @services
              new-services (get-services-from-configuration config)
              new-state (get-new-state current-state new-services)]
          (reset! services new-state))))))

(defn file-discovery
  ([discovery-config] (file-discovery discovery-config {:interval 60}))
  ([discovery-config global-config]
   (let [services (atom {})
         d (get-file-discovery)]
     (every! (:interval global-config) 30
             (fn [] (initialize d discovery-config global-config))))))

