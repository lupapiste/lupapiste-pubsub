(ns lupapiste-pubsub.edn
  (:require [clojure.tools.reader.edn :as edn]))

(defn encode
  "Accepts any clojure value, returns EDN encoded string."
  [data]
  (binding [*print-length* nil]
    (pr-str data)))

(defn decode
  "Accepts EDN encoded string, return decoded value"
  [data]
  (edn/read-string data))
