(ns lupapiste-pubsub.core
  (:require [lupapiste-pubsub.protocol :as pubsub]
            [lupapiste-pubsub.publisher-util :as pub-util]
            [lupapiste-pubsub.subscriber-util :as sub-util]
            [taoensso.timbre :as timbre])
  (:import [com.google.cloud.pubsub.v1 Publisher Subscriber]))


(defrecord PubSubClient [config *publishers *subscribers]
  pubsub/MessageQueueClient
  (publish [this topic-name message]
    (-> (pubsub/get-publisher this topic-name)
        (pub-util/publish this message)))

  (subscribe [this topic-name handler]
    (pubsub/subscribe this topic-name handler nil))

  (subscribe [_ topic-name handler additional-config]
    (when-not (get @*subscribers topic-name)
      (->> (sub-util/build-subscriber (merge config additional-config) topic-name handler)
           (swap! *subscribers assoc topic-name))))

  (get-publisher [_ topic-name]
    (if topic-name
      (or (get @*publishers topic-name)
          (let [publisher (pub-util/build-publisher config topic-name)]
            (swap! *publishers assoc topic-name publisher)
            publisher))
      (throw (IllegalArgumentException. "No topic-name provided for publisher"))))

  (stop-subscriber [_ topic-name]
    (some-> (get @*subscribers topic-name) sub-util/stop-subscriber))

  (remove-subscription [_ topic-name]
    (some-> (get @*subscribers topic-name) sub-util/stop-subscriber)
    (sub-util/delete-subscription config topic-name))

  (halt [_]
    (timbre/info "Tearing down publishers")
    (->> @*publishers
         (pmap (fn [[_ ^Publisher pub]]
                 (pub-util/shutdown-publisher pub)))
         dorun)
    (reset! *publishers {})
    (timbre/info "Tearing down subscribers")
    (->> @*subscribers
         (pmap (fn [[_ ^Subscriber sub]]
                 (sub-util/stop-subscriber sub)))
         dorun)
    (reset! *subscribers {})))


(defn init [config]
  (->PubSubClient config (atom {}) (atom {})))
