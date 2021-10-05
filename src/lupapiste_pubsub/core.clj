(ns lupapiste-pubsub.core
  (:require [lupapiste-pubsub.protocol :as pubsub]
            [lupapiste-pubsub.publisher-util :as pub-util]
            [lupapiste-pubsub.subscriber-util :as sub-util]
            [taoensso.timbre :as timbre])
  (:import [com.google.cloud.pubsub.v1 Publisher Subscriber]
           [java.util.concurrent TimeUnit]))


(defrecord PubSubClient [config *publishers *subscribers]
  pubsub/MessageQueueClient
  (publish [this topic-name message]
    (-> (pubsub/get-publisher this topic-name)
        (pub-util/publish this message)))

  (subscribe [_ topic-name handler]
    (when-not (get @*subscribers topic-name)
      (->> (sub-util/build-subscriber config topic-name handler)
           (swap! *subscribers assoc topic-name))))

  (get-publisher [_ topic-name]
    (if topic-name
      (or (get @*publishers topic-name)
          (let [publisher (pub-util/build-publisher config topic-name)]
            (swap! *publishers assoc topic-name publisher)
            publisher))
      (throw (IllegalArgumentException. "No topic-name provided for publisher"))))

  (halt [_]
    (timbre/info "Tearing down publishers")
    (->> @*publishers
         (pmap (fn [[_ ^Publisher pub]]
                 (try
                   (.shutdown pub)
                   (.awaitTermination pub 20 TimeUnit/SECONDS)
                   (timbre/info (.getTopicNameString pub) "terminated")
                   (catch Throwable t
                     (timbre/error t)))))
         dorun)
    (reset! *publishers {})
    (timbre/info "Tearing down subscribers")
    (->> @*subscribers
         (pmap (fn [[_ ^Subscriber sub]]
                 (try
                   (.stopAsync sub)
                   (.awaitTerminated sub 5 TimeUnit/SECONDS)
                   (timbre/info (.getSubscriptionNameString sub) "terminated")
                   (catch Throwable t
                     (timbre/error t)))))
         dorun)
    (reset! *subscribers {})))


(defn init [config]
  (->PubSubClient config (atom {}) (atom {})))
