(ns lupapiste-pubsub.core
  (:require [lupapiste-pubsub.protocol :as pubsub]
            [lupapiste-pubsub.publisher-util :as pub-util]
            [lupapiste-pubsub.subscriber-util :as sub-util]
            [taoensso.timbre :as timbre]
            [lupapiste-pubsub.topic :as topic])
  (:import [com.google.cloud.pubsub.v1 Publisher Subscriber MessageReceiver AckReplyConsumer]
           [java.util.concurrent TimeUnit]
           [com.google.pubsub.v1 PubsubMessage]))


(deftype PubSubClient [config *publishers *subscribers]
  MessageReceiver
  (^void receiveMessage [this ^PubsubMessage message ^AckReplyConsumer consumer]
    (sub-util/receive-message (assoc config :pub-sub-client this) message consumer))

  pubsub/MessageQueueClient
  (publish [this topic-name message]
    (-> (pubsub/get-publisher this topic-name)
        (pub-util/publish this message)))

  (subscribe [this topic-name]
    (let [actual-topic-name (topic/env-prefix (:environment config) topic-name)]
      (when-not (get @*subscribers actual-topic-name)
        (->> (sub-util/build-subscriber config actual-topic-name this)
             (swap! *subscribers assoc actual-topic-name)))))

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


(defn init [{:keys [topics] :as config}]
  (let [pub-sub-client (->PubSubClient config (atom {}) (atom {}))]
    (future
      (Thread/sleep 5000) ; Sleep to let rest of the system get up before starting processing
      (doseq [topic topics]
        (pubsub/subscribe pub-sub-client topic)))
    pub-sub-client))
