(ns lupapiste-pubsub.publisher
  (:require [lupapiste-pubsub.edn :as edn]
            [lupapiste-pubsub.topic :as topic]
            [taoensso.timbre :as timbre])
  (:import [com.google.cloud ServiceOptions]
           [com.google.cloud.pubsub.v1 Publisher]
           [com.google.protobuf ByteString]
           [com.google.pubsub.v1 PubsubMessage TopicName]
           [java.util.concurrent TimeUnit]
           [com.google.api.gax.batching BatchingSettings FlowControlSettings FlowController$LimitExceededBehavior]
           [org.threeten.bp Duration]))

(defn publisher [^TopicName topic channel-provider credentials-provider]
  (timbre/info "Creating publisher for topic" (.toString topic))
  (-> (Publisher/newBuilder topic)
      (.setChannelProvider channel-provider)
      (.setCredentialsProvider credentials-provider)
      (.setBatchingSettings (-> (BatchingSettings/newBuilder)
                                (.setDelayThreshold (Duration/ofMillis 2000))
                                (.setRequestByteThreshold 200000)
                                (.setElementCountThreshold 500)
                                (.setFlowControlSettings (-> (FlowControlSettings/newBuilder)
                                                             (.setLimitExceededBehavior FlowController$LimitExceededBehavior/Block)
                                                             (.build)))
                                (.build)))
      (.build)))

(defprotocol MessagePublisher
  (publish [this message])
  (halt [this]))

(defn- response-topic [{{:keys [customer-id]} :data} environment]
  (if customer-id
    (topic/from-integration-topic customer-id environment)
    (topic/from-conversion-service-topic environment)))

(deftype PubSubPublisher [environment publishers]
  MessagePublisher
  (publish [_ {{:keys [customer-id]} :data topic :topic-name :as message}]
    (if (or customer-id topic)
      (if-let [pub ^Publisher (get publishers (or customer-id topic))]
        (let [data (-> (dissoc message :topic-name)
                       (assoc :response-topic (response-topic message environment))
                       (edn/encode)
                       (ByteString/copyFromUtf8))
              msg  (-> (PubsubMessage/newBuilder)
                       (.setData data)
                       (.build))]
          (timbre/debug "Publishing message" (:message-id message) "to queue" (.toString (.getTopicName pub)))
          (.publish pub msg))
        (throw (ex-info (str "No publisher found for customer id " customer-id) {:message message})))
      (throw (ex-info (str "Message must include the key :customer-id under :data, or :topic-name") {:message message}))))

  (halt [_]
    (timbre/info "Tearing down publishers")
    (->> publishers
         (pmap (fn [[_ ^Publisher pub]]
                 (try
                   (.shutdown pub)
                   (.awaitTermination pub 20 TimeUnit/SECONDS)
                   (catch Throwable t
                     (timbre/error t)))
                 (timbre/info (.getTopicNameString pub) "terminated")))
         dorun)))

(def non-customer-topics ["to-conversion-service"])

(defn- init-publisher [{:keys [credentials-provider channel-provider topic-admin project-id]}
                       topic-str
                       publisher-key]
  (let [topic (TopicName/of (or project-id (ServiceOptions/getDefaultProjectId)) topic-str)]
    (topic/setup-topic topic-admin topic)
    [publisher-key (publisher topic channel-provider credentials-provider)]))

(defn init [{:keys [customer-ids environment] :as config}]
  (->> customer-ids
       (map #(init-publisher config (str "to-integration-" %) %))
       (concat (map #(init-publisher config % %) non-customer-topics))
       (into {})
       (->PubSubPublisher environment)))
