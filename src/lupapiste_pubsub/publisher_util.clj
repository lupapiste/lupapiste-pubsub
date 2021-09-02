(ns lupapiste-pubsub.publisher-util
  (:require [lupapiste-pubsub.edn :as edn]
            [lupapiste-pubsub.protocol :as pubsub]
            [lupapiste-pubsub.topic :as topic]
            [taoensso.timbre :as timbre])
  (:import [com.google.api.gax.batching BatchingSettings FlowControlSettings FlowController$LimitExceededBehavior]
           [com.google.protobuf ByteString]
           [com.google.pubsub.v1 PubsubMessage TopicName]
           [com.google.cloud.pubsub.v1 Publisher]
           [com.google.cloud ServiceOptions]
           [org.threeten.bp Duration]))


(defn build-publisher [{:keys [channel-provider credentials-provider topic-admin project-id]} ^String topic-name]
  (timbre/info "Creating publisher for topic" topic-name)
  (let [topic (TopicName/of (or project-id (ServiceOptions/getDefaultProjectId)) topic-name)]
    (topic/setup-topic topic-admin topic)
    (-> (Publisher/newBuilder topic)
        (.setChannelProvider channel-provider)
        (.setCredentialsProvider credentials-provider)
        (.setBatchingSettings (-> (BatchingSettings/newBuilder)
                                  (.setDelayThreshold (Duration/ofMillis 500))
                                  (.setRequestByteThreshold 200000)
                                  (.setElementCountThreshold 500)
                                  (.setFlowControlSettings
                                    (-> (FlowControlSettings/newBuilder)
                                        (.setMaxOutstandingElementCount 10000)
                                        (.setMaxOutstandingRequestBytes 10000000) ; 10 MB
                                        (.setLimitExceededBehavior FlowController$LimitExceededBehavior/Block)
                                        (.build)))
                                  (.build)))
        (.build))))


(defn- subscribe-to-response-topic [pub-sub-client {:keys [response-topic response-handler]}]
  (when (and response-topic response-handler)
    (pubsub/subscribe pub-sub-client response-topic response-handler)))


(defn publish [^Publisher pub pub-sub-client message]
  (subscribe-to-response-topic pub-sub-client message)
  (let [data (-> (edn/encode message)
                 (ByteString/copyFromUtf8))
        msg  (-> (PubsubMessage/newBuilder)
                 (.setData data)
                 (.build))]
    (timbre/debug "Publishing message" (:message-id message) "to queue" (.toString (.getTopicName pub)))
    (.publish pub msg)))
