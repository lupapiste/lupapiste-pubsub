(ns lupapiste-pubsub.subscriber-util
  (:require [lupapiste-pubsub.edn :as edn]
            [lupapiste-pubsub.topic :as topic]
            [taoensso.timbre :as timbre])
  (:import [com.google.api.gax.rpc NotFoundException]
           [com.google.cloud ServiceOptions]
           [com.google.cloud.pubsub.v1 MessageReceiver AckReplyConsumer Subscriber SubscriptionAdminClient]
           [com.google.pubsub.v1 PubsubMessage ProjectSubscriptionName PushConfig TopicName]
           [com.google.api.gax.batching FlowControlSettings]))


(defn ^MessageReceiver build-receiver [handler]
  (reify MessageReceiver
    (^void receiveMessage [_ ^PubsubMessage message ^AckReplyConsumer consumer]
      (try
        (if-let [edn-msg (try (-> message .getData .toStringUtf8 edn/decode)
                              (catch Exception e
                                (timbre/error e "Could not read message data as edn")))]
          (if (handler edn-msg)
            (.ack consumer)
            ;; Handler signaled that the operation failed (temporarily)
            (.nack consumer))
          (do (timbre/error "No data present in Pub/Sub message id" (.getMessageId message))
              (.ack consumer)))
        (catch Exception e
          ;; Unhandled Exception in handler. Should not occur, message is nacked so that data is not lost
          (.nack consumer)
          (timbre/error e "Pub/Sub receiver error handling message" (.getMessageId message)))))))


(defn setup-subscription [^SubscriptionAdminClient client
                          ^ProjectSubscriptionName subscription
                          ^TopicName topic
                          ^long ack-deadline-seconds]
  (try
    (.getSubscription client subscription)
    (catch NotFoundException _
      (.createSubscription client subscription topic (PushConfig/getDefaultInstance) ack-deadline-seconds))))


(defn build-subscriber
  [{:keys [project-id topic-admin subscription-admin channel-provider credentials-provider]}
   topic-name
   handler]
  (try
    (let [project-id   (or project-id (ServiceOptions/getDefaultProjectId))
          topic        (TopicName/of project-id topic-name)
          subscription (ProjectSubscriptionName/of project-id (str topic-name "-subscription"))
          _            (topic/setup-topic topic-admin topic)
          _            (setup-subscription subscription-admin subscription topic 30)
          flow-control (-> (FlowControlSettings/newBuilder)
                           (.setMaxOutstandingElementCount 30)
                           (.build))
          receiver     (build-receiver handler)
          subscriber   (-> (Subscriber/newBuilder subscription receiver)
                           (.setChannelProvider channel-provider)
                           (.setCredentialsProvider credentials-provider)
                           (.setFlowControlSettings flow-control)
                           (.build))]
      (timbre/info "Creating subscriber for subscription" (.toString subscription))
      (-> subscriber
          (.startAsync)
          (.awaitRunning))
      subscriber)
    (catch Throwable t
      (timbre/error t "Could not init Pub/Sub subscriber for" topic-name))))
