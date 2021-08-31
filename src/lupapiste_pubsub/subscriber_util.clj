(ns lupapiste-pubsub.subscriber-util
  (:require [lupapiste-pubsub.edn :as edn]
            [lupapiste-pubsub.topic :as topic]
            [taoensso.timbre :as timbre])
  (:import [com.google.api.gax.rpc NotFoundException]
           [com.google.cloud ServiceOptions]
           [com.google.cloud.pubsub.v1 MessageReceiver AckReplyConsumer Subscriber SubscriptionAdminClient]
           [com.google.pubsub.v1 PubsubMessage ProjectSubscriptionName PushConfig TopicName]
           [com.google.api.gax.batching FlowControlSettings]))


(defn- process-response [{:keys [response-to error] :as msg}]
  (try
    (if error
      (timbre/warn "Error response to message" (str response-to) ":" error)
      (timbre/debug "Received response to message" (str response-to)))
    ;; TODO: Implement response handlers?
    (catch Exception e
      (timbre/error e "Response handler error for message" (pr-str msg)))))


(defn receive-message [_ ^PubsubMessage message ^AckReplyConsumer consumer]
  (try
    (if-let [edn-msg (-> message .getData .toStringUtf8 edn/decode)]
      (if (:response-to edn-msg)
        (process-response edn-msg)
        (timbre/error "No supported action in Pub/Sub message" (pr-str edn-msg)))
      (timbre/error "No data present in Pub/Sub message id" (.getMessageId message)))
    (.ack consumer)
    (catch Exception e
      (.ack consumer)
      (timbre/error e "Pub/Sub receiver error handling message" (.getMessageId message)))))


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
   ^MessageReceiver receiver]
  (try
    (let [project-id   (or project-id (ServiceOptions/getDefaultProjectId))
          topic        (TopicName/of project-id topic-name)
          subscription (ProjectSubscriptionName/of project-id (str topic-name "-subscription"))
          _            (topic/setup-topic topic-admin topic)
          _            (setup-subscription subscription-admin subscription topic 30)
          flow-control (-> (FlowControlSettings/newBuilder)
                           (.setMaxOutstandingElementCount 30)
                           (.build))
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
