(ns lupapiste-pubsub.subscriber-util
  (:require [lupapiste-pubsub.edn :as edn]
            [lupapiste-pubsub.topic :as topic]
            [taoensso.timbre :as timbre])
  (:import [com.google.api.gax.rpc NotFoundException]
           [com.google.cloud ServiceOptions]
           [com.google.cloud.pubsub.v1 MessageReceiver AckReplyConsumer Subscriber SubscriptionAdminClient]
           [com.google.protobuf Duration FieldMask]
           [com.google.pubsub.v1 PubsubMessage ProjectSubscriptionName PushConfig TopicName RetryPolicy UpdateSubscriptionRequest Subscription SubscriptionName]
           [com.google.api.gax.batching FlowControlSettings]
           [com.google.api.gax.core InstantiatingExecutorProvider]
           [java.util.concurrent TimeUnit]))


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


(defn- ^RetryPolicy retry-policy []
  (-> (RetryPolicy/newBuilder)
      (.setMinimumBackoff (-> (Duration/newBuilder)
                              (.setSeconds 10)))
      (.setMaximumBackoff (-> (Duration/newBuilder)
                              (.setSeconds 240)))
      (.build)))


(defn setup-subscription [^SubscriptionAdminClient client
                          ^ProjectSubscriptionName subscription-name
                          ^TopicName topic
                          ack-deadline-seconds
                          enable-exactly-once-delivery?]
  (try
    (let [subscription (.getSubscription client subscription-name)]
      (when (or (not= (.getAckDeadlineSeconds subscription) ack-deadline-seconds)
                (nil? (.getRetryPolicy subscription)))
        (let [new-sub        (-> (.toBuilder subscription)
                                 (.setAckDeadlineSeconds ack-deadline-seconds)
                                 (.setRetryPolicy (retry-policy))
                                 (.build))
              field-mask     (-> (FieldMask/newBuilder)
                                 (.addAllPaths ["ack_deadline_seconds" "retry_policy"])
                                 (.build))
              update-request (-> (UpdateSubscriptionRequest/newBuilder)
                                 (.setSubscription new-sub)
                                 (.setUpdateMask field-mask)
                                 (.build))]
          (timbre/info "Updating subscription" (.toString subscription-name))
          (.updateSubscription client update-request))))
    (catch NotFoundException _
      (timbre/info "Creating subscription" (.toString subscription-name))
      (->> (-> (Subscription/newBuilder)
               (.setName (.toString subscription-name))
               (.setTopic (.toString topic))
               (.setPushConfig (PushConfig/getDefaultInstance))
               (.setAckDeadlineSeconds ack-deadline-seconds)
               (.setRetryPolicy (retry-policy))
               (.setEnableExactlyOnceDelivery (boolean enable-exactly-once-delivery?))
               (.build))
           (.createSubscription client)))))


(defn build-subscriber
  [{:keys [project-id topic-admin subscription-admin channel-provider credentials-provider
           thread-count ack-deadline-seconds enable-exactly-once-delivery? max-elements-per-thread]}
   topic-name
   handler]
  (try
    (let [project-id        (or project-id (ServiceOptions/getDefaultProjectId))
          topic             (TopicName/of project-id topic-name)
          subscription      (ProjectSubscriptionName/of project-id (str topic-name "-subscription"))
          _                 (topic/setup-topic topic-admin topic)
          _                 (setup-subscription subscription-admin
                                                subscription
                                                topic
                                                (or ack-deadline-seconds 300)
                                                enable-exactly-once-delivery?)
          thread-count      (or thread-count 2)
          executor-provider (-> (InstantiatingExecutorProvider/newBuilder)
                                (.setExecutorThreadCount thread-count)
                                (.build))
          max-elements      (or max-elements-per-thread 5)
          flow-control      (-> (FlowControlSettings/newBuilder)
                                (.setMaxOutstandingElementCount (* max-elements thread-count))
                                (.build))
          receiver          (build-receiver handler)
          subscriber        (-> (Subscriber/newBuilder subscription receiver)
                                (.setChannelProvider channel-provider)
                                (.setCredentialsProvider credentials-provider)
                                (.setExecutorProvider executor-provider)
                                (.setFlowControlSettings flow-control)
                                (.build))]
      (timbre/info "Creating subscriber for subscription" (.toString subscription))
      (-> subscriber
          (.startAsync)
          (.awaitRunning))
      subscriber)
    (catch Throwable t
      (timbre/error t "Could not init Pub/Sub subscriber for" topic-name))))


(defn stop-subscriber [^Subscriber sub]
  (try
    (.stopAsync sub)
    (.awaitTerminated sub 5 TimeUnit/SECONDS)
    (timbre/info (.getSubscriptionNameString sub) "terminated")
    (catch Throwable t
      (timbre/error t))))


(defn delete-subscription [{:keys [project-id subscription-admin]} topic-name]
  (let [project-id        (or project-id (ServiceOptions/getDefaultProjectId))
        subscription-name (SubscriptionName/of project-id (str topic-name "-subscription"))]
    (try
      (.deleteSubscription ^SubscriptionAdminClient subscription-admin subscription-name)
      (timbre/info "Deleted subscription" (.toString subscription-name))
      (catch NotFoundException _
        ;; ignore
        )
      (catch Throwable t
        (timbre/error t)))))
