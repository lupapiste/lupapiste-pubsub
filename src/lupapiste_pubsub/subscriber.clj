(ns lupapiste-pubsub.subscriber
  (:require [lupapiste-pubsub.edn :as edn]
            [lupapiste-pubsub.topic :as topic]
            [taoensso.timbre :as timbre])
  (:import [com.google.api.gax.rpc NotFoundException]
           [com.google.cloud ServiceOptions]
           [com.google.cloud.pubsub.v1 MessageReceiver AckReplyConsumer Subscriber SubscriptionAdminClient]
           [com.google.pubsub.v1 PubsubMessage ProjectSubscriptionName PushConfig TopicName]
           [java.util.concurrent TimeUnit]))

(defn- process-response [{:keys [response-to error] :as msg}]
  (try
    (if error
      (timbre/warn "Error response to message" (str response-to) ":" error)
      (timbre/debug "Received response to message" (str response-to)))
    ;; TODO: Implement response handlers?
    (catch Exception e
      (timbre/error e "Response handler error for message" (pr-str msg)))))

(defrecord Receiver []
  MessageReceiver
  (^void receiveMessage [_ ^PubsubMessage message ^AckReplyConsumer consumer]
    (try
      (if-let [edn-msg (-> message .getData .toStringUtf8 edn/decode)]
        (if (:response-to edn-msg)
          (process-response edn-msg)
          (timbre/error "No supported action in Pub/Sub message" (pr-str edn-msg)))
        (timbre/error "No data present in Pub/Sub message id" (.getMessageId message)))
      (.ack consumer)
      (catch Exception e
        (.ack consumer)
        (timbre/error e "Pub/Sub receiver error handling message" (.getMessageId message))))))

(defn- subscribe
  [^ProjectSubscriptionName subscription channel-provider credentials]
  (let [receiver   (->Receiver)
        subscriber (-> (Subscriber/newBuilder subscription ^MessageReceiver receiver)
                       (.setChannelProvider channel-provider)
                       (.setCredentialsProvider credentials)
                       (.build))]
    (timbre/info "Creating subscriber for subscription" (.toString subscription))
    (-> subscriber
        (.startAsync)
        (.awaitRunning))
    subscriber))

(defn setup-subscription [^SubscriptionAdminClient client
                          ^ProjectSubscriptionName subscription
                          ^TopicName topic
                          ^long ack-deadline-seconds]
  (try
    (.getSubscription client subscription)
    (catch NotFoundException _
      (.createSubscription client subscription topic (PushConfig/getDefaultInstance) ack-deadline-seconds))))

(defn non-customer-topics [environment]
  [(topic/from-conversion-service-topic environment)
   (topic/from-cloud-scheduler-topic environment)])

(defn- init-subscriber
  [{:keys [credentials-provider channel-provider topic-admin subscription-admin project-id]}
   topic-str]
  (future
    (try
      (let [project-id   (or project-id (ServiceOptions/getDefaultProjectId))
            topic        (TopicName/of project-id topic-str)
            subscription (ProjectSubscriptionName/of project-id (str topic-str "-subscription"))]
        (Thread/sleep 5000) ; Sleep to let rest of the system get up before starting processing
        (topic/setup-topic topic-admin topic)
        (setup-subscription subscription-admin subscription topic 30)
        (subscribe subscription channel-provider credentials-provider))
      (catch Throwable t
        (timbre/error t "Could not init Pub/Sub subscriber for" topic-str)))))

(defn init [{:keys [customer-ids environment] :as config}]
  (concat
    (mapv #(init-subscriber config (topic/from-integration-topic % environment)) customer-ids)
    (mapv #(init-subscriber config %) (non-customer-topics environment))))

(defn halt [subscriber-futures]
  (timbre/info "Tearing down subscribers")
  (->> subscriber-futures
       (pmap (fn [sub-future]
               (when-let [^Subscriber sub @sub-future]
                 (try
                   (.stopAsync sub)
                   (.awaitTerminated sub 5 TimeUnit/SECONDS)
                   (catch Throwable t
                     (timbre/error t)))
                 (timbre/info (.getSubscriptionNameString sub) "terminated"))))
       dorun))
