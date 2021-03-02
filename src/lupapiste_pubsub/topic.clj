(ns lupapiste-pubsub.topic
  (:import [com.google.cloud.pubsub.v1 TopicAdminClient]
           [com.google.pubsub.v1 TopicName]
           [com.google.api.gax.rpc NotFoundException]))

(defn from-conversion-service-topic [environment]
  (str environment "-from-conversion-service"))

(defn from-integration-topic [customer-id environment]
  (str environment "-from-integration-" customer-id))

(defn from-cloud-scheduler-topic [environment]
  (str environment "-from-cloud-scheduler"))

(defn setup-topic [^TopicAdminClient client ^TopicName project-topic]
  (try
    (.getTopic client project-topic)
    (catch NotFoundException _
      (.createTopic client project-topic))))
