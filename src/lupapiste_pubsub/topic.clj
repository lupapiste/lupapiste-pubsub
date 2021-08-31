(ns lupapiste-pubsub.topic
  (:import [com.google.cloud.pubsub.v1 TopicAdminClient]
           [com.google.pubsub.v1 TopicName]
           [com.google.api.gax.rpc NotFoundException]))


(defn env-prefix [environment topic]
  (str environment "-" topic))


(defn setup-topic [^TopicAdminClient client ^TopicName project-topic]
  (try
    (.getTopic client project-topic)
    (catch NotFoundException _
      (.createTopic client project-topic))))
