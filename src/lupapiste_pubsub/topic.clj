(ns lupapiste-pubsub.topic
  (:import [com.google.cloud.pubsub.v1 TopicAdminClient]
           [com.google.pubsub.v1 TopicName]
           [com.google.api.gax.rpc NotFoundException]
           [io.grpc StatusRuntimeException Status]))


(defn env-prefix [environment topic]
  (str environment "-" topic))


(defn setup-topic [^TopicAdminClient client ^TopicName project-topic]
  (try
    (.getTopic client project-topic)
    (catch NotFoundException _
      (try (.createTopic client project-topic)
           (catch StatusRuntimeException ex
             (when-not (= (.getStatus ex) Status/ALREADY_EXISTS)
               ;; Ignore already exists, probably tried to create at the same time from multiple threads
               (throw ex)))))))
