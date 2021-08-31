(ns lupapiste-pubsub.bootstrap
  (:require [taoensso.timbre :as timbre]
            [clojure.java.io :as io])
  (:import [com.google.cloud.pubsub.v1 TopicAdminClient TopicAdminSettings SubscriptionAdminSettings SubscriptionAdminClient]
           [io.grpc ManagedChannelBuilder ManagedChannel]
           [com.google.api.gax.rpc FixedTransportChannelProvider]
           [com.google.api.gax.grpc GrpcTransportChannel InstantiatingGrpcChannelProvider]
           [java.util.concurrent TimeUnit]
           [com.google.api.gax.core NoCredentialsProvider GoogleCredentialsProvider BackgroundResource FixedCredentialsProvider]
           [com.google.auth.oauth2 ServiceAccountCredentials]))

(defn custom-transport-channel-provider [endpoint]
  (timbre/info "Connecting to Pub/Sub endpoint" endpoint)
  (-> (ManagedChannelBuilder/forTarget endpoint)
      (.usePlaintext)
      (.build)
      (GrpcTransportChannel/create)
      (FixedTransportChannelProvider/create)))

(defn default-transport-channel-provider []
  (timbre/info "Connecting to Pub/Sub global cloud endpoint")
  (-> (InstantiatingGrpcChannelProvider/newBuilder)
      (.build)))

(defn transport-channel-provider [endpoint]
  (if endpoint
    (custom-transport-channel-provider endpoint)
    (default-transport-channel-provider)))

(defn terminate-transport! [channel-provider]
  (when (instance? FixedTransportChannelProvider channel-provider)
    (let [tc   ^GrpcTransportChannel (.getTransportChannel channel-provider)
          chan ^ManagedChannel (.getChannel tc)]
      (.shutdown tc)
      (.shutdown chan)
      (.awaitTermination chan 5 TimeUnit/SECONDS))))

(defn topic-admin-client [{:keys [channel-provider credentials-provider]}]
  (-> (TopicAdminSettings/newBuilder)
      (.setTransportChannelProvider channel-provider)
      (.setCredentialsProvider credentials-provider)
      ^TopicAdminSettings (.build)
      (TopicAdminClient/create)))

(defn subscription-admin-client [{:keys [channel-provider credentials-provider]}]
  (-> (SubscriptionAdminSettings/newBuilder)
      (.setTransportChannelProvider channel-provider)
      (.setCredentialsProvider credentials-provider)
      ^SubscriptionAdminSettings (.build)
      (SubscriptionAdminClient/create)))

(defn shutdown-client [^BackgroundResource client]
  (.shutdown client)
  (.awaitTermination client 5 TimeUnit/SECONDS))

(defn no-credentials-provider []
  (NoCredentialsProvider/create))

(defn google-credentials-provider []
  (-> (GoogleCredentialsProvider/newBuilder)
      (.setScopesToApply ["https://www.googleapis.com/auth/cloud-platform"
                          "https://www.googleapis.com/auth/pubsub"])
      (.build)))

(defn fixed-credentials-provider [service-account-file]
  (when service-account-file
    (-> (with-open [is (io/input-stream service-account-file)]
          (ServiceAccountCredentials/fromStream is))
        (FixedCredentialsProvider/create))))
