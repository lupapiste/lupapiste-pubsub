(ns lupapiste-pubsub.protocol)

(defprotocol MessageQueueClient
  (publish [this topic-name message])
  (subscribe [this topic-name])
  (get-publisher [this topic-name])
  (halt [this]))
