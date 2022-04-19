(ns lupapiste-pubsub.protocol)

(defprotocol MessageQueueClient
  (publish [this topic-name message]
    "Publish a message to the topic, which will be created if necessary. Message should be Clojure map.")
  (subscribe [this topic-name handler]
    "Subscribe to the topic, which will be created if necessary. The decoded message map will be passed to `handler`.
     Handler must return a truthy value for the message to be ACKed from the queue.
     Returning a falsy value results in a NACK and the message will be redelivered.")
  (get-publisher [this topic-name]
    "Get or create the Publisher for the given topic name.")
  (stop-subscriber [this topic-name]
    "Stops subscriber.")
  (remove-subscription [this topic-name]
    "Shuts down subscriber and removes subscription. Messages in the subscription will be lost.")
  (halt [this]
    "Shutdown all publishers and subscribers associated with this client."))
