(defproject fi.lupapiste/pubsub-client "2.5.1"
  :description "Common GCP Pub/Sub utils for Lupapiste"
  :url "https://github.com/lupapiste/lupapiste-pubsub"
  :license {:name         "European Union Public Licence v. 1.2"
            :url          "https://joinup.ec.europa.eu/collection/eupl/eupl-text-eupl-12"
            :distribution :manual}
  :dependencies [[com.google.cloud/google-cloud-core "2.5.11"]
                 [com.google.cloud/google-cloud-pubsub "1.116.3"]
                 [com.taoensso/timbre "5.2.1"]
                 [org.clojure/tools.reader "1.3.6"]]
  :profiles {:dev {:dependencies [[org.clojure/clojure "1.10.3"]]}}
  :deploy-repositories [["clojars" {:username      :env/clojars_username
                                    :password      :env/clojars_token
                                    :sign-releases false}]]
  )
