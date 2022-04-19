(defproject lupapiste/pubsub-client "2.3.2"
  :description "Common GCP Pub/Sub utils for Lupapiste"
  :url "https://github.com/cloudpermit/lupapiste-pubsub"
  :dependencies [[com.google.cloud/google-cloud-core "2.5.11"]
                 [com.google.cloud/google-cloud-pubsub "1.116.3"]
                 [com.taoensso/timbre "5.2.1"]
                 [org.clojure/tools.reader "1.3.6"]]
  :profiles {:dev {:dependencies [[org.clojure/clojure "1.10.3"]]}}
  :deploy-repositories [["releases" {:url           "https://maven.pkg.github.com/cloudpermit/lupapiste-pubsub"
                                     :username      :env/cloudpermit_github_username
                                     :password      :env/cloudpermit_github_token
                                     :sign-releases false}]]
  )
