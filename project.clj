(defproject lupapiste/pubsub-client "2.2.0"
  :description "Common GCP Pub/Sub utils for Lupapiste"
  :url "https://github.com/cloudpermit/lupapiste-pubsub"
  :dependencies [[com.google.cloud/google-cloud-core "2.4.0"]
                 [com.google.cloud/google-cloud-pubsub "1.115.5"]
                 [com.taoensso/timbre "5.1.2"]
                 [org.clojure/tools.reader "1.3.6"]]
  :profiles {:dev {:dependencies [[org.clojure/clojure "1.10.3"]]}}
  :deploy-repositories [["releases" {:url           "https://maven.pkg.github.com/cloudpermit/lupapiste-pubsub"
                                     :username      :env/cloudpermit_github_username
                                     :password      :env/cloudpermit_github_token
                                     :sign-releases false}]]
  )
