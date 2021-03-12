(defproject lupapiste/pubsub-client "1.0.1"
  :description "Common GCP Pub/Sub utils for Lupapiste"
  :url "https://github.com/cloudpermit/lupapiste-pubsub"
  :dependencies [[com.google.cloud/google-cloud-core "1.94.2"]
                 [com.google.cloud/google-cloud-pubsub "1.111.4"]
                 [com.taoensso/timbre "4.10.0"]
                 [org.clojure/tools.reader "1.3.5"]]
  :profiles {:dev {:dependencies [[org.clojure/clojure "1.10.2"]]}}
  :deploy-repositories [["releases" {:url           "https://maven.pkg.github.com/cloudpermit/lupapiste-pubsub"
                                     :username      :env/cloudpermit_github_username
                                     :password      :env/cloudpermit_github_token
                                     :sign-releases false}]]
  )
