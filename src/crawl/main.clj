(ns crawl.main
  (:use crawl.core
	crawl.plan
	crawl.data
	somnium.congomongo
	crawl.stat
	crawl.server
	clojure.contrib.duck-streams)
  (:import
   (java.util.concurrent LinkedBlockingQueue)
   (org.openqa.selenium.chrome ChromeDriver ChromeNotRunningException)
   (org.openqa.selenium StaleElementReferenceException)))

(def *fips-queue* (LinkedBlockingQueue.))

(def *plan-queue* (LinkedBlockingQueue.))

(def *error-fips-queue* (LinkedBlockingQueue.))

(defn test-update-stat []
  (stat-update-process "hello"))

(defn test-binding []
  (.start
   (Thread.
    (fn []
      (binding [*tid* (.toString (rand))]
	(stat-new-thread "hi")
	(test-update-stat)))))
  (.start
   (Thread.
    (fn []
      (binding [*tid* (.toString (rand))]
	(stat-new-thread "hi")
	(test-update-stat))))))


(defn new-extract-by-fips-task []
  (.start
   (Thread.
    (fn []
      (binding [*driver* (ChromeDriver.)
		*tid* (.toString (rand))]
	(mongo! :db "medicare" :host "127.0.0.1")
	;;(stat-new-thread "zip")
	(doseq [fip (repeatedly #(.take  *fips-queue*))]
	  (try (doseq [plan (remove nil? (extract-plan-by-fip fip))]
		 (.put *plan-queue* plan))
	       (catch ChromeNotRunningException ce
		 (throw ce))
	       (catch Exception e
		 (.put *error-fips-queue* {:e e :fip fip})))
	  (Thread/sleep 1000)))))))

(defn new-extract-benefit-task []
  (.start
   (Thread.
    (fn []
      (binding [*driver* (ChromeDriver.)
		*tid* (.toString (rand))]
	(mongo! :db "medicare" :host "127.0.0.1")
	;;(stat-new-thread "plan")
	(goto-plan-result)
	(doseq [plan (repeatedly #(.take *plan-queue*))]
	  ;;(stat-begin-process plan "go-plan")
	  (go-plan plan)
	  ;;(stat-update-process "save-benefit")
	  (save-benefits plan)
	  (mark-completed plan)
	  ))))))

(defn monitor-start []
  (start-server))

(defn queue-all-fips []
  (doseq [fip *fips-zips*]
    (.put *fips-queue* fip)))

(defn reset-db []
  (mongo! :db "medicare" :host "127.0.0.1")
  (destroy! :plans {}))


(defn queue-unprocess-fips []
  (mongo! :db "medicare" :host "127.0.0.1")
  (let [processed-fip (get-extracted-fip)]
    (doseq [fip-zip *fips-zips*]
      (let [fip (first fip-zip)]
	(if-not (.contains processed-fip fip)
	  (.put *fips-queue* fip-zip))))))


(defn queue-unprocess-plan []
  (mongo! :db "medicare" :host "127.0.0.1")
  (doseq [plan (fetch :plans :where {:process_detail false})]
    (.put *plan-queue* (:_id plan))))

(defn queue-unprocess-2-plan []
  (mongo! :db "medicare" :host "127.0.0.1")
  (doseq [plan (fetch :plans :where {:benefit-detail2 nil})]
    (.put *plan-queue* (:_id plan))))


(defn get-stale-error []
  (let [stale
	(filter (fn [item]
		  (instance? StaleElementReferenceException (:e item)))
		*error-fips-queue*)]
    (map :fip stale)))

(defn queue-unprocess-from-stale []
  (doseq [stale (get-stale-error)]
    (.put *fips-queue* stale)))

(defn get-timeout-error []
  (let [to
	(filter (fn [item]
		  (re-find #"timeout" (.getMessage (:e item))))
		*error-fips-queue*)]
    (map :fip to)))

(defn dump-sql-to-file [file]
  (mongo! :db "medicare" :host "127.0.0.1")
  (doseq [sqlall (dump-sql)]
    (doseq [stmt sqlall]
      (append-spit file stmt))))
