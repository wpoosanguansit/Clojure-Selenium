(ns crawl.stat
  (:import (java.util Date)))

(defstruct thread-stat
  :id
  :type
  :begin-timestamp
  :last-timestamp
  :on-process-key
  :on-process-desc
  :processed)

(def *stat-list* (agent []))

(defn set-tid [tid]
  (def *tid* tid))

(defn stat-new-thread [type]
  (let [timestamp (Date.)
	stat (struct-map thread-stat
	       :id *tid*
	       :type type
	       :begin-timestamp timestamp
	       :last-timestamp timestamp
	       :processed [])]
    (send *stat-list* (fn [state]
			  (conj state stat)))))

(defn stat-update-fn [tid map-value]
  (fn [stat]
    (if (= (:id stat) tid)
      (merge stat (assoc map-value :last-timestamp (Date.)))
      stat)))

(defn stat-finish-fn [tid]
  (fn [stat]
    (if (= (:id stat) tid)
      (let [key (:on-process-key stat)
	    processed (:processed stat)
	    begin (:begin-timestamp stat)
	    finish (:last-timestamp stat)
	    time (- (.getTime finish) (.getTime begin))]
	(merge stat {:on-process-key nil
		     :on-process-desc nil
		     :last-timestamp (Date.)
		     :processed (conj processed (list key time finish))}
	       ))
      stat)))

(defn stat-begin-process [key desc]
  (let [func (stat-update-fn *tid*
		{:begin-timestamp (Date.)
		 :on-process-key key
		 :on-process-desc desc})]
    (send *stat-list* (fn [state] (map func state)))))

(defn stat-update-process [desc]
  (let [func (stat-update-fn *tid* {:on-process-desc desc})]
    (send *stat-list*
	  (fn [state] (map func state)))))

(defn stat-finish-process []
  (let [func (stat-finish-fn *tid*)]
    (send *stat-list*
	  (fn [state] (map func state)))))
  

(defn test-script []
  (stat-new-thread "zip")
  (println @*stat-list*)
  (stat-begin-process "1331" "1 of 4")
  (stat-update-process "2 of 4")
  (stat-update-process "3 of 4")
  (stat-finish-process )
  (stat-begin-process "2331" "1 of 4")
  (stat-update-process "2 of 4")
  (stat-update-process "3 of 4"))

(defn reset-stat []
  (send *stat-list* (fn [state] [])))
  