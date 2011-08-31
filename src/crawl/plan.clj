(ns crawl.plan
  (:use somnium.congomongo))

(defn connect-db []
  (mongo!
   :db "medicare"))

(defn insert-plan [
		   id               ;; H3924-045-0
		   plan_name_long   ;; ?
		   contract_name    ;; Geisinger Gold Preferred 2 (PPO)
		   plan_type        ;; ma | mapd | pdp
		   cover_drugs      ;; true | false
		   fips             ;;
		   ]
  " return true if non-exist plan "
  (let [oldValue (fetch-one :plans :where { :_id id })
	ary (.split id "-")
	contract_h_name (nth ary 0)
	plan_name (nth ary 1)
	segment (nth ary 2)]
    (if (nil? oldValue)
      (do
	(insert! :plans
		 { :_id id
		  :contract_h_name contract_h_name
		  :plan_name plan_name
		  :segment segment
		  :contract_name contract_name
		  :plan_name_long plan_name_long
		  :plan_type plan_type
		  :cover_drugs cover_drugs
		  :fips fips
		  :event-id nil
		  :process_detail false
		  :benefit-detail [] })
	true)
      false)))

(defn mark-completed [plan]
  (update! :plans
	   {:_id plan}
	   {:$set {:process_detail true}}))

(def benefit-type {
		   "Premium and Other Important Information"	1
		   "Doctor and Hospital Choice"	2
		   "Inpatient Hospital Care"	3
		   "Inpatient Mental Health Care"	4
		   "Skilled Nursing Facility"	5
		   "Home Health Care"	6
		   "Hospice"	7
		   "Doctor Office Visits"	8
		   "Chiropractic Services"	9
		   "Podiatry Services"	10
		   "Outpatient Mental Health Care" 11
		   "Outpatient Substance Abuse Care"	12
		   "Outpatient Services/Surgery"	13
		   "Ambulance Services"	14
		   "Emergency Care"	15
		   "Urgently Needed Care"	16
		   "Outpatient Rehabilitation Services"	17
		   "Durable Medical Equipment"	18
		   "Prosthetic Devices"	19
		   "Diabetes Self-Monitoring Training and Supplies" 20
		   "Diagnostic Tests, X-Rays, and Lab Services"	21
		   "Bone Mass Measurement"	22
		   "Colorectal Screening Exams"	23
		   "Immunizations"	24
		   "Mammograms (Annual Screening)"	25
		   "Pap Smears and Pelvic Exams"	26
		   "Prostate Cancer Screening Exams"	27
		   "Prescription Drugs"	29
		   "Dental Services" 30
		   "Hearing Services"	31
		   "Vision Services"	32
		   "Physical Exams"	33
		   "Health/Wellness Education"	34
		   "Transportation"	35
		   "Acupuncture"	36
		   "Point of Service"	37
		   "End-Stage Renal Disease (ESRD)"	28
		   })

(defn insert-plan-detail [plan_id key value]
  (let [cat_id (get benefit-type key)]
    (update! :plans
	     {:_id plan_id}
	     {:$push {:benefit-detail {
				       :name key
				       :value value
				       :cat_id cat_id}}})))

(defn insert-plan-detail-2 [plan_id key value]
  (let [cat_id (get benefit-type key)]
    (update! :plans
	     {:_id plan_id}
	     {:$push {:benefit-detail2 {
				       :name key
				       :value value
				       :cat_id cat_id}}})))
	    
	       
(defn un-process-plans []
  (fetch :plans :where {:process_detail false}))

(defn escape-sql [txt]
  (.. txt
      (replace "'" "''")
      (replace "\n" "\\n")))

(defn plan-sql [plan]
  (format "insert into plan (contract_h_name, plan_name, segment, contract_name, plan_name_long, plan_type, cover_drugs, fips) values ('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')"
	  (:contract_h_name plan)
	  (:plan_name plan)
	  (:segment plan)
	  (:contract_name plan)
	  (:plan_name_long plan)
	  (:plan_type plan)
	  (:cover_drugs plan)
	  (:fips plan)))

(defn benefit-sql [plan]
  (let [contract_h_name (:contract_h_name plan)
	plan_name (:plan_name plan)
	segment (:segment plan)
	fips (:fips plan)
	lid (:event-id plan)]
    (map (fn [benefit] (format "INSERT INTO [mds_1_6_r3].[mds_schema].[MDS_WC_TMP_MOC_BENEFIT] ([LOAD_EVENT_ID], [CONTRACT_H_NAME], [PLAN_NAME], [SEGMENT], [CATEGORYID], [CATEGORYNAME], [BENEFIT_STAT], [FROMFIPS]) values ('%s', '%s', '%s', '%s', %s, '%s', '%s', '%s');\nGO\n"
			       lid
			       contract_h_name
			       plan_name
			       segment
			       (if (nil? (:cat_id benefit)) "NULL" (format "'%s'" (:cat_id benefit)))
			       (:name benefit)
			       (escape-sql (:value benefit))
			       fips)) (:benefit-detail plan))))

(defn sql [plan]
  (let [psql (plan-sql plan)
	bsqls (benefit-sql plan)]
    (concat [psql] bsqls)))

(defn dump-sql []
  (let [plans (fetch :plans :where {:process_detail true})]
    (map benefit-sql plans)))

(defn get-extracted-fip []
  (let [fip-set (java.util.HashSet.)]
    (doseq [code (map :fips (fetch :plans))]
      (.add fip-set code))
    fip-set))

(defn get-unprocess-plan []
  (let [plans (fetch :plans)
	no-detail-list (remove
			(fn [plan] (not (nil? (:process-detail plan))))
			plans)]
    (map :_id no-detail-list)))