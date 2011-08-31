(ns crawl.server
  (:use compojure.core ring.adapter.jetty hiccup.core crawl.stat)
  (:require [compojure.route :as route]))

(defn render-html [body]
  (html [:html [:head [:title "crawl stat"]
		[:style ".history {display:block;\npadding-left: 50;margin-top:20;}\n"
		 ".attribute {font-weight: 800}"]
		[:script "setInterval('window.location=window.location',12000)"]
		[:body body]]]))


(defn render-history [his]
  (let [key (nth his 0)
	duration (nth his 1)
	time (nth his 2)
	func (fn [attribute value]
	       [:div {:class "item"}
		[:span {:class "attribute"} attribute] ": "
		[:span {:class "value"} value]]
	       )]
    [:div {:class "history"}
     (func "key" key)
     (func "duration" (format "%d ms." duration))
     (func "time" time)]))

(defn render-attribute-fn [stat]
  (fn [type attribute]
    (let [value (attribute stat)]
      [type {:class "item"}
       [:span {:class "attribute"} attribute] ": "
       [:span {:class "value"} value]])))
	       
(defn render-stat [stat]
  (let [render-func (render-attribute-fn stat)]
    [:div
     (render-func :h1 :id)
     (render-func :div :type)
     (render-func :div :begin-timestamp)
     (render-func :div :last-timestamp)
     (render-func :div :on-process-key)
     (render-func :div :on-process-desc)
     [:h3 "Finished"]
     (map render-history (:processed stat))]))

(defn main-screen []
  (render-html (map render-stat @*stat-list*)))

(defroutes main-routes
  (GET "/" [] (main-screen)))

(defonce server (run-jetty #'main-routes {:port 8080 :join? false}))

(defn start-server []
  (.start server))

(defn stop-server []
  (.stop server))