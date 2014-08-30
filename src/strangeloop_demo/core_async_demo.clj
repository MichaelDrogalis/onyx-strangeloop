(ns strangeloop-demo.core-async-demo
  (:require [clojure.core.async :refer [chan <!! >!! close!]]
            [onyx.peer.task-lifecycle-extensions :as l-ext]
            [onyx.plugin.core-async]
            [onyx.api]))

(def workflow {:input {:increment :output}})

(def catalog
  [{:onyx/name :input
    :onyx/ident :core.async/read-from-chan
    :onyx/type :input
    :onyx/medium :core.async
    :onyx/consumption :concurrent
    :onyx/batch-size 25
    :onyx/doc "Reads segments from a core.async channel"}

   {:onyx/name :increment
    :onyx/fn :strangeloop-demo.core-async-demo/increment
    :onyx/type :transformer
    :onyx/consumption :concurrent
    :onyx/batch-size 25}

   {:onyx/name :output
    :onyx/ident :core.async/write-to-chan
    :onyx/type :output
    :onyx/medium :core.async
    :onyx/consumption :concurrent
    :onyx/batch-size 25
    :onyx/doc "Writes segments to a core.async channel"}])

(defn increment [segment]
  (update-in segment [:n] inc))

(def in-chan (chan 10000))

(def out-chan (chan 10000))

(defmethod l-ext/inject-lifecycle-resources [:input :core.async]
  [_ _] {:core-async/in-chan in-chan})

(defmethod l-ext/inject-lifecycle-resources [:output :core.async]
  [_ _] {:core-async/out-chan out-chan})

(def id (java.util.UUID/randomUUID))

(def coord-opts
  {:hornetq/mode :vm
   :hornetq/server? true
   :hornetq.server/type :vm
   :zookeeper/address "127.0.0.1:2185"
   :zookeeper/server? true
   :zookeeper.server/port 2185
   :onyx/id id
   :onyx.coordinator/revoke-delay 5000})

(def peer-opts
  {:hornetq/mode :vm
   :zookeeper/address "127.0.0.1:2185"
   :onyx/id id})

(def conn (onyx.api/connect :memory coord-opts))

(def n-segments 10)

(doseq [n (range n-segments)]
  (>!! in-chan {:n n}))

(>!! in-chan :done)

(close! in-chan)

(def v-peers (onyx.api/start-peers conn 1 peer-opts))

(onyx.api/submit-job conn {:catalog catalog :workflow workflow})

(def results (doall (map (fn [x] (<!! out-chan)) (range (inc n-segments)))))

(clojure.pprint/pprint results)

(doseq [v-peer v-peers]
  ((:shutdown-fn v-peer)))

(onyx.api/shutdown conn)

