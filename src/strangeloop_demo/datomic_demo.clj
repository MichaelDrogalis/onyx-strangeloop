(ns strangeloop-demo.datomic-test
  (:require [clojure.core.async :refer [chan <!!]]
            [datomic.api :as d]
            [onyx.peer.task-lifecycle-extensions :as l-ext]
            [onyx.plugin.datomic]
            [onyx.plugin.core-async]
            [onyx.api]))

(def db-uri (str "datomic:mem://" (java.util.UUID/randomUUID)))

(def schema
  [{:db/id #db/id [:db.part/db]
    :db/ident :com.mdrogalis/people
    :db.install/_partition :db.part/db}

   {:db/id #db/id [:db.part/db]
    :db/ident :user/name
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db}])

(d/create-database db-uri)

(def conn (d/connect db-uri))

@(d/transact conn schema)

(def people
  [{:db/id (d/tempid :com.mdrogalis/people)
    :user/name "Mike"}
   {:db/id (d/tempid :com.mdrogalis/people)
    :user/name "Dorrene"}
   {:db/id (d/tempid :com.mdrogalis/people)
    :user/name "Chris"}
   {:db/id (d/tempid :com.mdrogalis/people)
    :user/name "Derek"}
   {:db/id (d/tempid :com.mdrogalis/people)
    :user/name "Kristen"}])

@(d/transact conn people)

(def db (d/db conn))

(def t (d/next-t db))

(def batch-size 1000)

(def id (str (java.util.UUID/randomUUID)))

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

(def query '[:find ?a :where
             [?e :user/name ?a]
             [(count ?a) ?x]
             [(<= ?x 5)]])

(defn my-test-query [{:keys [datoms] :as segment}]
  {:names (d/q query datoms)})

(def workflow {:partition-datoms {:read-datoms {:query :output}}})

(def catalog
  [{:onyx/name :partition-datoms
    :onyx/ident :datomic/partition-datoms
    :onyx/type :input
    :onyx/medium :datomic
    :onyx/consumption :sequential
    :onyx/bootstrap? true
    :datomic/uri db-uri
    :datomic/t t
    :datomic/datoms-per-segment batch-size
    :datomic/partition :com.mdrogalis/people
    :onyx/batch-size batch-size
    :onyx/doc "Creates ranges over an :eavt index to parellelize loading datoms downstream"}

   {:onyx/name :read-datoms
    :onyx/ident :datomic/read-datoms
    :onyx/fn :onyx.plugin.datomic/read-datoms
    :onyx/type :transformer
    :onyx/consumption :concurrent
    :onyx/batch-size batch-size
    :datomic/uri db-uri
    :datomic/t t
    :onyx/doc "Reads and enqueues a range of the :eavt datom index"}

   {:onyx/name :query
    :onyx/fn :strangeloop-demo.datomic-test/my-test-query
    :onyx/type :transformer
    :onyx/consumption :concurrent
    :onyx/batch-size batch-size
    :onyx/doc "Queries for names of 5 characters or fewer"}

   {:onyx/name :output
    :onyx/ident :core.async/write-to-chan
    :onyx/type :output
    :onyx/medium :core.async
    :onyx/consumption :concurrent
    :onyx/batch-size batch-size
    :onyx/doc "Writes segments to a core.async channel"}])

(def out-chan (chan 1000))

(defmethod l-ext/inject-lifecycle-resources [:output :core.async]
  [_ _] {:core-async/out-chan out-chan})

(def v-peers (onyx.api/start-peers conn 1 peer-opts))

(onyx.api/submit-job conn {:catalog catalog :workflow workflow})

(defn take-segments! [ch]
  (loop [x []]
    (let [segment (<!! ch)]
      (let [stack (conj x segment)]
        (if-not (= segment :done)
          (recur stack)
          stack)))))

(def results (take-segments! out-chan))

(clojure.pprint/pprint results)

(doseq [v-peer v-peers]
  ((:shutdown-fn v-peer)))

(onyx.api/shutdown conn)

