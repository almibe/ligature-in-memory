(defproject ligature-in-memory "0.1.0-SNAPSHOT"
  :description "An In-Memory implementation of Ligature."
  :url "https://github.com/almibe/ligature-in-memory"
  :license {:name "EPL-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.10.0"]]
  :repl-options {:init-ns org.libraryweasel.ligature.memory.core})
