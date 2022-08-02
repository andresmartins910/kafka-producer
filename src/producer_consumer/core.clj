(ns producer-consumer.core
  (:gen-class)
  (:import (org.apache.kafka.clients.producer KafkaProducer ProducerRecord)
           (org.apache.kafka.common.serialization StringSerializer)))

(defn run-app [topic key value]
  (let [bootstrap-server ["localhost:9092"
                          "localhost:9093"
                          "localhost:9094"] 
        producer-props {"value.serializer" StringSerializer
                        "key.serializer" StringSerializer
                        "bootstrap.servers" bootstrap-server} 
        create-msg #(ProducerRecord. topic key value) 
        key key
        value value]
    (with-open [producer (KafkaProducer. producer-props)]
      (.send producer
             (create-msg)
             (println "Message '" value "' of key '" key "' sent successfully!")))))

(defn -main []
  (run-app "name1" "1" "Hello, Apache Kafka!"))