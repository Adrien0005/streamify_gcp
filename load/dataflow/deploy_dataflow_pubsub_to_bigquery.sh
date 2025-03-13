# gcloud dataflow flex-template run mqtt-to-pubsub \
#   --project=dt-dta-adrien-sandbox-dev \
#   --region=us-central1 \
#   --template-file-gcs-location=gs://dataflow-templates-us-central1/latest/flex/Mqtt_to_PubSub \
#   --parameters inputTopic=/alallemand/iot/home_weather,brokerServer=tcp://broker.mqtt.cool:1883,outputTopic=projects/dt-dta-$
#   --disable-public-ips \
#   --enable-streaming-engine
