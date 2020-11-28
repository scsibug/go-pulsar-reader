package main

import (
	"context"
	"fmt"
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/linkedin/goavro"
	"log"
	"time"
)

func main() {
	codec, err := goavro.NewCodec(`
{
      "name": "Reading",
      "type": "record",
      "fields": [
        {
          "name": "value",
          "type": [
            "null",
            "float"
          ]
        }
      ]
    }`)
    if err != nil {
        fmt.Println(err)
    }
	if codec != nil {
		fmt.Println("codec is well formed")
	}
	
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL:               "pulsar://ektar.wellorder.net:6650",
		OperationTimeout:  30 * time.Second,
		ConnectionTimeout: 30 * time.Second,
	})
	if err != nil {
		log.Fatalf("Could not instantiate Pulsar client: %v", err)
	}
	consumer, err := client.Subscribe(pulsar.ConsumerOptions{
		Topic:            "homeiot/prod/ektar-zw095-power",
		SubscriptionName: "my-sub1",
		Type:             pulsar.Shared,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer consumer.Close()

	for  {
		msg, err := consumer.Receive(context.Background())
		if err != nil {
			log.Fatal(err)
		}
		
		//fmt.Printf("Received message msgId: %#v -- content: '%s'\n",
		//			msg.ID(), string(msg.Payload()))

		native, _, err := codec.NativeFromBinary(msg.Payload())
		if err != nil {
			fmt.Println(err)
		}
		// Convert native Go form to textual Avro data
		//textual, err := codec.TextualFromNative(nil, native)
		//if err != nil {
		//   fmt.Println(err)
		//}

		// NOTE: Textual encoding will show all fields, even those with values that
		// match their default values
		record := native.(map[string]interface{})["value"]
		floatval := record.(map[string]interface{})["float"].(float32)
		fmt.Printf("%s : %f watts\n", msg.PublishTime().Format(time.UnixDate), floatval)

//		valmap := native.(map[string]interface{})
//		fmt.Printf("%v\n", native)
//		fmt.Println(string(textual))
//		fmt.Println("\n")
		consumer.Ack(msg)
	}

	if err := consumer.Unsubscribe(); err != nil {
		log.Fatal(err)
	}
	defer client.Close()
}
