package com.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

class ProducerCallback implements Callback{

    @Override
    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
        System.out.println("ha terminado con el offset "+ recordMetadata.offset());
    }
}
public class AsyncProducer {
    public static void main( String[] args ) throws InterruptedException {

        System.out.println( "my first producer" );
        //existen tres propierdades fundamentales
        Properties properties = new Properties();

        //hay 3 que son obligatorias: 1. donde nos conectamos 2 y 3 : serliazadores de clave y valor
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"iprocuratio.com:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        //crear un productor
        /**
         * al productor le decimos como vamos a seriliazrlo, si es algo custom pues sería
         * lo que se ha realizado por nuestra parte
         *
         */
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(properties);
        //crear un kafka producer
        // la key el sentido que tiene es que vaya
        //más ordenado , es decir con la misma key, calcula más
        // o menos a que partición.

        ProducerRecord<String, String> record = new ProducerRecord<>("partitions", "clase","Monica con key");

        //envio mensajese sincrono // se genera un objeto future a futuro. con get
        // objeto future es una promesa- Es un objeto que se resolverá. Promesa sync await

        kafkaProducer.send(record,new ProducerCallback());

        //el objeto de callback al generar promesas, lo que se hace ejecuta el send con el record
        // y al terminar    ejecuta el callback
        System.out.println("enviar ejecutardo");
        Thread.sleep(3000L);
    }
}
