package com.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
class ProducerCallback4Partition implements Callback {

    @Override
    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
        System.out.println("ha terminado con el offset "+ recordMetadata.offset() + " la partición "+ recordMetadata.partition());
    }
}
public class Partition {
    public static void main( String[] args ) throws InterruptedException {

        System.out.println( "my first producer" );
        //existen tres propierdades fundamentales
        Properties properties = new Properties();

        //hay 3 que son obligatorias: 1. donde nos conectamos 2 y 3 : serliazadores de clave y valor
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        //Linger up to 100 ms before sending batch if size not met
        properties.put(ProducerConfig.LINGER_MS_CONFIG, 100);

        //Batch up to 64K buffer sizes.
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG,  16_384 * 4);
        //crear un productor
        /**
         * al productor le decimos como vamos a seriliazrlo, si es algo custom pues sería
         * lo que se ha realizado por nuestra parte
         *
         */
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(properties);
        //crear un kafka producer
        ProducerRecord<String, String> record = new ProducerRecord<>("partition",2,"key","1");


        //envio mensajese sincrono // se genera un objeto future a futuro. con get
        // objeto future es una promesa- Es un objeto que se resolverá. Promesa sync await

        kafkaProducer.send(record,new ProducerCallback4Partition());
        ProducerRecord<String, String> record2 = new ProducerRecord<>("partition","2");


        //envio mensajese sincrono // se genera un objeto future a futuro. con get
        // objeto future es una promesa- Es un objeto que se resolverá. Promesa sync await

        kafkaProducer.send(record2,new ProducerCallback4Partition());
        ProducerRecord<String, String> record3 = new ProducerRecord<>("partition","2");


        //envio mensajese sincrono // se genera un objeto future a futuro. con get
        // objeto future es una promesa- Es un objeto que se resolverá. Promesa sync await

        kafkaProducer.send(record3,new ProducerCallback4Partition());

        //el objeto de callback al generar promesas, lo que se hace ejecuta el send con el record
        // y al terminar    ejecuta el callback
        System.out.println("enviar ejecutardo");
        Thread.sleep(3000L);
    }
}
