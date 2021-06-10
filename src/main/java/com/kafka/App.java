package com.kafka;

import kafka.Kafka;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
        
        System.out.println( "my first producer" );
        //existen tres propierdades fundamentales
        Properties properties = new Properties();

        //hay 3 que son obligatorias: 1. donde nos conectamos 2 y 3 : serliazadores de clave y valor
        properties.put("bootstrap.servers","localhost:9092");
        properties.put("key.serializer", StringSerializer.class.getName());
        properties.put("value.serializer",StringSerializer.class.getName());
        //crear un productor
        /**
         * al productor le decimos como vamos a seriliazrlo, si es algo custom pues sería
         * lo que se ha realizado por nuestra parte
         *
         */
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(properties);
        //crear un kafka producer
        ProducerRecord<String, String> record = new ProducerRecord<>("test","hola mudo");

        //envio mensajese sincrono // se genera un objeto future a futuro. con get
        // objeto future es una promesa- Es un objeto que se resolverá. Promesa sync await

        try {
            RecordMetadata respuesta = kafkaProducer.send(record).get();
            System.out.println("offset: "+ respuesta.offset() + " partion" + respuesta.partition());
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }
}
