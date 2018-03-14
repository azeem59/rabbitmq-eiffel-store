/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rabbitmqvici;



import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.util.JSON;

import com.rabbitmq.client.*;

import java.io.IOException;

public class Recv {

  private final static String QUEUE_NAME = "vici";

  public static void main(String[] argv) throws Exception {
      Mongo mongo = new Mongo("localhost", 3001);
			DB db = mongo.getDB("meteor");
			DBCollection collection = db.getCollection("eiffel-events");

			// convert JSON to DBObject directly
			
    ConnectionFactory factory = new ConnectionFactory();
    factory.setHost("localhost");
    Connection connection = factory.newConnection();
    Channel channel = connection.createChannel();

    channel.queueDeclare(QUEUE_NAME, false, false, false, null);
    System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

    Consumer consumer = new DefaultConsumer(channel) {
      @Override
      public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body)
          throws IOException {
        String message = new String(body);
       DBObject dbObject = (DBObject) JSON.parse(message);
       collection.insert(dbObject);
       

        System.out.println(" [x] Received " + dbObject + "");
      }
    };
    channel.basicConsume(QUEUE_NAME, true, consumer);
  }
}





