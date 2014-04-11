package net.mrkzea.solace;

import javax.jms.*;
import java.util.HashMap;
import java.util.Map;



/* Example implementation for producer/consumer */
public class ProducerConsumerImpl extends AbstractProducerConsumer {


    public ProducerConsumerImpl(Map<String, String> solaceProperties) {
        super(solaceProperties);
    }


    @Override
    public void produce() {
        try {

            String something = "Something " + System.currentTimeMillis();
            System.out.println("Producer produced: " + something);

            TextMessage txtMsg = session.createTextMessage();
            txtMsg.setText(something);

            producer.send(txtMsg);

        } catch (JMSException e) {
            e.printStackTrace();
        }
    }



    @Override
    public void onMessage(Message message) {
        if (!(message instanceof TextMessage)) {
            return;
        }
        TextMessage textMessage = (TextMessage) message;
        try {
            System.out.println("Consumer Received: " + textMessage.getText());
        } catch (JMSException e) {
            e.printStackTrace();
        }
    }


    public static void main(String[] args) {
        Map<String, String> properties = new HashMap<String, String>();
        properties.put("host", "tcp:solace-host.com");
        properties.put("port", "55555");
        properties.put("username", "test");
        properties.put("password", "test");
        properties.put("VPN", "test_env");
        properties.put("queue", "queue.Test");
        properties.put("messages_to_produce", "10");
        new ProducerConsumerImpl(properties);
    }

}
