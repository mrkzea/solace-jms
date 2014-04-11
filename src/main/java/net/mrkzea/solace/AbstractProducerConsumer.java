package net.mrkzea.solace;

import com.solacesystems.jms.SolConnectionFactoryImpl;
import com.solacesystems.jms.property.JMSProperties;

import javax.jms.*;
import java.util.Hashtable;
import java.util.Map;


/* Simple client to produce and consume from solace queues */
public abstract class AbstractProducerConsumer extends Thread implements MessageListener {


    private SolConnectionFactoryImpl connFactory;
    private Connection conn;
    protected MessageProducer producer;
    protected MessageConsumer consumer;
    protected Session session;
    int waitToProduceMilli = 1000;
    private String queueName;
    private boolean isProducerEnabled;
    private boolean isConsumerEnabled;
    private int nrOfMessagesLimit;


    public AbstractProducerConsumer(Map<String, String> solaceProperties) {
        try {
            connFactory = new SolConnectionFactoryImpl(new JMSProperties(new Hashtable()));
            connFactory.setHost(solaceProperties.get("host"));
            connFactory.setPort(Integer.valueOf(solaceProperties.get("port")));
            connFactory.setUsername(solaceProperties.get("username"));
            connFactory.setPassword(solaceProperties.get("password"));
            connFactory.setVPN(solaceProperties.get("VPN"));
            connFactory.setDirectTransport(false);
            connFactory.setDirectOptimized(false);
            this.queueName = solaceProperties.get("queue");
            this.isConsumerEnabled = solaceProperties.get("consumer_active") != null ? Boolean.valueOf(solaceProperties.get("consumer_active")) : true;
            this.isProducerEnabled = solaceProperties.get("producer_active") != null ? Boolean.valueOf(solaceProperties.get("producer_active")) : true;
            this.nrOfMessagesLimit = solaceProperties.get("messages_to_produce") != null ? Integer.valueOf(solaceProperties.get("messages_to_produce")) : 0;
            start();

        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    public void setHowLongToWaitBetweenSends(int howLongMillis) {
        this.waitToProduceMilli = howLongMillis;
    }

    public void setQueueName(String queueName) {
        this.queueName = queueName;
    }

    public void setConsumerEnabled(boolean isEnabled) {
        this.isConsumerEnabled = isEnabled;
    }

    public void setProducerEnabled(boolean isEnabled) {
        this.isProducerEnabled = isEnabled;
    }


    public void run() {

        int produced = 0;
        try {
            conn = connFactory.createConnection();
            session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue(queueName);

            if (isConsumerEnabled) {
                consumer = session.createConsumer(queue);
                consumer.setMessageListener(this);
            }
            if (isProducerEnabled) {
                producer = session.createProducer(queue);
            }

            conn.start();

        } catch (Exception e) {
            System.out.println("ERROR hey");
            e.printStackTrace();
        }


        while (true) {
            try {

                sleep(waitToProduceMilli);
                produce();

                if (nrOfMessagesLimit > 0 && produced++ >= nrOfMessagesLimit) {
                    System.out.println("Closing...");
                    producer.close();
                    sleep(1000);
                    consumer.close();
                    session.close();
                    conn.close();
                    return;
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (JMSException e) {
                e.printStackTrace();
            }
        }
    }


    public abstract void produce();

    public abstract void onMessage(Message message);


}
