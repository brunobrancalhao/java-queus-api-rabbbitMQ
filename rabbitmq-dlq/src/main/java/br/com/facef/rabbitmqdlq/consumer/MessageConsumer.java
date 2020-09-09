package br.com.facef.rabbitmqdlq.consumer;

import br.com.facef.rabbitmqdlq.configuration.DirectExchangeConfiguration;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.AmqpRejectAndDontRequeueException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;

import java.util.ArrayList;
import java.util.Map;

@Configuration
@Slf4j
public class MessageConsumer {

  @Autowired
  private RabbitTemplate rabbitTemplate;

  @RabbitListener(queues = DirectExchangeConfiguration.ORDER_MESSAGES_QUEUE_NAME)
  public void processOrderMessage(Message message) {
    log.info("Processing message: {}", message.toString());
    // By default the messages will be requeued
    throw new RuntimeException("Business Rule Exception");
    // To dont requeue message can throw AmqpRejectAndDontRequeueException
    //throw new AmqpRejectAndDontRequeueException("Business Rule Exception");
  }

  @RabbitListener(queues = DirectExchangeConfiguration.ORDER_MESSAGES_QUEUE_DLQ_NAME)
  public void processOrderMessageDlq(Message message) {
    log.info("Processing message DQL: {}", message.toString());

    try {
      throw new RuntimeException("Business Rule Exception");

    } catch (Exception e) {

      Map<String, Object> headers = message.getMessageProperties().getHeaders();

      Integer retriesHeader = (Integer) headers.get(DirectExchangeConfiguration.X_RETRIES_HEADER);

      log.info("count-fails: {}", retriesHeader);

      if (retriesHeader == null) {
        retriesHeader = Integer.valueOf(0);
      }

      if (retriesHeader >= 5) {
        this.rabbitTemplate.send(DirectExchangeConfiguration.ORDER_MESSAGES_QUEUE_PARKING_LOT_NAME, message
        );
      } else {
        headers.put(DirectExchangeConfiguration.X_RETRIES_HEADER, retriesHeader + 1);
        this.rabbitTemplate.send(DirectExchangeConfiguration.ORDER_MESSAGES_QUEUE_DLQ_NAME, message);
      }
    }
  }

  @RabbitListener(queues = DirectExchangeConfiguration.ORDER_MESSAGES_QUEUE_PARKING_LOT_NAME)
  public void processOrderMessageParkingLot(Message message) {
    log.info("Processing message ParkingLot: {}", message.toString());
  }


}
