package com.devs4j_transactions;


import com.devs4j_transactions.models.Devs4jTransaction;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.javafaker.Faker;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

import java.util.List;

@SpringBootApplication
@EnableScheduling
public class Devs4jTransactionsApplication  {

	public static final Logger log = LoggerFactory.getLogger(Devs4jTransactionsApplication.class);

	@Autowired
	private RestHighLevelClient client;

	@Autowired
	private ObjectMapper mapper;

	@Autowired
	private KafkaTemplate<String, String> kafkaTemplate;

	@KafkaListener(topics = "devs4j-transactions", groupId = "devs4j-group", containerFactory = "listenerContainerFactory")
	public void Listen(List<ConsumerRecord<String, String>> messages) throws JsonProcessingException {
		for	(ConsumerRecord<String, String> message:messages) {
			//Devs4jTransaction transaction = mapper.readValue(message.value() , Devs4jTransaction.class);
			/*log.info("Partition = {}, Offset = {}, Key = {}, Message = {}", message.partition(),
					message.offset(), message.key(), message.value());*/
			IndexRequest index = buildIndexRequest(String.format("%s-%s%s", message.partition(),
					message.key(), message.offset()) ,message.value());
			client.indexAsync(index, RequestOptions.DEFAULT, new ActionListener<IndexResponse>() {
				@Override
				public void onResponse(IndexResponse indexResponse) {
					log.debug("Successful Request");
				}

				@Override
				public void onFailure(Exception e) {
					log.error("Error storing message {} ", e);
				}
			});
		}
	}

	private IndexRequest buildIndexRequest(String key, String value) {
		IndexRequest indexRequest = new IndexRequest("devs4j-transactions");
		indexRequest.id(key);
		indexRequest.source(value, XContentType.JSON);
		return indexRequest;
	}

	@Scheduled(fixedRate = 15000)
	public void sendMessages() throws JsonProcessingException {
		Faker faker = new Faker();
		for (int i = 0; i < 10000; i++) {
			Devs4jTransaction transaction = new Devs4jTransaction();
			transaction.setUsername(faker.name().username());
			transaction.setName(faker.name().firstName());
			transaction.setLastName(faker.name().lastName());
			transaction.setAmount(faker.number().randomDouble(4, 0, 20000));
			kafkaTemplate.send("devs4j-transactions", transaction.getUsername() , mapper.writeValueAsString(transaction));
		}
	}

	public static void main(String[] args) {
		SpringApplication.run(Devs4jTransactionsApplication.class, args);
	}

	/*@Autowired
	private RestHighLevelClient client;

	@Override
	public void run(String... args) throws Exception {
		IndexRequest indexRequest = new IndexRequest("devs4j-transactions");
		indexRequest.id("44");
		indexRequest.source("{\\\"nombre\\\":\\\"Sammie\\\",\"\n" +
				"+\"\\\"apellido\\\":\\\"Goldner\\\",\"\n" +
				"+\"\\\"username\\\":\\\"hugh.vonrueden\\\",\"\n" +
				" +\"\\\"monto\\\":9622235.2009}", XContentType.JSON);
		IndexResponse response = client.index(indexRequest, RequestOptions.DEFAULT);
		log.info("Response id {} ", response.getId());
	}*/
}
