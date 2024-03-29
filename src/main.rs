use std::str::FromStr;
use std::sync::atomic::AtomicI64;
use std::sync::atomic::Ordering;

use clap::{value_t, App, Arg};
use futures::stream::FuturesUnordered;
use futures::{StreamExt, TryStreamExt};
use log::info;
use serde_json::Value;
use testcontainers::clients::Cli;
use tokio_util::sync::CancellationToken;
use std::sync::Arc;

use rdkafka::config::ClientConfig;
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::Consumer;
use rdkafka::message::{BorrowedMessage, OwnedMessage};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::Message;
use scylla::{QueryResult, Session, SessionBuilder};
use scylla::frame::value::CqlTimeuuid;

async fn run_async_processor(
    brokers: String,
    group_id: String,
    input_topic: String,
    scylla_uri: String,
    atomic_counter: Arc<AtomicI64>,
    token: CancellationToken
) {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("group.id", &group_id)
        .set("bootstrap.servers", &brokers)
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .create()
        .expect("Consumer creation failed");

    consumer
        .subscribe(&[&input_topic])
        .expect("Can't subscribe to specified topic");

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .set("message.timeout.ms", "5000")
        .create()
        .expect("Producer creation error");

    let thread_reference_aware_session: Arc<Session> = Arc::new(SessionBuilder::new()
        .known_node(&scylla_uri)
        .build()
        .await
        .unwrap());

    let insert_query = "INSERT INTO store.users (id,first_name,last_name,email,ip_address) VALUES(?,?,?,?,?)";

    let stream_processor = consumer.stream().try_for_each(
        |borrowed_message| {
        let _producer = producer.clone();
        let session = thread_reference_aware_session.clone();
        let inner_atomic_counter = Arc::clone(&atomic_counter);
        let token = token.clone();
        async move {
            // Borrowed messages can't outlive the consumer they are received from, so they need to
            // be owned in order to be sent to a separate thread.
            let owned_message = borrowed_message.detach();

            let global_counter = &format!("8e14e760-7fa8-11eb-bc66-{}", 
                format!("{:0>12}", inner_atomic_counter.fetch_add(1 , Ordering::SeqCst)));
            let id = CqlTimeuuid::from_str(global_counter).unwrap();

            let _dummy_result_stirng = match &owned_message.payload_view::<str>() {
                Some(Ok(kafka_payload)) => {
                    println!("KAFKA PAYLOAD: {}", kafka_payload);
                    let json: Value = serde_json::from_str(&kafka_payload).unwrap();
                    let first_name = json["first_name"].as_str().unwrap();
                    let last_name = json["last_name"].as_str().unwrap();
                    let email = json["email"].as_str().unwrap();
                    let ip_address =json["ip_address"].as_str().unwrap();

                    match session.query(
                        insert_query, 
                            (&id, &first_name, &last_name, &email, &ip_address))
                            .await {
                                    Ok(_r) => {
                                        // hack:
                                        if first_name == "end" {
                                            println!("Done!");
                                            token.cancel();
                                        }
                                        // scylla -> kafka to be done via CDC

                                        // let produce_future = producer.send(
                                        //     FutureRecord::to(&output_topic)
                                        //         .key("some key")
                                        //         .payload(*kafkaPayload),
                                        //     Duration::from_secs(0),
                                        // );
                                        // match produce_future.await {
                                        //     Ok(delivery) => {}, //println!("Sent: {:?}", delivery),
                                        //     Err((e, _)) => println!("Error: {:?}", e),
                                        // }
                                        String::from("POSSIBLY_OK")
                                    },
                                    Err(err) => {
                                        println!("OH NOES!! {:?}", err);
                                        String::from("BAD_TIMES")
                                    }
                                }
                },
                Some(Err(_)) => "Message payload is not a string".to_owned(),
                None => "No payload".to_owned(),
            };
            Ok(())
        }
    });

    info!("Starting event loop");
    stream_processor.await.expect("stream processing failed");
    info!("Stream processing terminated");
}

#[tokio::main]
async fn main() {
    let matches = App::new("Async example")
        .version(option_env!("CARGO_PKG_VERSION").unwrap_or(""))
        .about("Asynchronous computation example")
        .arg(
            Arg::with_name("brokers")
                .short("b")
                .long("brokers")
                .help("Broker list in kafka format")
                .takes_value(true)
                .default_value(option_env!("BROKERS").unwrap_or("localhost:9092")),
        )
        .arg(
            Arg::with_name("group-id")
                .short("g")
                .long("group-id")
                .help("Consumer group id")
                .takes_value(true)
                .default_value(option_env!("GROUP_ID").unwrap_or("example_consumer_group_id")),
        )
        .arg(
            Arg::with_name("log-conf")
                .long("log-conf")
                .help("Configure the logging format (example: 'rdkafka=trace')")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("input-topic")
                .long("input-topic")
                .help("Input topic")
                .takes_value(true)
                .default_value(option_env!("INPUT_TOPIC").unwrap_or("in")),
        )
        .arg(
            Arg::with_name("output-topic")
                .long("output-topic")
                .help("Output topic")
                .takes_value(true)
                //.required(true),
                .default_value(option_env!("OUTPUT_TOPIC").unwrap_or("out")),
        )
        .arg(
            Arg::with_name("num-workers")
                .long("num-workers")
                .help("Number of workers")
                .takes_value(true)
                .default_value(option_env!("NUM_WORKERS").unwrap_or("1")),
        )
        .arg(
            Arg::with_name("scylla-uri")
                .long("scylla-uri")
                .help("Number of workers")
                .takes_value(true)
                .default_value(option_env!("SCYLLA_URI").unwrap_or("localhost:9042")),
        )
        .get_matches();

    let brokers = matches.value_of("brokers").unwrap();
    let group_id = matches.value_of("group-id").unwrap();
    let input_topic = matches.value_of("input-topic").unwrap();
    let output_topic = matches.value_of("output-topic").unwrap();
    let scylla_uri = String::from(matches.value_of("scylla-uri").unwrap());
    let num_workers = value_t!(matches, "num-workers", usize).unwrap();
    let atomic_counter = Arc::new(AtomicI64::new(0));
    let token = CancellationToken::new();

    println!("brokers:{}",brokers);
    println!("group_id:{}",group_id);
    println!("input_topic:{}",input_topic);
    println!("output_topic:{}",output_topic);
    println!("num_workers:{}",num_workers);
    println!("scylla_uri:{}",scylla_uri);

    run(num_workers, String::from(brokers), group_id, input_topic, scylla_uri, atomic_counter, token).await
}

async fn run(num_workers: usize,
            brokers: String,
            group_id: &str,
            input_topic: &str,
            scylla_uri: String,
            atomic_counter: Arc<AtomicI64>,
            token: CancellationToken) {
    (0..num_workers)
    .map(|_| { 
        let ac = atomic_counter.clone();
        let t = token.clone();
        tokio::spawn(run_async_processor(
            brokers.to_owned(),
            group_id.to_owned(),
            input_topic.to_owned(),
            scylla_uri.to_owned(),
            ac,
            t
        ))
    })
    .collect::<FuturesUnordered<_>>()
    .for_each(|_| async { () })
    .await
}

#[cfg(test)]
use std::env;
use testcontainers::clients;
use testcontainers::core::{ExecCommand, WaitFor};
use testcontainers::{Container, GenericImage, RunnableImage};

#[tokio::test]
async fn it_should_store_kafka_messages_in_scylladb() {
    let topic = "test-topic";
    let topic_out = "test-topic-out";
    let docker = clients::Cli::default();
    
    let (_kafka_node, bootstrap_servers) = create_kafka_node(&docker, topic, topic_out);
    let (_scylla_node, scylla_uri) = create_scylla_node(&docker);
    

    let kafka_producer = ClientConfig::new()
        .set("bootstrap.servers", &bootstrap_servers)
        .set("message.timeout.ms", "5000")
        .create::<FutureProducer>()
        .expect("Failed to create Kafka FutureProducer");

    let scylla_session = SessionBuilder::new()
        .known_node(&scylla_uri)
        .build()
        .await
        .unwrap();

    let _keyspace_query = scylla_session.query(
        "CREATE KEYSPACE store WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};", ())
            .await
            .unwrap();
    let _table_query = scylla_session.query(
        "CREATE TABLE store.users (
            id timeuuid PRIMARY KEY,
            email ascii,
            first_name ascii,
            ip_address ascii,
            last_name ascii);", ())
            .await
            .unwrap();

    let atomic_counter = Arc::new(AtomicI64::new(0));
    let number_of_test_messages_to_produce: i64 = 5;
    let mut test_messages_to_produce: Vec<String> = (0..number_of_test_messages_to_produce)
        .map(|i| format!(r#"
            {{
                "id":{i},
                "first_name":"fil",
                "last_name":"k",
                "email":"nope@nope.com",
                "ip_address":"127.0.0.1"
            }}"#))
        .collect();
    test_messages_to_produce.push(format!(r#"
    {{
        "id":0,
        "first_name":"end",
        "last_name":"",
        "email":"",
        "ip_address":""
    }}"#));

    for (i, message) in test_messages_to_produce.iter().enumerate() {
        kafka_producer
            .send(
                FutureRecord::to(topic)
                    .payload(message)
                    .key(&format!("Key {i}")),
                    std::time::Duration::from_secs(0),
            )
            .await
            .unwrap();
    }

    let token = tokio_util::sync::CancellationToken::new();
    let cloned_token = token.clone();
    let _task_handler = tokio::spawn(async move {
        tokio::select! {
            _ = cloned_token.cancelled() => {
                let query_result = scylla_session.query("SELECT COUNT(*) FROM store.users;", ()).await.unwrap();
                let row  = query_result.first_row().unwrap();
                let typed = row.into_typed::<(i64,)>().unwrap();
                assert_eq!(typed.0, number_of_test_messages_to_produce + 1);
                use colored::Colorize;
                println!("{}", "test passed successfully".green());
                
                // TODO: clean up docker containers/images
                std::process::exit(0); 
            }
        }
    });
    let _handler = tokio::spawn({
        run(
            4,
            bootstrap_servers, 
            "test_group_id", 
            topic, 
            scylla_uri,
            atomic_counter,
            token)
    }).await;

    fn create_kafka_node<'a>(docker: &'a Cli, topic: &'a str, topic_out: &'a str) -> (Container<'a, rks::filkafka::Kafka>, String) {
        let kafka_node = docker.run(rks::filkafka::Kafka::default());
        let bootstrap_servers = format!(
            "127.0.0.1:{}",
            kafka_node.get_host_port_ipv4(rks::filkafka::KAFKA_PORT)
        );
        kafka_node.exec(ExecCommand { 
            cmd: format!("kafka-topics --create --topic {topic} --bootstrap-server localhost:9092 && kafka-topics --create --topic {topic_out} --bootstrap-server localhost:9092"),
            ready_conditions: vec![]});

        (kafka_node, bootstrap_servers)
    }

    fn create_scylla_node<'a>(docker: &'a Cli) -> (Container<'a, GenericImage>, String) {
        let msg = WaitFor::Duration { length: std::time::Duration::from_secs(30) }; // YUCK! // TODO: the below doesn't work
        //let msg = WaitFor::message_on_stdout("standard_role_manager - Created default superuser role 'cassandra'.");
        let scylla_image = GenericImage::new("scylladb/scylla", "latest")
            .with_exposed_port(9042)
            .with_wait_for(msg.clone());
        let scylla_runnable_image: RunnableImage<GenericImage> = scylla_image.into();
        let scylla_node = docker.run(scylla_runnable_image);
        let scyla_port = scylla_node.get_host_port_ipv4(9042);
        let scylla_uri = format!("localhost:{}", scyla_port);

        (scylla_node, scylla_uri)
    }
}