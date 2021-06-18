//! Some integration tests that require an MQTT 3.1.1 broker listening.
//!
//! Download [mosquitto](https://mosquitto.org/download/) version
//! 1.6.8 or higher and run it with the supplied mosquitto.conf from
//! the ${REPO}/tests directory:
//!
//!```shell
//! ${MOSQUITTO_PATH}/mosquitto -c mosquitto.conf
//! ```
//!
//! This will run an unencrypted listener at localhost:1883, and a TLS
//! encrypted listener at localhost:8883, using the certificates and
//! keys in ${REPO}/tests/certs, which were generated using these
//! instructions: <https://stackoverflow.com/a/21340898/94819>

#![deny(warnings)]

use mqtt_async_client::{
    client::{
        Client,
        Publish,
        QoS,
        Subscribe,
        SubscribeTopic,
        Unsubscribe,
        UnsubscribeTopic,
    },
    Result,
};
#[cfg(feature = "tls")]
use mqtt_async_client::Error;
#[cfg(feature = "tls")]
use rustls;
use std::sync::Once;
#[cfg(feature = "tls")]
use std::io::Cursor;
use tokio::{
    self,
    time::{
        Duration,
        timeout,
    },
};

#[test]
fn pub_and_sub_plain() -> Result<()> {
    init_logger();
    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(async {
        let mut c = plain_client()?;
        c.connect().await?;

        // Subscribe
        let subopts = Subscribe::new(vec![
            SubscribeTopic { qos: QoS::AtMostOnce, topic_path: "test/pub_and_sub".to_owned() }
            ]);
        let subres = c.subscribe(subopts).await?;
        subres.any_failures()?;

        // Publish
        let mut p = Publish::new("test/pub_and_sub".to_owned(), "x".as_bytes().to_vec());
        p.set_qos(QoS::AtMostOnce);
        c.publish(&p).await?;

        // Read
        let r = c.read_subscriptions().await?;
        assert_eq!(r.topic(), "test/pub_and_sub");
        assert_eq!(r.payload(), b"x");
        c.disconnect().await?;
        Ok(())
    })
}

#[test]
#[cfg(feature = "websocket")]
fn pub_and_sub_websocket() -> Result<()> {
    init_logger();
    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(async {
        let mut c = websocket_client()?;
        c.connect().await?;

        // Subscribe
        let subopts = Subscribe::new(vec![
            SubscribeTopic { qos: QoS::AtMostOnce, topic_path: "test/pub_and_sub_websocket".to_owned() }
            ]);
        let subres = c.subscribe(subopts).await?;
        subres.any_failures()?;

        // Publish
        let mut p = Publish::new("test/pub_and_sub_websocket".to_owned(), "x".as_bytes().to_vec());
        p.set_qos(QoS::AtMostOnce);
        c.publish(&p).await?;

        // Read
        let r = c.read_subscriptions().await?;
        assert_eq!(r.topic(), "test/pub_and_sub_websocket");
        assert_eq!(r.payload(), b"x");
        c.disconnect().await?;
        Ok(())
    })
}

#[cfg(feature = "tls")]
#[test]
fn pub_and_sub_tls() -> Result<()> {
    init_logger();
    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(async {
        let mut c = tls_client()?;
        c.connect().await?;

        // Subscribe
        let subopts = Subscribe::new(vec![
            SubscribeTopic { qos: QoS::AtMostOnce, topic_path: "test/pub_and_sub_tls".to_owned() }
            ]);
        let subres = c.subscribe(subopts).await?;
        subres.any_failures()?;

        // Publish
        let mut p = Publish::new("test/pub_and_sub_tls".to_owned(), "x".as_bytes().to_vec());
        p.set_qos(QoS::AtMostOnce);
        c.publish(&p).await?;

        // Read
        let r = c.read_subscriptions().await?;
        assert_eq!(r.topic(), "test/pub_and_sub_tls");
        assert_eq!(r.payload(), b"x");
        c.disconnect().await?;
        Ok(())
    })
}

#[test]
fn unsubscribe() -> Result<()> {
    init_logger();
    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(async {
        let mut c = plain_client()?;
        c.connect().await?;

        // Subscribe
        let subopts = Subscribe::new(vec![
            SubscribeTopic { qos: QoS::AtMostOnce, topic_path: "test/unsub".to_owned() }
            ]);
        let subres = c.subscribe(subopts).await?;
        subres.any_failures()?;

        // Unsubscribe
        c.unsubscribe(Unsubscribe::new(vec![
            UnsubscribeTopic::new("test/unsub".to_owned()),
            ])).await?;

        // Publish
        let mut p = Publish::new("test/unsub".to_owned(), "x".as_bytes().to_vec());
        p.set_qos(QoS::AtMostOnce);
        c.publish(&p).await?;

        // Read and timeout.
        let r = timeout(Duration::from_secs(3), c.read_subscriptions()).await;
        assert!(r.is_err());
        c.disconnect().await?;
        Ok(())
    })
}

#[test]
fn retain() -> Result<()> {
    init_logger();
    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(async {
        let mut c = plain_client()?;
        c.connect().await?;

        // Publish
        let mut p = Publish::new("test/retain".to_owned(), "x".as_bytes().to_vec());
        p.set_qos(QoS::AtMostOnce);
        p.set_retain(true);
        c.publish(&p).await?;

        // Subscribe
        let subopts = Subscribe::new(vec![
            SubscribeTopic { qos: QoS::AtMostOnce, topic_path: "test/retain".to_owned() }
            ]);
        let subres = c.subscribe(subopts).await?;
        subres.any_failures()?;

        // Read
        let r = c.read_subscriptions().await?;
        assert_eq!(r.topic(), "test/retain");
        assert_eq!(r.payload(), b"x");
        c.disconnect().await?;
        Ok(())
    })
}

#[cfg(feature = "tls")]
fn tls_client() -> Result<Client> {
    let mut cc = rustls::ClientConfig::new();
    let cert_bytes = include_bytes!("certs/cacert.pem");
    let cert = rustls::internal::pemfile::certs(&mut Cursor::new(&cert_bytes[..]))
        .map_err(|_| Error::from("Error parsing cert file"))?[0].clone();
    cc.root_store.add(&cert)
        .map_err(|e| Error::from_std_err(e))?;
    Client::builder()
        .set_host("localhost".to_owned())
        .set_port(8883)
        .set_tls_client_config(cc)
        .set_connect_retry_delay(Duration::from_secs(1))
        .build()
}

#[cfg(feature = "websocket")]
fn websocket_client() -> Result<Client> {
    Client::builder()
        .set_host("ws://127.0.0.1:9001".to_owned())
        .set_websocket()
        .set_connect_retry_delay(Duration::from_secs(1))
        .build()
}

fn plain_client() -> Result<Client> {
    Client::builder()
        .set_host("localhost".to_owned())
        .set_port(1883)
        .set_connect_retry_delay(Duration::from_secs(1))
        .build()
}

static LOGGER_INIT: Once = Once::new();

fn init_logger() {
    LOGGER_INIT.call_once(|| env_logger::init());
}
