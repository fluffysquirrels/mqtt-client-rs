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
//! instructions: https://stackoverflow.com/a/21340898/94819

#![deny(warnings)]

use mqtt_client::{
    client::{
        Client,
        Publish,
        QoS,
        Subscribe,
        SubscribeTopic,
        Unsubscribe,
        UnsubscribeTopic,
    },
    Error,
    Result,
};
use rustls;
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
    let mut rt = tokio::runtime::Runtime::new()?;
    rt.block_on(async {
        let mut c = plain_client()?;
        c.connect().await?;

        // Subscribe to "a"
        let subopts = Subscribe::new(vec![
            SubscribeTopic { qos: QoS::AtMostOnce, topic_path: "test/pub_and_sub".to_owned() }
            ]);
        let subres = c.subscribe(subopts).await?;
        subres.any_failures()?;

        // Publish to "a"
        let mut p = Publish::new("test/pub_and_sub".to_owned(), "x".as_bytes().to_vec());
        p.set_qos(QoS::AtMostOnce);
        c.publish(&p).await?;

        // Read from "a".
        let r = c.read_subscriptions().await?;
        assert_eq!(r.topic(), "test/pub_and_sub");
        assert_eq!(r.payload(), b"x");
        Ok(())
    })
}

#[test]
fn pub_and_sub_tls() -> Result<()> {
    let mut rt = tokio::runtime::Runtime::new()?;
    rt.block_on(async {
        let mut c = tls_client()?;
        c.connect().await?;

        // Subscribe to "a"
        let subopts = Subscribe::new(vec![
            SubscribeTopic { qos: QoS::AtMostOnce, topic_path: "test/pub_and_sub_tls".to_owned() }
            ]);
        let subres = c.subscribe(subopts).await?;
        subres.any_failures()?;

        // Publish to "a"
        let mut p = Publish::new("test/pub_and_sub_tls".to_owned(), "x".as_bytes().to_vec());
        p.set_qos(QoS::AtMostOnce);
        c.publish(&p).await?;

        // Read from "a".
        let r = c.read_subscriptions().await?;
        assert_eq!(r.topic(), "test/pub_and_sub_tls");
        assert_eq!(r.payload(), b"x");
        Ok(())
    })
}

#[test]
fn unsubscribe() -> Result<()> {
    let mut rt = tokio::runtime::Runtime::new()?;
    rt.block_on(async {
        let mut c = tls_client()?;
        c.connect().await?;

        // Subscribe to "test/unsub"
        let subopts = Subscribe::new(vec![
            SubscribeTopic { qos: QoS::AtMostOnce, topic_path: "test/unsub".to_owned() }
            ]);
        let subres = c.subscribe(subopts).await?;
        subres.any_failures()?;

        // Unsubscribe from "test/unsub"
        c.unsubscribe(Unsubscribe::new(vec![
            UnsubscribeTopic::new("test/unsub".to_owned()),
            ])).await?;

        // Publish to "a"
        let mut p = Publish::new("test/unsub".to_owned(), "x".as_bytes().to_vec());
        p.set_qos(QoS::AtMostOnce);
        c.publish(&p).await?;

        // Read from "a" and timeout.
        let r = timeout(Duration::from_secs(3), c.read_subscriptions()).await;
        assert!(r.is_err());
        Ok(())
    })
}

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
        .build()
}

#[allow(dead_code)]
fn plain_client() -> Result<Client> {
    Client::builder()
        .set_host("localhost".to_owned())
        .set_port(1883)
        .build()
}
