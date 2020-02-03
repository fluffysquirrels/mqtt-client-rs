use bytes::BytesMut;
use crate::{
    client::{
        builder::ClientBuilder,
        value_types::{
            KeepAlive,
            Publish,
            ReadResult,
            Subscribe,
            SubscribeResult,
            Unsubscribe,
        },
    },
    Error,
    Result,
    util::{
        FreePidList,
        TokioRuntime,
    }
};
use futures_util::{
    future::{
        FutureExt,
        pending,
    },
    select,
};
use log::{debug, error, trace};
use mqttrs::{
    ConnectReturnCode,
    Packet,
    Pid,
    QoS,
    QosPid,
    self
};
use rustls;
use std::{
    cell::RefCell,
    collections::BTreeMap,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use tokio::{
    io::{
        AsyncRead,
        AsyncReadExt,
        AsyncWrite,
        AsyncWriteExt,
    },
    net::TcpStream,
    sync::{
        mpsc,
        oneshot,
    },
    time::{
        delay_until,
        Duration,
        Elapsed,
        Instant,
        timeout,
    },
};
use tokio_rustls::{
    self,
    client::TlsStream,
    TlsConnector,
    webpki::DNSNameRef,
};

/// An MQTT client.
///
/// Start building an instance by calling Client::builder() to get a
/// ClientBuilder, using the fluent builder pattern on ClientBuilder,
/// then calling ClientBuilder::build(). For example:
///
/// ```
/// # use mqtt_async_client::client::Client;
/// let client =
///     Client::builder()
///        .set_host("example.com".to_owned())
///        .build();
/// ```
pub struct Client {
    options: ClientOptions,
    state: ConnectState,
    free_write_pids: RefCell<FreePidList>,
}

#[derive(Clone)]
pub(crate) struct ClientOptions {
    pub(crate) host: String,
    pub(crate) port: u16,
    pub(crate) username: Option<String>,
    pub(crate) password: Option<Vec<u8>>,
    pub(crate) keep_alive: KeepAlive,
    pub(crate) runtime: TokioRuntime,
    pub(crate) client_id: Option<String>,
    pub(crate) packet_buffer_len: usize,
    pub(crate) max_packet_len: usize,
    pub(crate) operation_timeout: Duration,
    pub(crate) tls_client_config: Option<Arc<rustls::ClientConfig>>,

}

pub(crate) enum ConnectState {
    Disconnected,
    Connected(ClientConnection)
}

/// The client side of the communication channels to an IO task.
pub(crate) struct ClientConnection {
    /// Sender to send IO requests to the IO task.
    tx_write_requests: mpsc::Sender<IoRequest>,

    /// Receiver to receive Publish packets from the IO task.
    rx_recv_published: mpsc::Receiver<Packet>,
}

/// The state held by the IO task, a long-running tokio future. The IO
/// task manages the underlying TCP connection, sends periodic
/// keep-alive ping packets, and sends response packets to tasks that
/// are waiting.
struct IoTask {
    /// Options configured for the client.
    options: ClientOptions,

    /// The stream connected to an MQTT broker.
    stream: AsyncStream,

    /// A buffer with data read from `stream`.
    read_buf: BytesMut,

    /// The number of bytes at the start of `read_buf` that have been
    /// read from `stream`.
    read_bufn: usize,

    /// Receiver to receive IO requests for the IO task.
    rx_write_requests: mpsc::Receiver<IoRequest>,

    /// Sender to send Publish packets from the IO task.
    tx_recv_published: mpsc::Sender<Packet>,

    /// The time the last packet was written to `stream`.
    /// Used to calculate when to send a Pingreq
    last_write_time: Instant,

    /// The time the last Pingreq packet was written to `stream`.
    last_pingreq_time: Instant,

    /// The time the last Pingresp packet was read from `stream`.
    last_pingresp_time: Instant,

    /// A map from response Pid to the IoRequest that initiated the
    /// request that will be responded to.
    pid_response_map: BTreeMap<Pid, IoRequest>,

    /// Represents the task waiting in Client.connect() for the Connack
    /// connection response.
    connack_response: Option<IoRequest>,
}

/// An IO request from `Client` to the IO task.
#[derive(Debug)]
pub(crate) struct IoRequest {
    /// A one-shot channel Sender to send the result of the IO request.
    tx_result: oneshot::Sender<IoResult>,

    /// Represents the data needed to carry out the IO request.
    io_type: IoType,
}

/// The data the IO task needs to carry out an IO request.
#[derive(Debug)]
enum IoType {
    /// A packet to write that expects no response.
    WriteOnly { packet: Packet },

    /// A packet to write that expects a response with a certain `Pid`.
    WriteAndResponse { packet: Packet, response_pid: Pid },

    /// A connect packet that expects a Connack response.
    WriteConnect { packet: Packet },

    /// A request to shut down the TCP connection gracefully.
    Shutdown,
}

/// The result of an IO request sent by the IO task, which may contain a packet.
#[derive(Debug)]
struct IoResult {
    result: Result<Option<Packet>>,
}

/// A wrapper for the data connection, which may or may not be encrypted.
enum AsyncStream {
    TcpStream(TcpStream),
    TlsStream(TlsStream<TcpStream>),
}

impl AsyncRead for AsyncStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context,
        buf: &mut [u8]
    ) -> Poll<std::io::Result<usize>> {
        match Pin::get_mut(self) {
            AsyncStream::TcpStream(tcp) => Pin::new(tcp).poll_read(cx, buf),
            AsyncStream::TlsStream(tls) => Pin::new(tls).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for AsyncStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context,
        buf: &[u8]
    ) -> Poll<std::result::Result<usize, tokio::io::Error>> {
        match Pin::get_mut(self) {
            AsyncStream::TcpStream(tcp) => Pin::new(tcp).poll_write(cx, buf),
            AsyncStream::TlsStream(tls) => Pin::new(tls).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context
    ) -> Poll<std::result::Result<(), tokio::io::Error>> {
        match Pin::get_mut(self) {
            AsyncStream::TcpStream(tcp) => Pin::new(tcp).poll_flush(cx),
            AsyncStream::TlsStream(tls) => Pin::new(tls).poll_flush(cx),
        }
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        cx: &mut Context
    ) -> Poll<std::result::Result<(), tokio::io::Error>> {
        match Pin::get_mut(self) {
            AsyncStream::TcpStream(tcp) => Pin::new(tcp).poll_shutdown(cx),
            AsyncStream::TlsStream(tls) => Pin::new(tls).poll_shutdown(cx),
        }
    }
}

impl Client {
    /// Start a fluent builder interface to construct a `Client`.
    pub fn builder() -> ClientBuilder {
        ClientBuilder::default()
    }

    pub(crate) fn new(opts: ClientOptions) -> Result<Client> {
        Ok(Client {
            options: opts,
            state: ConnectState::Disconnected,
            free_write_pids: RefCell::new(FreePidList::new()),
        })
    }

    async fn connect_stream(&mut self) -> Result<AsyncStream> {
        debug!("Connecting to {}:{}", self.options.host, self.options.port);
        match self.options.tls_client_config {
            Some(ref c) => {
                let connector = TlsConnector::from(c.clone());
                let domain = DNSNameRef::try_from_ascii_str(&*self.options.host)
                                        .map_err(|e| Error::from_std_err(e))?;
                let tcp = TcpStream::connect((&*self.options.host, self.options.port)).await?;
                let conn = connector.connect(domain, tcp).await?;
                Ok(AsyncStream::TlsStream(conn))
            },
            None => {
                let tcp = TcpStream::connect((&*self.options.host, self.options.port)).await?;
                Ok(AsyncStream::TcpStream(tcp))
            }
        }
    }

    /// Open a connection to the configured MQTT broker.
    pub async fn connect(&mut self) -> Result<()> {
        self.check_disconnected()?;
        let stream = self.connect_stream().await?;
        let (tx_write_requests, rx_write_requests) =
            mpsc::channel::<IoRequest>(self.options.packet_buffer_len);
        // TODO: Change this to allow control messages, e.g. disconnected?
        let (tx_recv_published, rx_recv_published) =
            mpsc::channel::<Packet>(self.options.packet_buffer_len);
        self.state = ConnectState::Connected(ClientConnection {
            tx_write_requests,
            rx_recv_published,
        });
        let io = IoTask {
            options: self.options.clone(),
            stream,
            read_buf: BytesMut::with_capacity(self.options.max_packet_len),
            read_bufn: 0,
            rx_write_requests,
            tx_recv_published,
            last_write_time: Instant::now(),
            last_pingreq_time: Instant::now(),
            last_pingresp_time: Instant::now(),
            pid_response_map: BTreeMap::new(),
            connack_response: None,
        };
        self.options.runtime.spawn(io.run());

        let conn = Packet::Connect(mqttrs::Connect {
            protocol: mqttrs::Protocol::MQTT311,
            keep_alive: match self.options.keep_alive {
                KeepAlive::Disabled => 0,
                KeepAlive::Enabled { secs } => secs,
            },
            client_id: match &self.options.client_id {
                None => "".to_owned(),
                Some(cid) => cid.to_owned(),
            },
            clean_session: true, // TODO
            last_will: None, // TODO
            username: self.options.username.clone(),
            password: self.options.password.clone(),
        });
        let connack = timeout(self.options.operation_timeout, self.write_connect(&conn)).await;
        if let Err(Elapsed { .. }) = connack {
            let _ = self.shutdown().await;
            return Err(format!("Timeout waiting for Connack after {}ms",
                               self.options.operation_timeout.as_millis()).into());
        }
        let connack = connack.expect("Not a timeout")?;
        match connack {
            Packet::Connack(ca) => {
                if ca.code != ConnectReturnCode::Accepted {
                    let _ = self.shutdown().await;
                    return Err(format!("Bad connect return code: {:?}", ca.code).into());
                }
            },
            _ => {
                self.shutdown().await?;
                return Err("Received packet not CONNACK after connect".into());
            }
        }
        Ok(())
    }

    /// Publish some data on a topic.
    ///
    /// Note that this method takes `&self`. This means a caller can
    /// create several publish futures to publish several payloads of
    /// data simultaneously without waiting for responses.
    pub async fn publish(&self, p: &Publish) -> Result<()> {
        let qos = p.qos();
        if qos == QoS::ExactlyOnce {
            return Err("QoS::ExactlyOnce is not supported".into());
        }
        let p2 = Packet::Publish(mqttrs::Publish {
            dup: false, // TODO.
            qospid: match qos {
                QoS::AtMostOnce => QosPid::AtMostOnce,
                QoS::AtLeastOnce => QosPid::AtLeastOnce(self.alloc_write_pid()?),
                QoS::ExactlyOnce => panic!("Not reached"),
            },
            retain: false, // TODO
            topic_name: p.topic().to_owned(),
            payload: p.payload().to_owned(),
        });
        match qos {
            QoS::AtMostOnce => self.write_only_packet(&p2).await?,
            QoS::AtLeastOnce => {
                let res = timeout(self.options.operation_timeout, self.write_response_packet(&p2)).await;
                if let Err(Elapsed { .. }) = res {
                    // We report this but can't really deal with it properly.
                    // The protocol says we can't re-use the packet ID so we have to leak it
                    // and potentially run out of packet IDs.
                    return Err(format!("Timeout waiting for Puback after {}ms",
                                       self.options.operation_timeout.as_millis()).into());
                }
                let res = res.expect("No timeout")?;
                match res {
                    Packet::Puback(pid) => self.free_write_pid(pid)?,
                    _ => error!("Bad packet response for publish: {:#?}", res),
                }
            },
            QoS::ExactlyOnce => panic!("Not reached"),
        };
        Ok(())
    }

    /// Subscribe to some topics.`read_subscriptions` will return
    /// data for them.
    pub async fn subscribe(&mut self, s: Subscribe) -> Result<SubscribeResult> {
        let pid = self.alloc_write_pid()?;
        // TODO: Support subscribe to qos == ExactlyOnce.
        if s.topics().iter().any(|t| t.qos == QoS::ExactlyOnce) {
            return Err("Qos::ExactlyOnce is not supported right now".into())
        }
        let p = Packet::Subscribe(mqttrs::Subscribe {
            pid: pid,
            topics: s.topics().to_owned(),
        });
        let res = timeout(self.options.operation_timeout, self.write_response_packet(&p)).await;
        if let Err(Elapsed { .. }) = res {
            // We report this but can't really deal with it properly.
            // The protocol says we can't re-use the packet ID so we have to leak it
            // and potentially run out of packet IDs.
            return Err(format!("Timeout waiting for Suback after {}ms",
                               self.options.operation_timeout.as_millis()).into());
        }
        let res = res.expect("No timeout")?;
        match res {
            Packet::Suback(mqttrs::Suback {
                pid: suback_pid,
                return_codes: rcs,
            }) if suback_pid == pid => {
                self.free_write_pid(pid)?;
                Ok(SubscribeResult {
                    return_codes: rcs
                })
            },
            _ => {
                return Err(format!("Unexpected packet waiting for Suback(Pid={:?}): {:#?}",
                                   pid, res)
                           .into());
            }
        }
    }

    /// Unsubscribe from some topics. `read_subscriptions` will no
    /// longer return data for them.
    pub async fn unsubscribe(&mut self, u: Unsubscribe) -> Result<()> {
        let pid = self.alloc_write_pid()?;
        let p = Packet::Unsubscribe(mqttrs::Unsubscribe {
            pid: pid,
            topics: u.topics().iter().map(|ut| ut.topic_name().to_owned())
                     .collect::<Vec<String>>(),
        });
        let res = timeout(self.options.operation_timeout, self.write_response_packet(&p)).await;
        if let Err(Elapsed { .. }) = res {
            // We report this but can't really deal with it properly.
            // The protocol says we can't re-use the packet ID so we have to leak it
            // and potentially run out of packet IDs.
            return Err(format!("Timeout waiting for Unsuback after {}ms",
                               self.options.operation_timeout.as_millis()).into());
        }
        let res = res.expect("No timeout")?;
        match res {
            Packet::Unsuback(ack_pid)
            if ack_pid == pid => {
                self.free_write_pid(pid)?;
                Ok(())
            },
            _ => {
                return Err(format!("Unexpected packet waiting for Unsuback(Pid={:?}): {:#?}",
                                   pid, res)
                           .into());
            }
        }
    }

    /// Wait for the next Publish packet for one of this Client's subscriptions.
    pub async fn read_subscriptions(&mut self) -> Result<ReadResult> {
        let c = self.check_connected_mut()?;
        let r = match c.rx_recv_published.recv().await {
            Some(r) => r,
            None => {
                // Sender closed.
                self.state = ConnectState::Disconnected;
                return Err(Error::Disconnected);
            }
        };
        match r {
            Packet::Publish(p) => {
                match p.qospid {
                    QosPid::AtMostOnce => (),
                    QosPid::AtLeastOnce(pid) => {
                        self.write_only_packet(&Packet::Puback(pid)).await?;
                    },
                    QosPid::ExactlyOnce(_) => {
                        error!("Received publish with unimplemented QoS: ExactlyOnce");
                    }
                }
                let rr = ReadResult {
                    topic: p.topic_name,
                    payload: p.payload,
                };
                Ok(rr)
            },
            _ => {
                return Err(format!("Unexpected packet waiting for read: {:#?}", r).into());
            }
        }
    }

    /// Gracefully close the connection to the server.
    pub async fn disconnect(&mut self) -> Result<()> {
        self.check_connected()?;
        debug!("Disconnecting");
        let p = Packet::Disconnect;
        self.write_only_packet(&p).await?;
        self.shutdown().await?;
        Ok(())
    }

    fn alloc_write_pid(&self) -> Result<Pid> {
        match self.free_write_pids.borrow_mut().alloc() {
            Some(pid) => Ok(Pid::try_from(pid).expect("Non-zero Pid")),
            None => Err(Error::from("No free Pids")),
        }
    }

    fn free_write_pid(&self, p: Pid) -> Result<()> {
        match self.free_write_pids.borrow_mut().free(p.get()) {
            true => Err(Error::from("Pid was already free")),
            false => Ok(())
        }
    }

    async fn shutdown(&mut self) -> Result <()> {
        let _c = self.check_connected()?;
        self.write_request(IoType::Shutdown).await?;
        self.state = ConnectState::Disconnected;
        Ok(())
    }

    async fn write_only_packet(&self, p: &Packet) -> Result<()> {
        self.write_request(IoType::WriteOnly { packet: p.clone(), })
            .await.map(|_v| ())
    }

    async fn write_response_packet(&self, p: &Packet) -> Result<Packet> {
        let io_type = IoType::WriteAndResponse {
            packet: p.clone(),
            response_pid: packet_pid(p).expect("packet_pid"),
        };
        self.write_request(io_type)
            .await.map(|v| v.expect("return packet"))
    }

    async fn write_connect(&self, p: &Packet) -> Result<Packet> {
        self.write_request(IoType::WriteConnect { packet: p.clone() })
            .await.map(|v| v.expect("return packet"))
    }

    async fn write_request(&self, io_type: IoType) -> Result<Option<Packet>> {
        let c = self.check_connected()?;
        let (tx, rx) = oneshot::channel::<IoResult>();
        let req = IoRequest {
            tx_result: tx,
            io_type: io_type,
        };
        c.tx_write_requests.clone().send(req).await
            .map_err(|e| Error::from_std_err(e))?;
        // TODO: Add a timeout?
        let res = rx.await
            .map_err(|e| Error::from_std_err(e))?;
        res.result
    }

    fn check_connected_mut(&mut self) -> Result<&mut ClientConnection> {
        match self.state {
            ConnectState::Disconnected => Err(Error::Disconnected),
            ConnectState::Connected(ref mut c) => Ok(c),
        }
    }

    fn check_connected(&self) -> Result<&ClientConnection> {
        match self.state {
            ConnectState::Disconnected => Err(Error::Disconnected),
            ConnectState::Connected(ref c) => Ok(c),
        }
    }

    fn check_disconnected(&self) -> Result<()> {
        match self.state {
            ConnectState::Disconnected => Ok(()),
            ConnectState::Connected(_) => Err("Connected already".into()),
        }
    }
}

fn packet_pid(p: &Packet) -> Option<Pid> {
    match p {
        Packet::Connect(_) => None,
        Packet::Connack(_) => None,
        Packet::Publish(publish) => publish.qospid.pid(),
        Packet::Puback(pid) => Some(pid.to_owned()),
        Packet::Pubrec(pid) => Some(pid.to_owned()),
        Packet::Pubrel(pid) => Some(pid.to_owned()),
        Packet::Pubcomp(pid) => Some(pid.to_owned()),
        Packet::Subscribe(sub) => Some(sub.pid),
        Packet::Suback(suback) => Some(suback.pid),
        Packet::Unsubscribe(unsub) => Some(unsub.pid),
        Packet::Unsuback(pid) => Some(pid.to_owned()),
        Packet::Pingreq => None,
        Packet::Pingresp => None,
        Packet::Disconnect => None,
    }
}

/// Represents what happened "next" that we should handle.
enum SelectResult {
    /// An IO request from the Client
    IoReq(Option<IoRequest>),

    /// Read a packet from the network
    Read(Result<Packet>),

    /// Time to send a keep-alive ping request packet.
    Ping,

    /// Timeout waiting for a Pingresp.
    PingrespExpected,
}

impl IoTask {
    async fn run(mut self) {
        loop {
            if let Err(e) = Self::run_once(&mut self).await {
                error!("IoTask: Quitting run loop due to error: {}", e);
                return;
            }
        }
    }

    /// Unhandled errors are returned and terminate the run loop.
    async fn run_once(&mut self) -> Result<()> {
        let keepalive_next = match &self.options.keep_alive {
            KeepAlive::Disabled => None,
            KeepAlive::Enabled{ secs } => {
                let dur = Duration::from_secs(*secs as u64);
                Some(self.last_write_time + dur)
            },
        };

        let pingresp_expected_by = if self.last_pingreq_time > self.last_pingresp_time {
            Some(self.last_pingreq_time + self.options.operation_timeout)
        } else {
            None
        };

        // Select over futures to determine what to do next:
        // * Handle a write request from the Client
        // * Handle an incoming packet from the network
        // * Handle a keep-alive period elapsing and send a ping request
        // * Handle a PingrespExpected timeout and disconnect
        //
        // From these futures we compute an enum value in sel_res
        // that encapsulates what to do next, then match over
        // sel_res to actually do the work. The reason for this
        // structure is just to keep the borrow checker happy.
        // The futures calculation uses a mutable borrow on `stream`
        // for the `read_packet` call, but the mutable borrow ends there.
        // Then when we want to do the work we can take a new, separate mutable
        // borrow to write packets based on IO requests.
        // These two mutable borrows don't overlap.
        let sel_res: SelectResult = {
            let mut req_fut = Box::pin(self.rx_write_requests.recv().fuse());
            let mut read_fut = Box::pin(
                Self::read_packet(&mut self.stream, &mut self.read_buf, &mut self.read_bufn,
                                  self.options.max_packet_len).fuse());
            let mut ping_fut = match keepalive_next {
                Some(t) => Box::pin(delay_until(t).boxed().fuse()),
                None => Box::pin(pending().boxed().fuse()),
            };
            let mut pingresp_expected_fut = match pingresp_expected_by {
                Some(t) => Box::pin(delay_until(t).boxed().fuse()),
                None => Box::pin(pending().boxed().fuse()),
            };
            select! {
                req = req_fut => SelectResult::IoReq(req),
                read = read_fut => SelectResult::Read(read),
                _ = ping_fut => SelectResult::Ping,
                _ = pingresp_expected_fut => SelectResult::PingrespExpected,
            }
        };
        match sel_res {
            SelectResult::Read(read) => return self.handle_read(read).await,
            SelectResult::IoReq(req) => match req{
                None => {
                    // Sender closed.
                    debug!("IoTask: Req stream closed, shutting down.");
                    if let Err(e) = self.stream.shutdown().await {
                        error!("IoTask: Error shutting down stream: {:?}", e);
                    }
                    return Err(Error::Disconnected);
                },
                Some(req) => return self.handle_io_req(req).await,
            },
            SelectResult::Ping => return self.send_ping().await,
            SelectResult::PingrespExpected => {
                // We timed out waiting for a ping response from
                // the server, shutdown the stream.
                debug!("IoTask: Timed out waiting for Pingresp, shutting down.");
                if let Err(e) = self.stream.shutdown().await {
                    error!("IoTask: Error shutting down stream: {:?}", e);
                }
                return Err(Error::Disconnected);
            }
        }
    }

    async fn handle_read(&mut self, read: Result<Packet>) -> Result<()> {
        match read {
            Err(Error::Disconnected) => {
                return Err(Error::Disconnected);
            }
            Err(e) => {
                error!("IoTask: Failed to read packet: {:?}", e);
            },
            Ok(p) => {
                match p {
                    Packet::Pingresp => {
                        debug!("IoTask: Received Pingresp");
                        self.last_pingresp_time = Instant::now();
                    },
                    Packet::Publish(_) => {
                        if let Err(e) = self.tx_recv_published.send(p).await {
                            error!("IoTask: Failed to send Packet: {:?}", e);
                        }
                    },
                    Packet::Connack(_) => {
                        if let Some(ca_req) = self.connack_response.take() {
                            trace!("Sending connack response p={:?}",
                                   p);
                            let res = IoResult { result: Ok(Some(p)) };
                            if let Err(e) = ca_req.tx_result.send(res) {
                                error!("IoTask: Failed to send IoResult: {:?}", e);
                            }
                        }
                        self.connack_response = None;
                    }
                    _ => {
                        let pid = packet_pid(&p);
                        if let Some(pid) = pid {
                            let pid_response = self.pid_response_map.remove(&pid);
                            match pid_response {
                                None => error!("Unknown PID: {:?}", pid),
                                Some(req) => {
                                    trace!("Sending response PID={:?} p={:?}",
                                           pid, p);
                                    let res = IoResult { result: Ok(Some(p)) };
                                    if let Err(e) = req.tx_result.send(res) {
                                        error!("IoTask: Failed to send IoResult: {:?}",
                                               e);
                                    }
                                },
                            }
                        }
                    },
                }
            },
        }
        Ok(())
    }

    async fn handle_io_req(&mut self, req: IoRequest) -> Result<()> {
        self.last_write_time = Instant::now();
        let packet = req.io_type.packet();
        if let Some(p) = packet {
            let res = self.write_packet(&p).await;
            if let Err(ref e) = res {
                error!("IoTask: Error writing packet: {:?}", e);
                let res = IoResult { result: res.map(|_| None) };
                if let Err(e) = req.tx_result.send(res) {
                    error!("IoTask: Failed to send IoResult: {:?}", e);
                }
                return Ok(())
            }
            match req.io_type {
                IoType::WriteOnly { .. } => {
                    let res = IoResult { result: res.map(|_| None) };
                    if let Err(e) = req.tx_result.send(res) {
                        error!("IoTask: Failed to send IoResult: {:?}", e);
                    }
                },
                IoType::WriteAndResponse { response_pid, .. } => {
                    self.pid_response_map.insert(response_pid, req);
                },
                IoType::WriteConnect { .. } => {
                    self.connack_response = Some(req);
                },
                IoType::Shutdown => {
                    panic!("Not reached because shutdown has no packet")
                },
            }
        } else {
            match req.io_type {
                IoType::Shutdown => {
                    debug!("IoTask: IoType::Shutdown.");
                    if let Err(e) = self.stream.shutdown().await {
                        error!("IoTask: Error shutting down stream: {:?}", e);
                    }
                    let res = IoResult { result: Ok(None) };
                    if let Err(e) = req.tx_result.send(res) {
                        error!("IoTask: Failed to send IoResult: {:?}", e);
                    }
                    return Err(Error::Disconnected);
                }
                _ => (),
            }
        }
        Ok(())
    }

    async fn send_ping(&mut self) -> Result<()> {
        debug!("IoTask: Writing Pingreq");
        self.last_write_time = Instant::now();
        self.last_pingreq_time = Instant::now();
        let p = Packet::Pingreq;
        if let Err(e) = self.write_packet(&p).await {
            error!("IoTask: Failed to write ping: {:?}", e);
        }
        Ok(())
    }

    async fn write_packet(
        &mut self,
        p: &Packet,
    ) -> Result<()> {
        if cfg!(feature = "unsafe-logging") {
            trace!("write_packet p={:#?}", p);
        }
        // TODO: Test long packets.
        let mut bytes = BytesMut::with_capacity(self.options.max_packet_len);
        mqttrs::encode(&p, &mut bytes)?;
        if cfg!(feature = "unsafe-logging") {
            trace!("write_packet bytes p={:?}", &*bytes);
        }
        self.stream.write_all(&*bytes).await?;
        Ok(())
    }

    async fn read_packet(
        stream: &mut AsyncStream,
        read_buf: &mut BytesMut,
        read_bufn: &mut usize,
        max_packet_len: usize
    ) -> Result<Packet> {
        // TODO: Test long packets.
        loop {
            if cfg!(feature = "unsafe-logging") {
                trace!("read_packet Decoding buf={:?}", &read_buf[0..*read_bufn]);
            }
            if *read_bufn > 0 {
                // We already have some bytes in the buffer. Try to decode a packet
                read_buf.split_off(*read_bufn);
                let old_len = read_buf.len();
                let decoded = mqttrs::decode(read_buf)?;
                if cfg!(feature = "unsafe-logging") {
                    trace!("read_packet decoded={:#?}", decoded);
                }
                if let Some(p) = decoded {
                    let new_len = read_buf.len();
                    trace!("read_packet old_len={} new_len={} read_bufn={}",
                           old_len, new_len, *read_bufn);
                    *read_bufn -= old_len - new_len;
                    if cfg!(feature = "unsafe-logging") {
                        trace!("read_packet Remaining buf={:?}", &read_buf[0..*read_bufn]);
                    }
                    return Ok(p);
                }
            }
            read_buf.resize(max_packet_len, 0u8);
            let readlen = read_buf.len();
            trace!("read_packet read read_bufn={} readlen={}", *read_bufn, readlen);
            let nread = stream.read(&mut read_buf[*read_bufn..readlen]).await?;
            *read_bufn += nread;
            if nread == 0 {
                // Socket disconnected
                error!("IoTask: Socket disconnected");
                return Err(Error::Disconnected);
            }
        }
    }
}

impl IoType {
    fn packet(&self) -> Option<&Packet> {
        match self {
            IoType::Shutdown => None,
            IoType::WriteOnly { packet } => Some(&packet),
            IoType::WriteAndResponse { packet, .. } => Some(&packet),
            IoType::WriteConnect { packet, .. } => Some(&packet),
        }
    }
}
