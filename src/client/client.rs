#[cfg(feature = "websocket")]
use http::request::Request;
use bytes::BytesMut;
#[cfg(feature = "websocket")]
use tokio_tungstenite::tungstenite::http::Uri;
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
        AsyncStream,
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
use log::{debug, error, info, trace};
use mqttrs::{
    ConnectReturnCode,
    Packet,
    Pid,
    QoS,
    QosPid,
    self,
    SubscribeTopic,
};
#[cfg(feature = "tls")]
use rustls;
use std::{
    cmp::min,
    collections::BTreeMap,
    fmt,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
        Mutex,
    },
};
use tokio::{
    io::{
        AsyncReadExt,
        AsyncWriteExt,
    },
    net::TcpStream,
    sync::{
        mpsc,
        oneshot,
    },
    time::{
        sleep,
        sleep_until,
        Duration,
        error::Elapsed,
        Instant,
        timeout,
    },
};
#[cfg(feature = "tls")]
use tokio_rustls::{
    self,
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
///
/// `Client` is expected to be `Send` (passable between threads), but not
/// `Sync` (usable by multiple threads at the same time).
pub struct Client {
    /// Options configured for the client
    options: ClientOptions,

    /// Handle values to communicate with the IO task
    io_task_handle: Option<IoTaskHandle>,

    /// Tracks which Pids (MQTT packet IDs) are in use.
    ///
    /// This field uses a Mutex for interior mutability so that
    /// `Client` is `Send`. It's not expected to be `Sync`.
    free_write_pids: Mutex<FreePidList>,
}

impl fmt::Debug for Client {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Client")
         .field("options", &self.options)
         .finish()
    }
}

#[derive(Clone)]
pub(crate) struct ClientOptions {
    // See ClientBuilder methods for per-field documentation.

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
    pub(crate) connection_mode: ConnectionMode,
    pub(crate) automatic_connect: bool,
    pub(crate) connect_retry_delay: Duration,
}

impl fmt::Debug for ClientOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ClientOptions")
         .field("host", &self.host)
         .field("port", &self.port)
         .field("username", &self.username)
         // Deliberately skipping password field here to
         // avoid accidentially leaking it
         .field("keep_alive", &self.keep_alive)
         .field("client_id", &self.client_id)
         .field("packet_buffer_len", &self.packet_buffer_len)
         .field("max_packet_len", &self.max_packet_len)
         .field("operation_timeout", &self.operation_timeout)
         .field("automatic_connect", &self.automatic_connect)
         .field("connect_retry_delay", &self.connect_retry_delay)
         .finish()
    }
}

/// The client side of the communication channels to an IO task.
struct IoTaskHandle {
    /// Sender to send IO requests to the IO task.
    tx_io_requests: mpsc::Sender<IoRequest>,

    /// Receiver to receive Publish packets from the IO task.
    rx_recv_published: mpsc::Receiver<Result<Packet>>,

    /// Signal to the IO task to shutdown. Shared with IoTask.
    halt: Arc<AtomicBool>,
}

/// The state held by the IO task, a long-running tokio future. The IO
/// task manages the underlying TCP connection, sends periodic
/// keep-alive ping packets, and sends response packets to tasks that
/// are waiting.
struct IoTask {
    /// Options configured for the client.
    options: ClientOptions,

    /// Receiver to receive IO requests for the IO task.
    rx_io_requests: mpsc::Receiver<IoRequest>,

    /// Sender to send Publish packets from the IO task.
    tx_recv_published: mpsc::Sender<Result<Packet>>,

    /// enum value describing the current state as disconnected or connected.
    state: IoTaskState,

    /// Keeps track of active subscriptions in case they need to be
    /// replayed after reconnecting.
    subscriptions: BTreeMap<String, QoS>,

    /// Signal to the IO task to shutdown. Shared with IoTaskHandle.
    halt: Arc<AtomicBool>,
}

enum IoTaskState {
    Halted,
    Disconnected,
    Connected(IoTaskConnected),
}

/// The state associated with a network connection to an MQTT broker
struct IoTaskConnected {
    /// The stream connected to an MQTT broker.
    stream: AsyncStream,

    /// A buffer with data read from `stream`.
    read_buf: BytesMut,

    /// The number of bytes at the start of `read_buf` that have been
    /// read from `stream`.
    read_bufn: usize,

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
}

/// An IO request from `Client` to the IO task.
#[derive(Debug)]
struct IoRequest {
    /// A one-shot channel Sender to send the result of the IO request.
    tx_result: Option<oneshot::Sender<IoResult>>,

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

    /// A request to shut down the TCP connection gracefully.
    ShutdownConnection,
}

/// The result of an IO request sent by the IO task, which may contain a packet.
#[derive(Debug)]
struct IoResult {
    result: Result<Option<Packet>>,
}

impl Client {
    /// Start a fluent builder interface to construct a `Client`.
    pub fn builder() -> ClientBuilder {
        ClientBuilder::default()
    }

    pub(crate) fn new(opts: ClientOptions) -> Result<Client> {
        Ok(Client {
            options: opts,
            io_task_handle: None,
            free_write_pids: Mutex::new(FreePidList::new()),
        })
    }

    /// Open a connection to the configured MQTT broker.
    pub async fn connect(&mut self) -> Result<()> {
        self.spawn_io_task()?;
        Ok(())
    }

    fn spawn_io_task(&mut self) -> Result<()> {
        self.check_no_io_task()?;
        let (tx_io_requests, rx_io_requests) =
            mpsc::channel::<IoRequest>(self.options.packet_buffer_len);
        // TODO: Change this to allow control messages, e.g. disconnected?
        let (tx_recv_published, rx_recv_published) =
            mpsc::channel::<Result<Packet>>(self.options.packet_buffer_len);
        let halt = Arc::new(AtomicBool::new(false));
        self.io_task_handle = Some(IoTaskHandle {
            tx_io_requests,
            rx_recv_published,
            halt: halt.clone(),
        });
        let io = IoTask {
            options: self.options.clone(),
            rx_io_requests,
            tx_recv_published,
            state: IoTaskState::Disconnected,
            subscriptions: BTreeMap::new(),
            halt,
        };
        self.options.runtime.spawn(io.run());
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
            retain: p.retain(),
            topic_name: p.topic().to_owned(),
            payload: p.payload().to_owned(),
        });
        match qos {
            QoS::AtMostOnce => {
                let res = timeout(self.options.operation_timeout,
                                  self.write_only_packet(&p2)).await;
                if let Err(Elapsed { .. }) = res {
                    return Err(format!("Timeout writing publish after {}ms",
                                       self.options.operation_timeout.as_millis()).into());
                }
                res.expect("No timeout")?;
            }
            QoS::AtLeastOnce => {
                let res = timeout(self.options.operation_timeout,
                                  self.write_response_packet(&p2)).await;
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
            pid,
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
            pid,
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
        let h = self.check_io_task_mut()?;
        let r = match h.rx_recv_published.recv().await {
            Some(r) => r?,
            None => {
                // Sender closed.
                self.io_task_handle = None;
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
        self.check_io_task()?;
        debug!("Disconnecting");
        let p = Packet::Disconnect;
        let res = timeout(self.options.operation_timeout,
                          self.write_only_packet(&p)).await;
        if let Err(Elapsed { .. }) = res {
            return Err(format!("Timeout waiting for Disconnect to send after {}ms",
                               self.options.operation_timeout.as_millis()).into());
        }
        res.expect("No timeout")?;
        self.shutdown().await?;
        Ok(())
    }

    fn alloc_write_pid(&self) -> Result<Pid> {
        match self.free_write_pids.lock().expect("not poisoned").alloc() {
            Some(pid) => Ok(Pid::try_from(pid).expect("Non-zero Pid")),
            None => Err(Error::from("No free Pids")),
        }
    }

    fn free_write_pid(&self, p: Pid) -> Result<()> {
        match self.free_write_pids.lock().expect("not poisoned").free(p.get()) {
            true => Err(Error::from("Pid was already free")),
            false => Ok(())
        }
    }

    async fn shutdown(&mut self) -> Result <()> {
        let c = self.check_io_task()?;
        c.halt.store(true, Ordering::SeqCst);
        self.write_request(IoType::ShutdownConnection, None).await?;
        self.io_task_handle = None;
        Ok(())
    }

    async fn write_only_packet(&self, p: &Packet) -> Result<()> {
        self.write_request(IoType::WriteOnly { packet: p.clone(), }, None)
            .await.map(|_v| ())

    }

    async fn write_response_packet(&self, p: &Packet) -> Result<Packet> {
        let io_type = IoType::WriteAndResponse {
            packet: p.clone(),
            response_pid: packet_pid(p).expect("packet_pid"),
        };
        let (tx, rx) = oneshot::channel::<IoResult>();
        self.write_request(io_type, Some(tx))
            .await?;
        // TODO: Add a timeout?
        let res = rx.await.map_err(Error::from_std_err)?;
        res.result.map(|v| v.expect("return packet"))
    }

    async fn write_request(&self, io_type: IoType, tx_result: Option<oneshot::Sender<IoResult>>) -> Result<()> {
        // NB: Some duplication in IoTask::replay_subscriptions.

        let c = self.check_io_task()?;
        let req = IoRequest { tx_result, io_type };
        c.tx_io_requests.clone().send(req).await
            .map_err(|e| Error::from_std_err(e))?;
        Ok(())
    }

    fn check_io_task_mut(&mut self) -> Result<&mut IoTaskHandle> {
        match self.io_task_handle {
            Some(ref mut h) => Ok(h),
            None => Err("No IO task, did you call connect?".into()),
        }
    }

    fn check_io_task(&self) -> Result<&IoTaskHandle> {
        match self.io_task_handle {
            Some(ref h) => Ok(h),
            None => Err("No IO task, did you call connect?".into()),
        }
    }

    fn check_no_io_task(&self) -> Result<()> {
        match self.io_task_handle {
            Some(_) => Err("Already spawned IO task".into()),
            None => Ok(()),
        }
    }
}

/// Start network connection to the server.
async fn connect_stream(opts: &ClientOptions) -> Result<AsyncStream> {
    debug!("Connecting to {}:{}", opts.host, opts.port);
    match opts.connection_mode {
        #[cfg(feature = "tls")]
        ConnectionMode::Tls(ref c) => {
            let connector = TlsConnector::from(c.clone());
            let domain = DNSNameRef::try_from_ascii_str(&*opts.host)
                .map_err(|e| Error::from_std_err(e))?;
            let tcp = TcpStream::connect((&*opts.host, opts.port)).await?;
            let conn = connector.connect(domain, tcp).await?;
            Ok(AsyncStream::TlsStream(conn))
        },
        ConnectionMode::Tcp => {
            let tcp = TcpStream::connect((&*opts.host, opts.port)).await?;
            Ok(AsyncStream::TcpStream(tcp))
        }
        #[cfg(feature = "websocket")]
        ConnectionMode::Websocket => {
            // let url = format!("{}:{}", opts.host, opts.port);
            // println!("Websocket: \"{}\"", url);
            let websocket = tokio_tungstenite::connect_async(
                Request::get(opts.host.parse::<Uri>().unwrap()).header("Sec-WebSocket-Protocol", "mqtt").body(()).unwrap()
                ).await
                .map_err(crate::util::tungstenite_error_to_std_io_error)?.0;
            Ok(AsyncStream::WebSocket(websocket))
        }
    }
}

/// Build a connect packet from ClientOptions.
fn connect_packet(opts: &ClientOptions) -> Result<Packet> {
    Ok(Packet::Connect(mqttrs::Connect {
        protocol: mqttrs::Protocol::MQTT311,
        keep_alive: match opts.keep_alive {
            KeepAlive::Disabled => 0,
            KeepAlive::Enabled { secs } => secs,
        },
        client_id: match &opts.client_id {
            None => "".to_owned(),
            Some(cid) => cid.to_owned(),
        },
        clean_session: true, // TODO
        last_will: None, // TODO
        username: opts.username.clone(),
        password: opts.password.clone(),
    }))
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
            if self.halt.load(Ordering::SeqCst) {
                self.shutdown_conn().await;
                debug!("IoTask: halting by request.");
                self.state = IoTaskState::Halted;
                return;
            }

            match self.state {
                IoTaskState::Halted => return,
                IoTaskState::Disconnected =>
                    match Self::try_connect(&mut self).await {
                        Err(e) => {
                            error!("IoTask: Error connecting: {}", e);
                            if self.options.automatic_connect {
                                sleep(self.options.connect_retry_delay).await;
                            } else {
                                info!("IoTask: halting due to connection failure, auto connect is off.");
                                self.state = IoTaskState::Halted;
                                return;
                            }
                        },
                        Ok(()) => {
                            if let Err(e) = Self::replay_subscriptions(&mut self).await {
                                error!("IoTask: Error replaying subscriptions on reconnect: {}",
                                       e);
                            }
                        },
                    },
                IoTaskState::Connected(_) =>
                    match Self::run_once_connected(&mut self).await {
                        Err(Error::Disconnected) => {
                            info!("IoTask: Disconnected, resetting state");
                            self.state = IoTaskState::Disconnected;
                        },
                        Err(e) => {
                            error!("IoTask: Quitting run loop due to error: {}", e);
                            return;
                        },
                        _ => {},
                    },
            }
        }
    }

    async fn try_connect(&mut self) -> Result<()> {
        let stream = connect_stream(&self.options).await?;
        self.state =  IoTaskState::Connected(IoTaskConnected {
            stream,
            read_buf: BytesMut::with_capacity(self.options.max_packet_len),
            read_bufn: 0,
            last_write_time: Instant::now(),
            last_pingreq_time: Instant::now(),
            last_pingresp_time: Instant::now(),
            pid_response_map: BTreeMap::new(),
        });
        let c = match self.state {
            IoTaskState::Connected(ref mut c) => c,
            _ => panic!("Not reached"),
        };
        let conn = connect_packet(&self.options)?;
        debug!("IoTask: Sending connect packet");
        Self::write_packet(&self.options, c, &conn).await?;
        let read = Self::read_packet(&mut c.stream,
                                     &mut c.read_buf,
                                     &mut c.read_bufn,
                                     self.options.max_packet_len);
        let res = match timeout(self.options.operation_timeout,
                                read).await {
            // Timeout
            Err(Elapsed { .. }) =>
                Err(format!("Timeout waiting for Connack after {}ms",
                            self.options.operation_timeout.as_millis()).into()),

            // Non-timeout error
            Ok(Err(e)) => Err(e),

            Ok(Ok(Packet::Connack(ca))) => {
                match ca.code {
                    ConnectReturnCode::Accepted => {
                        debug!("IoTask: connack with code=Accepted.");
                        Ok(())
                    },
                    _ => Err(format!("Bad connect return code: {:?}", ca.code).into()),
                }
            },

            // Other unexpected packets.
            Ok(Ok(p)) =>
                Err(format!("Received packet not CONNACK after connect: {:?}", p).into()),
        };
        match res {
            Ok(()) => Ok(()),
            Err(e) => {
                self.shutdown_conn().await;
                Err(e)
            },
        }
    }

    /// Shutdown the network connection to the MQTT broker.
    ///
    /// Logs and swallows errors.
    async fn shutdown_conn(&mut self) {
        debug!("IoTask: shutdown_conn");
        let c = match self.state {
            // Already disconnected / halted, nothing more to do.
            IoTaskState::Disconnected |
            IoTaskState::Halted => return,

            IoTaskState::Connected(ref mut c) => c,
        };

        if let Err(e) = c.stream.shutdown().await {
            if e.kind() != std::io::ErrorKind::NotConnected {
                error!("IoTask: Error on stream shutdown in shutdown_conn: {:?}", e);
            }
        }
        self.state = IoTaskState::Disconnected;
    }

    async fn replay_subscriptions(&mut self) -> Result<()> {
        // NB: Some duplication in Client::subscribe and Client::write_request.
        let subs = self.subscriptions.clone();
        for (t, qos) in subs.iter() {
            trace!("Replaying subscription topic='{}' qos={:?}", t, qos);
            // Pick a high pid to probably avoid collisions with one allocated
            // by the Client.
            let pid = Pid::try_from(65535).expect("non-zero pid");
            let p = Packet::Subscribe(mqttrs::Subscribe {
                pid,
                topics: vec![SubscribeTopic { topic_path: t.to_owned(), qos: qos.to_owned() }]
            });
            let req = IoRequest {
                io_type: IoType::WriteAndResponse { packet: p, response_pid: pid },
                // TODO: I'm not sure how to receive the result; ignore it for now.
                tx_result: None,
            };
            self.handle_io_req(req).await?;
        }
        Ok(())
    }

    /// Unhandled errors are returned and terminate the run loop.
    async fn run_once_connected(&mut self) -> Result<()> {
        let c = match self.state {
            IoTaskState::Connected(ref mut c) => c,
            _ => panic!("Not reached"),
        };
        let pingreq_next = self.options.keep_alive.as_duration()
            .map(|dur| c.last_write_time + dur);

        let pingresp_expected_by =
            if self.options.keep_alive.is_enabled() &&
                c.last_pingreq_time > c.last_pingresp_time
            {
                // Expect a ping response before the operation timeout and the keepalive interval.
                // If the keepalive interval expired first then the "next operation" as
                // returned by SelectResult below would be Ping even when Pingresp is expected,
                // and we would never time out the connection.
                let ka = self.options.keep_alive.as_duration().expect("enabled");
                Some(c.last_pingreq_time + min(self.options.operation_timeout, ka))
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
            let mut req_fut = Box::pin(self.rx_io_requests.recv().fuse());
            let mut read_fut = Box::pin(
                Self::read_packet(&mut c.stream, &mut c.read_buf, &mut c.read_bufn,
                                  self.options.max_packet_len).fuse());
            let mut ping_fut = match pingreq_next {
                Some(t) => Box::pin(sleep_until(t).boxed().fuse()),
                None => Box::pin(pending().boxed().fuse()),
            };
            let mut pingresp_expected_fut = match pingresp_expected_by {
                Some(t) => Box::pin(sleep_until(t).boxed().fuse()),
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
            SelectResult::IoReq(req) => match req {
                None => {
                    // Sender closed.
                    debug!("IoTask: Req stream closed, shutting down.");
                    self.shutdown_conn().await;
                    return Err(Error::Disconnected);
                },
                Some(req) => return self.handle_io_req(req).await,
            },
            SelectResult::Ping => return self.send_ping().await,
            SelectResult::PingrespExpected => {
                // We timed out waiting for a ping response from
                // the server, shutdown the stream.
                debug!("IoTask: Timed out waiting for Pingresp, shutting down.");
                self.shutdown_conn().await;
                return Err(Error::Disconnected);
            }
        }
    }

    async fn handle_read(&mut self, read: Result<Packet>) -> Result<()> {
        let c = match self.state {
            IoTaskState::Connected(ref mut c) => c,
            _ => panic!("Not reached"),
        };

        match read {
            Err(Error::Disconnected) => {
                self.tx_recv_published.send(Err(Error::Disconnected)).await
                    .map_err(Error::from_std_err)?;
            }
            Err(e) => {
                self.tx_recv_published.send(
                        Err(format!("IoTask: Failed to read packet: {:?}", e).into())
                    )
                    .await
                    .map_err(Error::from_std_err)?;
            },
            Ok(p) => {
                match p {
                    Packet::Pingresp => {
                        debug!("IoTask: Received Pingresp");
                        c.last_pingresp_time = Instant::now();
                    },
                    Packet::Publish(_) => {
                        if let Err(e) = self.tx_recv_published.send(Ok(p)).await {
                            error!("IoTask: Failed to send Packet: {:?}", e);
                        }
                    },
                    Packet::Connack(_) => {
                        error!("IoTask: Unexpected CONNACK in handle_read(): {:?}", p);
                        self.shutdown_conn().await;
                        return Err(Error::Disconnected);
                    }
                    _ => {
                        let pid = packet_pid(&p);
                        if let Some(pid) = pid {
                            let pid_response = c.pid_response_map.remove(&pid);
                            match pid_response {
                                None => error!("Unknown PID: {:?}", pid),
                                Some(req) => {
                                    trace!("Sending response PID={:?} p={:?}",
                                           pid, p);
                                    let res = IoResult { result: Ok(Some(p)) };
                                    Self::send_io_result(req, res)?;
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
        let c = match self.state {
            IoTaskState::Connected(ref mut c) => c,
            _ => panic!("Not reached"),
        };
        let packet = req.io_type.packet();
        if let Some(p) = packet {
            c.last_write_time = Instant::now();
            let res = Self::write_packet(&self.options, c, &p).await;
            if let Err(e) = res {
                error!("IoTask: Error writing packet: {:?}", e);
                let res = IoResult { result: Err(e) };
                Self::send_io_result(req, res)?;
                return Ok(())
            }
            match p {
                Packet::Subscribe(s) => {
                    for st in s.topics.iter() {
                        trace!("Tracking subscription topic='{}', qos={:?}",
                               st.topic_path, st.qos);
                        let _ = self.subscriptions.insert(st.topic_path.clone(), st.qos);
                    }
                },
                Packet::Unsubscribe(u) => {
                    for t in u.topics.iter() {
                        trace!("Tracking unsubscription topic='{}'", t);
                        let _ = self.subscriptions.remove(t);
                    }
                },
                _ => {},
            }
            match req.io_type {
                IoType::WriteOnly { .. } => {
                    let res = IoResult { result: res.map(|_| None) };
                    Self::send_io_result(req, res)?;
                },
                IoType::WriteAndResponse { response_pid, .. } => {
                    c.pid_response_map.insert(response_pid, req);
                },
                IoType::ShutdownConnection => {
                    panic!("Not reached because ShutdownConnection has no packet")
                },
            }
        } else {
            match req.io_type {
                IoType::ShutdownConnection => {
                    debug!("IoTask: IoType::ShutdownConnection.");
                    self.shutdown_conn().await;
                    let res = IoResult { result: Ok(None) };
                    Self::send_io_result(req, res)?;
                    return Err(Error::Disconnected);
                }
                _ => (),
            }
        }
        Ok(())
    }

    fn send_io_result(req: IoRequest, res: IoResult) -> Result<()> {
        match req.tx_result {
            Some(tx) => {
                if let Err(e) = tx.send(res) {
                    error!("IoTask: Failed to send IoResult={:?}", e);
                }
            },
            None => {
                debug!("IoTask: Ignored IoResult: {:?}", res);
            },
        }
        Ok(())
    }

    async fn send_ping(&mut self) -> Result<()> {
        let c = match self.state {
            IoTaskState::Connected(ref mut c) => c,
            _ => panic!("Not reached"),
        };
        debug!("IoTask: Writing Pingreq");
        c.last_write_time = Instant::now();
        c.last_pingreq_time = Instant::now();
        let p = Packet::Pingreq;
        if let Err(e) = Self::write_packet(&self.options, c, &p).await {
            error!("IoTask: Failed to write ping: {:?}", e);
        }
        Ok(())
    }

    async fn write_packet(
        opts: &ClientOptions,
        c: &mut IoTaskConnected,
        p: &Packet,
    ) -> Result<()> {
        if cfg!(feature = "unsafe-logging") {
            trace!("write_packet p={:#?}", p);
        }
        // TODO: Test long packets.
        let mut bytes = BytesMut::with_capacity(opts.max_packet_len);
        mqttrs::encode(&p, &mut bytes)?;
        if cfg!(feature = "unsafe-logging") {
            trace!("write_packet bytes p={:?}", &*bytes);
        }
        c.stream.write_all(&*bytes).await?;
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
            IoType::ShutdownConnection => None,
            IoType::WriteOnly { packet } => Some(&packet),
            IoType::WriteAndResponse { packet, .. } => Some(&packet),
        }
    }
}

/// An enum for specifying which mode we will use to connect to the broker
#[derive(Clone)]
pub enum ConnectionMode {
    Tcp,
    #[cfg(feature = "websocket")]
    Websocket,
    #[cfg(feature = "tls")]
    Tls(Arc<rustls::ClientConfig>),
}
impl Default for ConnectionMode {
    fn default() -> Self {
        Self::Tcp
    }
}


#[cfg(test)]
mod test {
    use super::Client;

    #[test]
    fn client_is_send() {
        let c = Client::builder().set_host("localhost".to_owned()).build().unwrap();
        let _s: &dyn Send = &c;
    }
}
