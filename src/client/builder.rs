use crate::{
    client::{
        Client,
        ClientOptions,
        client::ConnectionMode,
        KeepAlive,
    },
    Result,
    util::{
        TokioRuntime,
    }
};

#[cfg(feature = "tls")]
use rustls;
#[cfg(feature = "tls")]
use std::sync::Arc;
use tokio::time::Duration;

/// A fluent builder interface to configure a Client.
///
/// Note that you must call `.set_host()` to configure a host to
/// connect to before `.build()`
#[derive(Default)]
pub struct ClientBuilder {
    host: Option<String>,
    port: Option<u16>,
    username: Option<String>,
    password: Option<Vec<u8>>,
    keep_alive: Option<KeepAlive>,
    runtime: TokioRuntime,
    client_id: Option<String>,
    packet_buffer_len: Option<usize>,
    max_packet_len: Option<usize>,
    operation_timeout: Option<Duration>,
    connection_mode: ConnectionMode,
    automatic_connect: Option<bool>,
    connect_retry_delay: Option<Duration>,
}

impl ClientBuilder {
    /// Build a new `Client` with this configuration.
    pub fn build(&mut self) -> Result<Client> {
        Client::new(
            ClientOptions {
                host: match self.host {
                    Some(ref h) => h.clone(),
                    None => return Err("You must set a host to build a Client".into())
                },
                port: self.port.unwrap_or(1883),
                username: self.username.clone(),
                password: self.password.clone(),
                keep_alive: self.keep_alive.unwrap_or(KeepAlive::from_secs(30)),
                runtime: self.runtime.clone(),
                client_id: self.client_id.clone(),
                packet_buffer_len: self.packet_buffer_len.unwrap_or(100),
                max_packet_len: self.max_packet_len.unwrap_or(64 * 1024),
                operation_timeout: self.operation_timeout.unwrap_or(Duration::from_secs(20)),
                connection_mode: self.connection_mode.clone(),
                automatic_connect: self.automatic_connect.unwrap_or(true),
                connect_retry_delay: self.connect_retry_delay.unwrap_or(Duration::from_secs(30)),
            })
    }

    /// Set host to connect to. This is a required parameter.
    pub fn set_host(&mut self, host: String) -> &mut Self {
        self.host = Some(host);
        self
    }

    /// Set TCP port to connect to.
    ///
    /// The default value is 1883.
    ///
    /// If using a websocket, this value is ignored. The port should be specified in the host URL.
    pub fn set_port(&mut self, port: u16) -> &mut Self {
        self.port = Some(port);
        self
    }

    /// Set username to authenticate with.
    ///
    /// The default value is no username.
    pub fn set_username(&mut self, username: Option<String>) -> &mut Self {
        self.username = username;
        self
    }

    /// Set password to authenticate with.
    ///
    /// The default is no password.
    pub fn set_password(&mut self, password: Option<Vec<u8>>) -> &mut Self {
        self.password = password;
        self
    }

    /// Set keep alive time.
    ///
    /// This controls how often ping requests are sent when the connection is idle.
    /// See [MQTT 3.1.1 specification section 3.1.2.10](http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/csprd02/mqtt-v3.1.1-csprd02.html#_Keep_Alive)
    ///
    /// The default value is 30 seconds.
    pub fn set_keep_alive(&mut self, keep_alive: KeepAlive) -> &mut Self {
        self.keep_alive = Some(keep_alive);
        self
    }

    /// Set the tokio runtime to spawn background tasks onto.
    ///
    /// The default is to use the default tokio runtime, i.e. `tokio::spawn()`.
    pub fn set_tokio_runtime(&mut self, rt: TokioRuntime) -> &mut Self {
        self.runtime = rt;
        self
    }

    /// Set the ClientId to connect with.
    pub fn set_client_id(&mut self, client_id: Option<String>) -> &mut Self {
        self.client_id = client_id;
        self
    }

    /// Set the inbound and outbound packet buffer length.
    ///
    /// The default is 100.
    pub fn set_packet_buffer_len(&mut self, packet_buffer_len: usize) -> &mut Self {
        self.packet_buffer_len = Some(packet_buffer_len);
        self
    }

    /// Set the maximum packet length.
    ///
    /// The default is 64 * 1024 bytes.
    pub fn set_max_packet_len(&mut self, max_packet_len: usize) -> &mut Self {
        self.max_packet_len = Some(max_packet_len);
        self
    }

    /// Set the timeout for operations.
    ///
    /// The default is 20 seconds.
    pub fn set_operation_timeout(&mut self, operation_timeout: Duration) -> &mut Self {
        self.operation_timeout = Some(operation_timeout);
        self
    }

    /// Set the TLS ClientConfig for the client-server connection.
    ///
    /// Enables TLS. By default TLS is disabled.
    #[cfg(feature = "tls")]
    pub fn set_tls_client_config(&mut self, tls_client_config: rustls::ClientConfig) -> &mut Self {
        self.connection_mode = ConnectionMode::Tls(Arc::new(tls_client_config));
        self
    }

    #[cfg(feature = "websocket")]
    /// Set the connection to use a websocket
    pub fn set_websocket(&mut self) -> &mut Self {
        self.connection_mode = ConnectionMode::Websocket;
        self
    }

    /// Sets the connection mode to the given value
    ///
    /// The default is to use Tcp
    pub fn set_connection_mode(&mut self, mode: ConnectionMode) -> &mut Self {
        self.connection_mode = mode;
        self
    }

    /// Set whether to automatically connect and reconnect.
    ///
    /// The default is true.
    pub fn set_automatic_connect(&mut self, automatic_connect: bool) -> &mut Self {
        self.automatic_connect = Some(automatic_connect);
        self
    }

    /// Set the delay between connect retries.
    ///
    /// The default is 30s.
    pub fn set_connect_retry_delay(&mut self, connect_retry_delay: Duration) -> &mut Self {
        self.connect_retry_delay = Some(connect_retry_delay);
        self
    }
}
