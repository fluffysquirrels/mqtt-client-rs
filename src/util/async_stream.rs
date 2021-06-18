#[cfg(feature = "websocket")]
use futures_util::{
    sink::{Sink, SinkExt},
    stream::Stream,
};
use std::{
    pin::Pin,
    task::{Context, Poll},
};
#[cfg(feature = "websocket")]
use std::io;
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    net::TcpStream,
};
#[cfg(feature = "tls")]
use tokio_rustls::client::TlsStream;
#[cfg(feature = "websocket")]
use tokio_tungstenite::{tungstenite::{protocol::Message, Error}, MaybeTlsStream, WebSocketStream};

/// A wrapper for the data connection, which may or may not be encrypted.
pub(crate) enum AsyncStream {
    TcpStream(TcpStream),
    #[cfg(feature = "tls")]
    TlsStream(TlsStream<TcpStream>),
    #[cfg(feature = "websocket")]
    WebSocket(WebSocketStream<MaybeTlsStream<TcpStream>>),
}

impl AsyncRead for AsyncStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context,
        buf: &mut ReadBuf,
    ) -> Poll<std::io::Result<()>> {
        match Pin::get_mut(self) {
            AsyncStream::TcpStream(tcp) => Pin::new(tcp).poll_read(cx, buf),
            #[cfg(feature = "tls")]
            AsyncStream::TlsStream(tls) => Pin::new(tls).poll_read(cx, buf),
            #[cfg(feature = "websocket")]
            AsyncStream::WebSocket(socket) => Pin::new(socket).poll_next(cx).map(|result| {
                result
                    .unwrap_or(Ok(Message::binary([])))
                    .map(|message| buf.put_slice(&message.into_data()))
                    .map_err(tungstenite_error_to_std_io_error)
            }),
        }
    }
}

impl AsyncWrite for AsyncStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context,
        buf: &[u8],
    ) -> Poll<std::result::Result<usize, tokio::io::Error>> {
        match Pin::get_mut(self) {
            AsyncStream::TcpStream(tcp) => Pin::new(tcp).poll_write(cx, buf),
            #[cfg(feature = "tls")]
            AsyncStream::TlsStream(tls) => Pin::new(tls).poll_write(cx, buf),
            #[cfg(feature = "websocket")]
            AsyncStream::WebSocket(socket) => socket.poll_ready_unpin(cx).map(|result| {
                match result {
                    Ok(()) => Pin::new(socket)
                        .start_send(Message::binary(buf))
                        .map(|()| buf.len()),
                    Err(e) => Err(e),
                }
                .map_err(tungstenite_error_to_std_io_error)
            }),
        }
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        cx: &mut Context,
    ) -> Poll<std::result::Result<(), tokio::io::Error>> {
        match Pin::get_mut(self) {
            AsyncStream::TcpStream(tcp) => Pin::new(tcp).poll_flush(cx),
            #[cfg(feature = "tls")]
            AsyncStream::TlsStream(tls) => Pin::new(tls).poll_flush(cx),
            #[cfg(feature = "websocket")]
            AsyncStream::WebSocket(socket) => Pin::new(socket)
                .poll_flush(cx)
                .map(|r| r.map_err(tungstenite_error_to_std_io_error)),
        }
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        cx: &mut Context,
    ) -> Poll<std::result::Result<(), tokio::io::Error>> {
        match Pin::get_mut(self) {
            AsyncStream::TcpStream(tcp) => Pin::new(tcp).poll_shutdown(cx),
            #[cfg(feature = "tls")]
            AsyncStream::TlsStream(tls) => Pin::new(tls).poll_shutdown(cx),
            #[cfg(feature = "websocket")]
            AsyncStream::WebSocket(socket) => Pin::new(socket)
                .poll_close(cx)
                .map(|r| r.map_err(tungstenite_error_to_std_io_error)),
        }
    }
}

/// Convert from tungstenite Error to std::io Error
#[cfg(feature = "websocket")]
pub fn tungstenite_error_to_std_io_error(e: Error) -> io::Error {
    match e {
        Error::Io(e) => e,
        Error::Utf8 => io::Error::new(io::ErrorKind::InvalidData, Error::Utf8),
        Error::Url(e) => io::Error::new(io::ErrorKind::InvalidInput, e),
        Error::HttpFormat(e) => io::Error::new(io::ErrorKind::InvalidData, e),
        e => io::Error::new(io::ErrorKind::Other, e),
    }
}
