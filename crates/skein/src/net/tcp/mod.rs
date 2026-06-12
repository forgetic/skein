pub mod listener;
pub mod socket;
pub mod split;
pub mod stream;
pub mod traits;
pub mod virtual_tcp;

// Re-export trait types for convenience
pub use stream::TcpStreamBuilder;
pub use traits::{
    IncomingStream, TcpListenerApi, TcpListenerBuilder, TcpListenerExt, TcpStreamApi,
};
pub use virtual_tcp::{VirtualConnectionInjector, VirtualTcpListener, VirtualTcpStream};
