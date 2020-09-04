use ipfs::Node;
use libp2p::{multiaddr::Protocol, Multiaddr};
use std::borrow::Cow;
use std::time::Duration;
use tokio::time::timeout;

// Make sure two instances of ipfs can be connected by `Multiaddr`.
#[tokio::test(max_threads = 1)]
async fn dial_dns() {
    tracing_subscriber::fmt::init();
    tracing::trace!("starting out");

    let node_a = Node::new("a").await;
    let node_b = Node::new("b").await;
    let (b_publickey, mut addrs) = node_b.identity().await.unwrap();
    let b_peer_id = b_publickey.into_peer_id();

    let tcp = addrs
        .into_iter()
        .flat_map(|m| {
            m.iter()
                .filter(|p| matches!(p, Protocol::Tcp(_)))
                .map(|p| p.acquire())
                .collect::<Vec<_>>()
                .into_iter()
        })
        .next()
        .unwrap();

    let port = if let Protocol::Tcp(port) = tcp {
        port
    } else {
        panic!("failed to unwrap port");
    };

    // check dns4
    let addr = libp2p::build_multiaddr!(
        Dns4(Cow::Borrowed("localhost")),
        Tcp(port),
        P2p(b_peer_id.clone())
    );
    let res = timeout(Duration::from_secs(1), node_a.connect(addr)).await;
    assert!(matches!(res, Err(tokio::time::Elapsed { .. })));

    // check dns6
    let addr = libp2p::build_multiaddr!(
        Dns6(Cow::Borrowed("localhost")),
        Tcp(port),
        P2p(b_peer_id.clone())
    );
    let res = timeout(Duration::from_secs(1), node_a.connect(addr)).await;
    assert!(matches!(res, Err(tokio::time::Elapsed { .. })));

    // check dns
    let addr = libp2p::build_multiaddr!(
        Dns(Cow::Borrowed("localhost")),
        Tcp(port),
        P2p(b_peer_id.clone())
    );
    let res = timeout(Duration::from_secs(1), node_a.connect(addr)).await;
    assert!(matches!(res, Err(tokio::time::Elapsed { .. })));

    // check dnsaddr
    let addr = libp2p::build_multiaddr!(
        Dnsaddr(Cow::Borrowed("localhost")),
        Tcp(port),
        P2p(b_peer_id)
    );
    let res = timeout(Duration::from_secs(1), node_a.connect(addr)).await;
    assert!(matches!(res, Err(tokio::time::Elapsed { .. })));
}
