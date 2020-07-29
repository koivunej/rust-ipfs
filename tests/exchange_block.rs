use async_std::future::timeout;
use ipfs::IpfsOptions;
use ipfs::{Block, Node};
use libipld::cid::{Cid, Codec};
use multihash::Sha2_256;
use std::time::Duration;

#[async_std::test]
async fn exchange_block() {
    tracing_subscriber::fmt::init();

    log::trace!("foobar");

    let data = b"hello block\n".to_vec().into_boxed_slice();
    let cid = Cid::new_v1(Codec::Raw, Sha2_256::digest(&data));

    let a = Node::with_name_and_options("a", IpfsOptions::inmemory_with_generated_keys()).await;
    let b = Node::with_name_and_options("b", IpfsOptions::inmemory_with_generated_keys()).await;

    let (_, mut addrs) = b.identity().await.unwrap();

    a.connect(addrs.pop().expect("b must have address to connect to"))
        .await
        .unwrap();

    a.put_block(Block {
        cid: cid.clone(),
        data: data.clone(),
    })
    .await
    .unwrap();

    let f = /*timeout(Duration::from_secs(10),*/ b.get_block(&cid) /*)*/;

    let Block { data: data2, .. } = f
        .await
        //.expect("get_block did not complete in time")
        .unwrap();

    assert_eq!(data, data2);
}
