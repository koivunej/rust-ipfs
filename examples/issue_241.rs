use async_std::task;
use futures::StreamExt;
use ipfs::{IpfsOptions, Types, UninitializedIpfs};

fn main() {
    env_logger::init();
    let options = IpfsOptions::<Types>::default();

    task::block_on(async move {
        println!("IPFS options: {:?}", options);
        let (ipfs, future) = UninitializedIpfs::new(options).await.start().await.unwrap();
        task::spawn(future);

        let id = ipfs.identity().await.unwrap().0.into_peer_id();
        println!("{}", id);

        // Subscribe
        let topic = "test1234".to_owned();
        let mut subscription = ipfs.pubsub_subscribe(topic.clone()).await.unwrap();

        ipfs.pubsub_publish(topic.clone(), vec![41, 41])
            .await
            .unwrap();
        while let Some(message) = subscription.next().await {
            println!("Got message: {:?}", message)
        }

        // Exit
        ipfs.exit_daemon().await;
    })
}
