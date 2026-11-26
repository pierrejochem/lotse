use std::time::Duration;

use lotse_rt::Clock;
use lotse_sim::Sim;

use super::all;

#[test]
fn the_futures_run_at_once_and_their_outputs_keep_their_order() {
    Sim::new(1).run(|rt| async move {
        let start = rt.now();
        let waits = [30u64, 10, 20];
        let outputs = all(waits.iter().map(|&ms| {
            let rt = rt.clone();
            async move {
                rt.sleep(Duration::from_millis(ms)).await;
                ms
            }
        }))
        .await;
        assert_eq!(outputs, vec![30, 10, 20], "outputs in the order the futures were given");
        assert_eq!(
            rt.now() - start,
            Duration::from_millis(30),
            "together they take as long as the longest, not the sum"
        );
    });
}

#[test]
fn no_futures_give_no_outputs() {
    Sim::new(1).run(|_rt| async move {
        let outputs = all(Vec::<std::future::Ready<u8>>::new()).await;
        assert!(outputs.is_empty());
    });
}
