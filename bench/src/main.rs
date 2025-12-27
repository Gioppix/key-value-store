use key_value_store::KVStorage;
use std::collections::HashMap;
use std::fs;
use std::sync::Arc;
use std::thread;

const NUM_THREADS: usize = 8;
const KNOWN_KEY_SPACE: u64 = 100;
const KEY_SPACE_SIZE: u64 = 1000000000;

fn gen_random_key(thread_id: usize) -> u64 {
    const TOTAL_KNOWN_SPACE: u64 = NUM_THREADS as u64 * KNOWN_KEY_SPACE;
    TOTAL_KNOWN_SPACE
        + (thread_id as u64 * KEY_SPACE_SIZE)
        + (rand::random::<u64>() % KEY_SPACE_SIZE)
}

fn random_value(seed: u64) -> Option<u64> {
    if rand::random::<u64>().is_multiple_of(10) {
        None
    } else {
        Some(seed)
    }
}

fn initialize_known_values(kv: &KVStorage, expected: &mut HashMap<u64, Option<u64>>, offset: u64) {
    for key in 0..KNOWN_KEY_SPACE {
        let actual_key = offset + key;
        let value = Some(actual_key * 100);
        expected.insert(actual_key, value);
        kv.write(actual_key, value).unwrap();
    }
}

fn verify_and_update_known_value(
    kv: &KVStorage,
    expected: &mut HashMap<u64, Option<u64>>,
    known_key: u64,
    thread_id: usize,
    seed: u64,
) {
    let stored = kv.read(&known_key).unwrap();
    let expected_value = expected.get(&known_key).unwrap();
    assert_eq!(
        stored, *expected_value,
        "Mismatch for known key {} in thread {}",
        known_key, thread_id
    );

    let new_value = random_value(seed);
    kv.write(known_key, new_value).unwrap();
    expected.insert(known_key, new_value);
}

fn main() {
    env_logger::init();

    let location = "./test-dbs";
    let _ = fs::remove_dir_all(location);
    fs::create_dir_all(location).unwrap();

    let kv = Arc::new(KVStorage::new(location).unwrap());

    let handles: Vec<_> = (0..NUM_THREADS)
        .map(|thread_id| {
            let kv_clone = Arc::clone(&kv);
            thread::spawn(move || {
                let mut expected_values = HashMap::new();
                let thread_key_offset = (thread_id as u64) * KNOWN_KEY_SPACE;

                initialize_known_values(&kv_clone, &mut expected_values, thread_key_offset);

                for i in 0..100000000 {
                    // println!("thread_id: {thread_id}, i: {i}");

                    let key = gen_random_key(thread_id);
                    let value = random_value(i * 2);

                    kv_clone.write(key, value).unwrap();
                    assert_eq!(kv_clone.read(&key).unwrap(), value);

                    if i.is_multiple_of(10) {
                        let _ = kv_clone.read(&gen_random_key(thread_id)).unwrap();
                    }

                    if i.is_multiple_of(100) {
                        let known_key =
                            thread_key_offset + (rand::random::<u64>() % KNOWN_KEY_SPACE);
                        verify_and_update_known_value(
                            &kv_clone,
                            &mut expected_values,
                            known_key,
                            thread_id,
                            i * 3 + known_key,
                        );
                    }
                }
            })
        })
        .collect();

    for handle in handles {
        handle.join().unwrap();
    }
}
