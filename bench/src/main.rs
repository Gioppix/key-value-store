use key_value_store::KVStorage;
use std::fs;

fn main() {
    // let env = env_logger::Env::new().default_filter_or("info");
    // let _ = env_logger::try_init_from_env(env);

    let location = "./test-dbs";
    let _ = fs::remove_dir_all(location);
    fs::create_dir_all(location).unwrap();

    let kv = KVStorage::new(location).unwrap();

    use std::collections::HashMap;

    // Create a small in-memory hashmap with known key/values
    let mut expected_values: HashMap<u64, Option<u64>> = HashMap::new();
    const KNOWN_KEY_SPACE: u64 = 100;

    // Initialize with some known key/values
    for key in 0..KNOWN_KEY_SPACE {
        let value = key * 100;
        expected_values.insert(key, Some(value));
        kv.write(key, Some(value)).unwrap();
    }

    // Helper function to generate random keys that don't overlap with known key space
    fn gen_random_key() -> u64 {
        const KEY_SPACE_SIZE: u64 = 1000000000;
        KNOWN_KEY_SPACE + (rand::random::<u64>() % KEY_SPACE_SIZE)
    }

    for i in 0..100000000 {
        let key = gen_random_key();

        // 10% null rate
        let value = if rand::random::<u64>() % 10 == 0 {
            None
        } else {
            Some(i * 2)
        };

        kv.write(key, value).unwrap();

        // Read recently written key
        assert_eq!(kv.read(&key).unwrap(), value);

        // Every 10 cycles read a random key
        if i % 10 == 0 {
            let random_key = gen_random_key();
            let _ = kv.read(&random_key).unwrap();
        }

        // Every 100 writes/reads, check and update known values
        if i % 100 == 0 {
            let known_key = rand::random::<u64>() % KNOWN_KEY_SPACE;

            // Check for correctness
            let stored_value = kv.read(&known_key).unwrap();
            let expected_value = expected_values.get(&known_key).unwrap();
            assert_eq!(
                stored_value, *expected_value,
                "Mismatch for known key {}",
                known_key
            );

            // Update the known value (10% null rate)
            let new_value = if rand::random::<u64>() % 10 == 0 {
                None
            } else {
                Some(i * 3 + known_key)
            };

            kv.write(known_key, new_value).unwrap();
            expected_values.insert(known_key, new_value);
        }
    }
}
