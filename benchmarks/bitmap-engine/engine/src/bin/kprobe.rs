fn main() {
    let (v, s) = rdkafka::util::get_rdkafka_version();
    println!("librdkafka {s} (0x{v:x})");
}
