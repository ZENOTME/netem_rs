fn main() {
    let mut config = prost_build::Config::new();
    config.out_dir("src/proto");
    tonic_build::configure()
        .compile_with_config(config, &["proto/remote_port.proto"], &["proto"])
        .unwrap();

    let mut config = prost_build::Config::new();
    config.out_dir("src/proto");
    tonic_build::configure()
        .compile_with_config(config, &["proto/actor.proto"], &["proto"])
        .unwrap();
}
