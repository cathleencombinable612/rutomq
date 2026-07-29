use failure::Error;
use std::fs;
use std::path::PathBuf;
use std::process::Command;

pub mod generate_messages;

fn main() -> Result<(), Error> {
    let mut arguments = std::env::args_os().skip(1);
    let schema_dir = PathBuf::from(
        arguments
            .next()
            .expect("usage: protocol_codegen <schema-dir> [messages-output-dir]"),
    );
    let output_dir = arguments
        .next()
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("src/messages"));
    let output_path = std::fs::canonicalize(output_dir)?;
    let messages_module_dir = output_path.to_str().unwrap();

    // Clear output directory
    for file in fs::read_dir(messages_module_dir)? {
        let file = file?;
        if file.file_type()?.is_file() {
            let path = file.path();
            if path.extension() == Some("rs".as_ref()) {
                fs::remove_file(path)?;
            }
        }
    }

    // Find input files
    let mut input_file_paths = Vec::new();
    for file in fs::read_dir(&schema_dir)? {
        let file = file?;
        if file.file_type()?.is_file() {
            let path = file.path();
            if path.extension() == Some("json".as_ref()) {
                input_file_paths.push(path);
            }
        }
    }

    generate_messages::run(messages_module_dir, input_file_paths)?;

    println!("Running cargo fmt...");
    let mut process = Command::new("cargo")
        .args(vec!["fmt"])
        .spawn()
        .expect("cargo fmt failed");

    process.wait().expect("cargo fmt failed");

    Ok(())
}
