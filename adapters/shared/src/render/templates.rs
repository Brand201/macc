use std::fs;
use std::path::Path;

pub fn load_template(path: impl AsRef<Path>) -> std::io::Result<String> {
    fs::read_to_string(path)
}
