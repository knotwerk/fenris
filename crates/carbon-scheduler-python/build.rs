use std::env;
use std::process::Command;

fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-env-changed=PYO3_PYTHON");

    let python = env::var("PYO3_PYTHON").unwrap_or_else(|_| String::from("python3"));
    let Ok(output) = Command::new(python)
        .args([
            "-c",
            "import sysconfig; print(sysconfig.get_config_var('LIBDIR') or '')",
        ])
        .output()
    else {
        return;
    };

    if !output.status.success() {
        return;
    }

    let libdir = String::from_utf8_lossy(&output.stdout).trim().to_owned();
    if !libdir.is_empty() {
        println!("cargo:rustc-link-arg=-Wl,-rpath,{libdir}");
    }
}
