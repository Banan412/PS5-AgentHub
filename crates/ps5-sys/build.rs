use std::{env, path::PathBuf};

fn main() {
    println!("cargo:rerun-if-env-changed=CARGO_CFG_TARGET_VENDOR");
    if env::var("CARGO_CFG_TARGET_VENDOR").as_deref() != Ok("sie") {
        return;
    }
    let sdk = env::var("PS5_PAYLOAD_SDK").unwrap_or_else(|_| "/opt/ps5-payload-sdk".into());
    println!("cargo:rerun-if-env-changed=PS5_PAYLOAD_SDK");
    println!("cargo:rerun-if-env-changed=LIBCLANG_PATH");
    println!("cargo:rerun-if-changed=include");

    let sdk_include = PathBuf::from(&sdk).join("target").join("include");
    assert!(
        sdk_include.exists(),
        "PS5 Payload SDK headers not found at {}: set PS5_PAYLOAD_SDK to the SDK install directory",
        sdk_include.display()
    );

    if let Ok(libclang_path) = env::var("LIBCLANG_PATH") {
        let p = PathBuf::from(&libclang_path);
        assert!(
            p.exists(),
            "LIBCLANG_PATH points to non-existent path: {}",
            p.display()
        );
    } else if cfg!(windows) {
        println!("cargo:warning=LIBCLANG_PATH is not set; bindgen will search PATH for libclang.dll");
    }

    // feature -> Sony libs it needs (libkernel_web, libSceLibcInternal and
    // libSceNet are always linked by prospero-clang).
    for (feature, libs) in [
        ("KERNEL_SYS", &["kernel_sys"][..]),
        ("NOTIFICATION", &["SceNotification"]),
        ("RANDOM", &["SceRandom"]),
        ("USER_SERVICE", &["SceUserService"]),
        ("SYSTEM_SERVICE", &["SceSystemService"]),
        ("APP_INST_UTIL", &["SceIpmi", "SceAppInstUtil"]),
        ("SSL", &["SceSsl"]),
        ("HTTP2", &["SceHttp2"]),
    ] {
        if env::var_os(format!("CARGO_FEATURE_{feature}")).is_some() {
            for lib in libs {
                println!("cargo:rustc-link-lib=dylib={lib}");
            }
        }
    }

    let bindings = bindgen::Builder::default()
        .header("include/wrapper.h")
        .clang_args([
            // PS5 is FreeBSD + SysV ABI; the sie-ps5 triple makes bindgen drop functions.
            "--target=x86_64-unknown-freebsd",
            "-nostdinc",
            &format!("-isystem{}", sdk_include.display()),
            "-Iinclude",
        ])
        .use_core()
        .ctypes_prefix("core::ffi")
        .layout_tests(false)
        // Bind whole files rather than name patterns: every function, type
        // and constant declared in these headers (plus what they depend on).
        .allowlist_file(concat!(
            r".*[\\/]ps5[\\/][a-z]+\.h|.*sce_extra\.h|",
            r".*[\\/](dirent|dlfcn|errno|fcntl|pthread|stdio|stdlib|string|time|unistd)\.h|",
            r".*[\\/]arpa[\\/]inet\.h|.*[\\/]netinet[\\/]in\.h|.*[\\/]sys[\\/](mman|socket|stat|sysctl)\.h",
        ))
        // core already provides these; bindgen would type size_t as u64.
        .blocklist_function("mem(cmp|cpy|move|set)|strlen")
        .blocklist_item("FP_.*") // libm-style float-class macros collide
        .default_enum_style(bindgen::EnumVariation::ModuleConsts)
        .generate()
        .expect("bindgen failed; is libclang installed (LIBCLANG_PATH) and PS5_PAYLOAD_SDK set?");

    let out = PathBuf::from(env::var("OUT_DIR").unwrap()).join("bindings.rs");
    bindings.write_to_file(out).unwrap();
}
